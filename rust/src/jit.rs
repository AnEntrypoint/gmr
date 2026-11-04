use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, UNIX_EPOCH};

use regex::Regex;
use rquickjs::promise::PromiseState;
use rquickjs::{Context, Ctx, Function, Module, Object, Promise, Runtime, Value as JsValue};
use serde_json::{json, Value};
use tokio::runtime::Handle;

use crate::types::Ctx as GmCtx;

const VERB_TIMEOUT: Duration = Duration::from_secs(900);
const KERNEL_TIMEOUT: Duration = Duration::from_secs(330);
const PUMP_TICK: Duration = Duration::from_millis(100);
const DRAIN_LIMIT: Duration = Duration::from_secs(10);
const CALL_MEMORY_BYTES: usize = 256 << 20;
const CALL_JS_STACK_BYTES: usize = 1 << 20;
const DISPATCH_FILE: &str = "dispatch.mjs";
const KERNEL_FILE: &str = "kernel/feed.mjs";
const VERBS_DIR: &str = "verbs";
const KERNEL_FUNCTIONS: [&str; 2] = ["nextContext", "finish"];
const SOURCE_EXTENSIONS: [&str; 4] = ["mjs", "js", "cjs", "json"];

const TIMERS: &str = r#"(function () {
  const native = { timer: globalThis.__jit_timer, clear: globalThis.__jit_clear };
  delete globalThis.__jit_timer;
  delete globalThis.__jit_clear;
  const noop = () => {};
  globalThis.console = { log: noop, info: noop, warn: noop, error: noop, debug: noop, trace: noop, dir: noop, table: noop, time: noop, timeEnd: noop };
  const timers = new Map();
  const arm = (fn, ms, args, repeat) => {
    const id = native.timer(Number(ms) || 0, repeat);
    timers.set(id, { run: () => fn(...args), repeat });
    return id;
  };
  globalThis.setTimeout = (fn, ms, ...args) => arm(fn, ms, args, false);
  globalThis.setInterval = (fn, ms, ...args) => arm(fn, ms, args, true);
  globalThis.clearTimeout = (id) => { if (timers.delete(id)) native.clear(id); };
  globalThis.clearInterval = globalThis.clearTimeout;
  if (typeof globalThis.queueMicrotask !== 'function') globalThis.queueMicrotask = (fn) => { Promise.resolve().then(fn); };
  return (id) => {
    const timer = timers.get(id);
    if (!timer) return;
    if (!timer.repeat) timers.delete(id);
    try { timer.run(); } catch (err) { noop(err); }
  };
})"#;

const AS_PROMISE: &str = "(value) => Promise.resolve(value)";

fn home() -> Option<&'static PathBuf> {
    static HOME: OnceLock<Option<PathBuf>> = OnceLock::new();
    HOME.get_or_init(|| {
        if let Some(dir) = std::env::var_os("GMNEW_HOME").map(PathBuf::from) {
            return dir.join("jit").is_dir().then_some(dir);
        }
        let exe = std::env::current_exe().ok()?;
        exe.ancestors().skip(1).find(|dir| dir.join("jit").is_dir()).map(Path::to_path_buf)
    })
    .as_ref()
}

pub fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("GMNEW_JIT").is_none_or(|v| v != "off") && home().is_some())
}

fn jit_root() -> Option<PathBuf> {
    home().map(|dir| dir.join("jit"))
}

struct Discovery {
    key: String,
    files: HashMap<String, PathBuf>,
    listing: Vec<Value>,
    kernel: HashSet<&'static str>,
    events: Vec<Value>,
    logged: AtomicBool,
}

impl Discovery {
    fn log_once(&self, gm: &GmCtx) {
        if !self.logged.swap(true, Ordering::SeqCst) {
            for event in &self.events {
                gm.run.event("jit-load", event.clone());
            }
        }
    }
}

fn caught(ctx: &Ctx<'_>, err: rquickjs::Error) -> String {
    if !matches!(err, rquickjs::Error::Exception) {
        return err.to_string();
    }
    let thrown = ctx.catch();
    if let Some(obj) = thrown.as_object() {
        if let Ok(Some(message)) = obj.get::<_, Option<String>>("message") {
            return message;
        }
    }
    thrown.get::<rquickjs::Coerced<String>>().map(|t| t.0).unwrap_or_else(|_| "a value that is not text was thrown".to_string())
}

fn stamp(path: &Path) -> Option<(u128, u64)> {
    let meta = std::fs::metadata(path).ok()?;
    let at = meta.modified().ok()?.duration_since(UNIX_EPOCH).ok()?.as_nanos();
    Some((at, meta.len()))
}

fn scan(root: &Path) -> String {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else { continue };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(kind) = entry.file_type() else { continue };
            if kind.is_dir() {
                if entry.file_name() != "node_modules" {
                    stack.push(path);
                }
            } else if path.extension().and_then(|e| e.to_str()).is_some_and(|e| SOURCE_EXTENSIONS.contains(&e)) {
                if let Some((at, len)) = stamp(&path) {
                    found.push(format!("{}:{at}:{len}", path.display()));
                }
            }
        }
    }
    found.sort();
    found.join(";")
}

fn module_name(path: &Path) -> String {
    let at = stamp(path).map(|(at, _)| at).unwrap_or(0);
    format!("{}@{at}", path.to_string_lossy().replace('\\', "/"))
}

fn shown(root: &Path, file: &Path) -> String {
    file.strip_prefix(root.parent().unwrap_or(root)).unwrap_or(file).to_string_lossy().replace('\\', "/")
}

fn spec_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| Regex::new(r#"(?m)(\bfrom\s*|\bimport\s*\(\s*|^\s*import\s*)(['"])([^'"\n]+)['"]"#).expect("spec pattern"))
}

fn link(source: &str, dir: &Path) -> Result<(String, Vec<PathBuf>), String> {
    let mut deps = Vec::new();
    let mut failure = None;
    let linked = spec_pattern().replace_all(source, |caps: &regex::Captures| match crate::runcode::resolve_spec(dir, &caps[3], true) {
        Ok(at) => {
            let name = module_name(&at);
            deps.push(at);
            format!("{}{}", &caps[1], json!(name))
        }
        Err(e) => {
            failure.get_or_insert(e);
            caps[0].to_string()
        }
    });
    match failure {
        Some(e) => Err(e),
        None => Ok((linked.into_owned(), deps)),
    }
}

fn declare(ctx: &Ctx<'_>, file: &Path, seen: &mut HashSet<PathBuf>) -> Result<Option<String>, String> {
    if !seen.insert(file.to_path_buf()) {
        return Ok(None);
    }
    let name = module_name(file);
    let text = std::fs::read_to_string(file).map_err(|e| format!("cannot read {}: {e}", file.display()))?;
    let (linked, deps) = link(&text, file.parent().unwrap_or(Path::new("/")))?;
    for dep in deps {
        declare(ctx, &dep, seen)?;
    }
    Module::declare(ctx.clone(), name.clone(), linked).map_err(|e| format!("{name}: {}", caught(ctx, e)))?;
    Ok(Some(name))
}

fn settle_jobs(ctx: &Ctx<'_>) {
    while ctx.execute_pending_job() {}
}

fn load<'js>(ctx: &Ctx<'js>, file: &Path, seen: &mut HashSet<PathBuf>) -> Result<Object<'js>, String> {
    let name = module_name(file);
    let text = std::fs::read_to_string(file).map_err(|e| format!("cannot read {}: {e}", file.display()))?;
    let (linked, deps) = link(&text, file.parent().unwrap_or(Path::new("/")))?;
    for dep in deps {
        declare(ctx, &dep, seen)?;
    }
    seen.insert(file.to_path_buf());
    let declared = Module::declare(ctx.clone(), name.clone(), linked).map_err(|e| format!("{name}: {}", caught(ctx, e)))?;
    let (module, evaluated) = declared.eval().map_err(|e| format!("{name}: {}", caught(ctx, e)))?;
    settle_jobs(ctx);
    match evaluated.state() {
        PromiseState::Resolved => module.namespace().map_err(|e| format!("{name}: {}", caught(ctx, e))),
        PromiseState::Rejected => Err(match evaluated.result::<JsValue>() {
            Some(Err(e)) => format!("{name}: {}", caught(ctx, e)),
            _ => format!("{name}: evaluation failed"),
        }),
        PromiseState::Pending => Err(format!("{name}: the module awaits at load, which the JIT host does not serve")),
    }
}

fn verb_files(root: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(root.join(VERBS_DIR))
        .map(|entries| entries.flatten().map(|e| e.path()).filter(|p| p.extension().is_some_and(|e| e == "mjs") && p.is_file()).collect())
        .unwrap_or_default();
    files.sort();
    files
}

fn fresh_context() -> Result<(Runtime, Context), String> {
    let runtime = Runtime::new().map_err(|e| e.to_string())?;
    runtime.set_memory_limit(CALL_MEMORY_BYTES);
    runtime.set_max_stack_size(CALL_JS_STACK_BYTES);
    let context = Context::full(&runtime).map_err(|e| e.to_string())?;
    Ok((runtime, context))
}

fn discover(root: &Path, key: String) -> Discovery {
    let started = Instant::now();
    let mut files = HashMap::new();
    let mut listing = Vec::new();
    let mut kernel = HashSet::new();
    let mut events = Vec::new();
    let outcome = fresh_context().and_then(|(_runtime, context)| {
        context.with(|ctx| -> Result<(), String> {
            ctx.eval::<Function, _>(TIMERS).and_then(|f| f.call::<_, Function>(())).map(|_| ()).ok();
            let mut seen = HashSet::new();
            let dispatch_file = root.join(DISPATCH_FILE);
            let dispatch = load(&ctx, &dispatch_file, &mut seen).map_err(|e| format!("{}: {e}", shown(root, &dispatch_file)))?;
            let register: Function = dispatch.get("register").map_err(|e| caught(&ctx, e))?;
            for file in verb_files(root) {
                let name = shown(root, &file);
                let outcome = load(&ctx, &file, &mut seen).and_then(|ns| {
                    let exported: JsValue = ns.get("verbs").map_err(|e| caught(&ctx, e))?;
                    register.call::<_, Vec<String>>((name.clone(), exported)).map_err(|e| caught(&ctx, e))
                });
                match outcome {
                    Ok(names) => {
                        for verb in names {
                            files.insert(verb, file.clone());
                        }
                    }
                    Err(error) => events.push(json!({ "file": name, "error": error })),
                }
            }
            let listed: Function = dispatch.get("listing").map_err(|e| caught(&ctx, e))?;
            let value: JsValue = listed.call(()).map_err(|e| caught(&ctx, e))?;
            let text = ctx.json_stringify(value).map_err(|e| caught(&ctx, e))?.and_then(|s| s.to_string().ok());
            listing = text.and_then(|t| serde_json::from_str(&t).ok()).unwrap_or_default();
            let kernel_file = root.join(KERNEL_FILE);
            if kernel_file.is_file() {
                match load(&ctx, &kernel_file, &mut seen) {
                    Ok(ns) => {
                        for function in KERNEL_FUNCTIONS {
                            if ns.get::<_, JsValue>(function).map(|v| v.is_function()).unwrap_or(false) {
                                kernel.insert(function);
                            }
                        }
                    }
                    Err(error) => events.push(json!({ "file": shown(root, &kernel_file), "error": error })),
                }
            }
            Ok(())
        })
    });
    if let Err(error) = outcome {
        events.push(json!({ "file": shown(root, &root.join(DISPATCH_FILE)), "error": error }));
        files.clear();
        listing.clear();
        kernel.clear();
    }
    events.push(json!({ "loaded": true, "verbs": files.len(), "kernel": kernel.len(), "failed": events.len(), "scanMs": started.elapsed().as_secs_f64() * 1000.0 }));
    Discovery { key, files, listing, kernel, events, logged: AtomicBool::new(false) }
}

fn current() -> Option<Arc<Discovery>> {
    static CACHE: OnceLock<Mutex<Option<Arc<Discovery>>>> = OnceLock::new();
    if !enabled() {
        return None;
    }
    let root = jit_root()?;
    let key = scan(&root);
    let mut slot = CACHE.get_or_init(|| Mutex::new(None)).lock().unwrap_or_else(|e| e.into_inner());
    if let Some(found) = slot.as_ref().filter(|d| d.key == key) {
        return Some(found.clone());
    }
    let fresh = Arc::new(discover(&root, key));
    *slot = Some(fresh.clone());
    Some(fresh)
}

async fn current_async() -> Option<Arc<Discovery>> {
    if !enabled() {
        return None;
    }
    tokio::task::spawn_blocking(current).await.ok().flatten()
}

pub fn listing() -> Vec<Value> {
    if !enabled() {
        return Vec::new();
    }
    let found = match Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(current),
        _ => current(),
    };
    found.map(|d| d.listing.clone()).unwrap_or_default()
}

pub async fn serves_kernel(function: &str) -> bool {
    current_async().await.is_some_and(|d| d.kernel.contains(function))
}

enum Target {
    Verb { file: PathBuf, name: String },
    Kernel { file: PathBuf, function: &'static str },
}

struct Timer {
    id: u32,
    due: Instant,
    every: Option<Duration>,
}

fn install_timers<'js>(ctx: &Ctx<'js>, timers: &Rc<RefCell<Vec<Timer>>>) -> rquickjs::Result<Function<'js>> {
    let globals = ctx.globals();
    let next = Rc::new(std::cell::Cell::new(0u32));
    {
        let timers = timers.clone();
        let timer = Function::new(ctx.clone(), move |ms: f64, repeat: bool| -> u32 {
            let id = next.get().wrapping_add(1);
            next.set(id);
            let wait = Duration::from_millis(if ms.is_finite() && ms > 0.0 { ms as u64 } else { 0 });
            timers.borrow_mut().push(Timer { id, due: Instant::now() + wait, every: repeat.then(|| wait.max(Duration::from_millis(1))) });
            id
        })?;
        globals.set("__jit_timer", timer)?;
    }
    {
        let timers = timers.clone();
        let clear = Function::new(ctx.clone(), move |id: u32| timers.borrow_mut().retain(|t| t.id != id))?;
        globals.set("__jit_clear", clear)?;
    }
    let build: Function = ctx.eval(TIMERS)?;
    build.call(())
}

fn due_timers(timers: &Rc<RefCell<Vec<Timer>>>) -> Vec<u32> {
    let now = Instant::now();
    let mut list = timers.borrow_mut();
    let mut fired = Vec::new();
    for timer in list.iter_mut() {
        if timer.due <= now {
            fired.push(timer.id);
            if let Some(every) = timer.every {
                timer.due = now + every;
            }
        }
    }
    list.retain(|t| t.due > now || t.every.is_some());
    fired
}

fn start<'js>(ctx: &Ctx<'js>, root: &Path, target: &Target, args: &Value, host: Object<'js>) -> Result<Promise<'js>, String> {
    let mut seen = HashSet::new();
    let input = ctx.json_parse(args.to_string()).map_err(|e| caught(ctx, e))?;
    let running: JsValue = match target {
        Target::Verb { file, name } => {
            let dispatch = load(ctx, &root.join(DISPATCH_FILE), &mut seen)?;
            let ns = load(ctx, file, &mut seen)?;
            let register: Function = dispatch.get("register").map_err(|e| caught(ctx, e))?;
            let exported: JsValue = ns.get("verbs").map_err(|e| caught(ctx, e))?;
            register.call::<_, JsValue>((shown(root, file), exported)).map_err(|e| caught(ctx, e))?;
            let call: Function = dispatch.get("dispatch").map_err(|e| caught(ctx, e))?;
            call.call((name.as_str(), input, host)).map_err(|e| caught(ctx, e))?
        }
        Target::Kernel { file, function } => {
            let ns = load(ctx, file, &mut seen)?;
            let call: Function = ns.get(*function).map_err(|e| caught(ctx, e))?;
            call.call((input, host)).map_err(|e| caught(ctx, e))?
        }
    };
    let wrap: Function = ctx.eval(AS_PROMISE).map_err(|e| caught(ctx, e))?;
    wrap.call((running,)).map_err(|e| caught(ctx, e))
}

fn drive<'js>(ctx: &Ctx<'js>, gm: Arc<GmCtx>, handle: Handle, root: &Path, target: &Target, args: &Value) -> Result<(Result<Value, String>, bool), String> {
    let timers = Rc::new(RefCell::new(Vec::new()));
    let fire = install_timers(ctx, &timers).map_err(|e| caught(ctx, e))?;
    let host = super::native::install(ctx, gm, root.parent().unwrap_or(root).to_path_buf(), handle).map_err(|e| caught(ctx, e))?;
    let host_object: Object = ctx.globals().get("host").map_err(|e| caught(ctx, e))?;
    let running = start(ctx, root, target, args, host_object);
    let outcome = running.and_then(|running| settle(ctx, &timers, &fire, &host, &running));
    let drain_until = Instant::now() + DRAIN_LIMIT;
    while host.in_flight() > 0 && Instant::now() < drain_until {
        if host.pump(PUMP_TICK).is_err() {
            break;
        }
        settle_jobs(ctx);
    }
    Ok((outcome, host.in_flight() == 0))
}

fn settle<'js>(ctx: &Ctx<'js>, timers: &Rc<RefCell<Vec<Timer>>>, fire: &Function<'js>, host: &super::native::Host<'js>, running: &Promise<'js>) -> Result<Value, String> {
    loop {
        settle_jobs(ctx);
        match running.state() {
            PromiseState::Resolved => {
                let value: JsValue = running.result::<JsValue>().unwrap_or(Ok(JsValue::new_undefined(ctx.clone()))).map_err(|e| caught(ctx, e))?;
                let text = ctx.json_stringify(value).map_err(|e| caught(ctx, e))?.and_then(|s| s.to_string().ok());
                return Ok(text.and_then(|t| serde_json::from_str(&t).ok()).unwrap_or(Value::Null));
            }
            PromiseState::Rejected => {
                return Err(match running.result::<JsValue>() {
                    Some(Err(e)) => caught(ctx, e),
                    _ => "the call failed".to_string(),
                })
            }
            PromiseState::Pending => {}
        }
        let due = due_timers(timers);
        if !due.is_empty() {
            for id in due {
                fire.call::<_, ()>((id,)).map_err(|e| caught(ctx, e))?;
                settle_jobs(ctx);
            }
            continue;
        }
        let next_due = timers.borrow().iter().map(|t| t.due).min();
        let wait = next_due.map(|d| d.saturating_duration_since(Instant::now())).unwrap_or(PUMP_TICK).min(PUMP_TICK);
        if host.in_flight() == 0 {
            if next_due.is_none() {
                return Err("the call awaits a promise that nothing settles".to_string());
            }
            std::thread::sleep(wait);
        } else {
            host.pump(wait).map_err(|e| caught(ctx, e))?;
        }
    }
}

fn run_call(gm: Arc<GmCtx>, handle: Handle, root: PathBuf, target: Target, args: Value) -> Value {
    let (runtime, context) = match fresh_context() {
        Ok(pair) => pair,
        Err(error) => return json!({ "ok": false, "error": error }),
    };
    let (outcome, drained) = match context.with(|ctx| drive(&ctx, gm, handle, &root, &target, &args)) {
        Ok(pair) => pair,
        Err(error) => (Err(error), true),
    };
    if drained {
        drop(context);
        drop(runtime);
    } else {
        std::mem::forget(context);
        std::mem::forget(runtime);
    }
    outcome.unwrap_or_else(|error| json!({ "ok": false, "error": error }))
}

async fn request(gm: Arc<GmCtx>, target: Target, args: Value, limit: Duration) -> Value {
    let root = match jit_root() {
        Some(root) => root,
        None => return json!({ "ok": false, "error": "the JIT checkout is not found" }),
    };
    let handle = Handle::current();
    let work = tokio::task::spawn_blocking(move || run_call(gm, handle, root, target, args));
    match tokio::time::timeout(limit, work).await {
        Ok(Ok(value)) => value,
        Ok(Err(e)) => json!({ "ok": false, "error": format!("the JIT call stopped: {e}") }),
        Err(_) => json!({ "ok": false, "error": format!("the JIT call did not return within {} ms", limit.as_millis()) }),
    }
}

pub async fn call_verb(gm: Arc<GmCtx>, name: &str, args: Value) -> Option<Value> {
    let found = current_async().await?;
    found.log_once(&gm);
    let file = found.files.get(name)?.clone();
    Some(request(gm, Target::Verb { file, name: name.to_string() }, args, VERB_TIMEOUT).await)
}

pub async fn kernel(gm: Arc<GmCtx>, function: &'static str, args: Value) -> Option<Value> {
    let found = current_async().await?;
    found.log_once(&gm);
    if !found.kernel.contains(function) {
        return None;
    }
    let file = jit_root()?.join(KERNEL_FILE);
    Some(request(gm, Target::Kernel { file, function }, args, KERNEL_TIMEOUT).await)
}
