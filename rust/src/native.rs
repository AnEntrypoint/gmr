use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, UNIX_EPOCH};

use rquickjs::{Ctx, Exception, Function, Object, Promise};
use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::runtime::Handle;
use tokio::sync::oneshot;

use crate::types::Ctx as GmCtx;

const OUTPUT_CAP: usize = 200_000;
const PIPE_CHUNK: usize = 8_192;
const DEFAULT_TIMEOUT_MS: u64 = 60_000;
const MAX_TIMEOUT_MS: u64 = 600_000;
const HTTP_TIMEOUT_MS: u64 = 30_000;
const HTTP_BODY_CAP: u64 = 16 << 20;
const MAX_REDIRECTS: u64 = 10;
const USER_AGENT: &str = "gm-new";

const PRELUDE: &str = r#"(function (call, sync, info) {
  const fail = (message) => {
    const error = new Error(String(message));
    const code = /^(E[A-Z]+):/.exec(error.message);
    if (code) error.code = code[1];
    return error;
  };
  const a = (op, ...args) => call(op, JSON.stringify(args)).then((text) => JSON.parse(text), (message) => { throw fail(message); });
  const s = (op, ...args) => {
    const reply = JSON.parse(sync(op, JSON.stringify(args)));
    if (Object.prototype.hasOwnProperty.call(reply, 'error')) throw fail(reply.error);
    return reply.value;
  };
  return Object.freeze({
    workspace: info.workspace,
    checkout: info.checkout,
    runDir: info.runDir,
    fs: Object.freeze({
      read: (path) => a('fs.read', path),
      readBytes: (path) => a('fs.readBytes', path),
      write: (path, text) => a('fs.write', path, text),
      stat: (path) => a('fs.stat', path),
      list: (path) => a('fs.list', path),
      remove: (path) => a('fs.remove', path),
      rename: (from, to) => a('fs.rename', from, to),
      mkdirp: (path) => a('fs.mkdirp', path),
      realpath: (path) => a('fs.realpath', path),
    }),
    proc: Object.freeze({
      run: (file, args, opts) => a('proc.run', file, args || [], opts || {}),
      shell: (command, opts) => a('proc.shell', command, opts || {}),
    }),
    http: (url, opts) => a('http', url, opts || {}),
    hash: Object.freeze({ sha256: (text) => s('hash.sha256', text) }),
    parseJs: (source, filename) => s('parseJs', source, filename),
    log: Object.freeze({
      append: (kind, fields) => a('log.append', kind, fields || {}),
      read: (filter) => a('log.read', filter || {}),
    }),
    jobs: Object.freeze({
      start: (name, command, opts) => a('jobs.start', name, command, opts || {}),
      status: (name, opts) => a('jobs.status', name, opts || {}),
      stop: (name, reason) => a('jobs.stop', name, reason),
      list: () => s('jobs.list'),
    }),
    lock: Object.freeze({
      acquire: (mode, paths) => a('lock.acquire', mode, paths || []),
      release: (token) => s('lock.release', token),
    }),
    mcp: Object.freeze({
      call: (server, tool, args) => a('mcp.call', server, tool, args || {}),
      tools: () => s('mcp.tools'),
    }),
    now: () => s('now'),
  });
})"#;

type Reply = (u64, Result<String, String>);
type Settle<'js> = (Function<'js>, Function<'js>);

pub struct Host<'js> {
    pending: Rc<RefCell<HashMap<u64, Settle<'js>>>>,
    replies: mpsc::Receiver<Reply>,
}

impl<'js> Host<'js> {
    pub fn in_flight(&self) -> usize {
        self.pending.borrow().len()
    }

    pub fn pump(&self, wait: Duration) -> rquickjs::Result<usize> {
        let mut replies = Vec::new();
        if !self.pending.borrow().is_empty() {
            if let Ok(reply) = self.replies.recv_timeout(wait) {
                replies.push(reply);
            }
        }
        while let Ok(reply) = self.replies.try_recv() {
            replies.push(reply);
        }
        let count = replies.len();
        for (id, outcome) in replies {
            let settle = self.pending.borrow_mut().remove(&id);
            if let Some((resolve, reject)) = settle {
                match outcome {
                    Ok(text) => resolve.call::<_, ()>((text,))?,
                    Err(message) => reject.call::<_, ()>((message,))?,
                }
            }
        }
        Ok(count)
    }
}

#[derive(Default)]
struct Held {
    next: AtomicU64,
    table: Mutex<HashMap<u64, oneshot::Sender<()>>>,
}

pub fn install<'js>(ctx: &Ctx<'js>, gm: Arc<GmCtx>, checkout: PathBuf, handle: Handle) -> rquickjs::Result<Host<'js>> {
    let (tx, replies) = mpsc::channel::<Reply>();
    let pending: Rc<RefCell<HashMap<u64, Settle<'js>>>> = Rc::new(RefCell::new(HashMap::new()));
    let held = Arc::new(Held::default());
    let next_call = Rc::new(AtomicU64::new(1));
    let call = {
        let pending = pending.clone();
        let gm = gm.clone();
        let held = held.clone();
        Function::new(ctx.clone(), move |c: Ctx<'js>, op: String, args: String| -> rquickjs::Result<Promise<'js>> {
            let (promise, resolve, reject) = c.promise()?;
            let id = next_call.fetch_add(1, Ordering::Relaxed);
            pending.borrow_mut().insert(id, (resolve, reject));
            let args = parse_args(&args);
            let tx = tx.clone();
            let gm = gm.clone();
            let held = held.clone();
            handle.spawn(async move {
                let outcome = run_async(gm, held, &op, args).await.map(|value| value.to_string());
                let _ = tx.send((id, outcome));
            });
            Ok(promise)
        })?
    };
    let sync = {
        let gm = gm.clone();
        let held = held.clone();
        Function::new(ctx.clone(), move |op: String, args: String| -> String {
            match run_sync(&gm, &held, &op, &parse_args(&args)) {
                Ok(value) => json!({ "value": value }).to_string(),
                Err(error) => json!({ "error": error }).to_string(),
            }
        })?
    };
    let info = Object::new(ctx.clone())?;
    info.set("workspace", path_text(&crate::search::absolute(&gm.workspace)))?;
    info.set("checkout", path_text(&crate::search::absolute(&checkout)))?;
    info.set("runDir", path_text(&crate::search::absolute(&gm.run.dir)))?;
    let build: Function = ctx.eval(PRELUDE).map_err(|e| thrown(ctx, e))?;
    let host: Object = build.call((call, sync, info)).map_err(|e| thrown(ctx, e))?;
    ctx.globals().set("host", host)?;
    Ok(Host { pending, replies })
}

fn thrown(ctx: &Ctx<'_>, error: rquickjs::Error) -> rquickjs::Error {
    if let rquickjs::Error::Exception = error {
        let caught = ctx.catch();
        let message = caught
            .as_exception()
            .and_then(|e| e.message())
            .unwrap_or_else(|| "the host prelude failed".to_string());
        return Exception::throw_message(ctx, &message);
    }
    error
}

fn parse_args(text: &str) -> Vec<Value> {
    match serde_json::from_str(text) {
        Ok(Value::Array(items)) => items,
        _ => Vec::new(),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn path_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn text_at(args: &[Value], at: usize, name: &str) -> Result<String, String> {
    match args.get(at) {
        Some(Value::String(text)) if !text.is_empty() => Ok(text.clone()),
        _ => Err(format!("{name} must be a non-empty string")),
    }
}

fn any_text_at(args: &[Value], at: usize, name: &str) -> Result<String, String> {
    match args.get(at) {
        Some(Value::String(text)) => Ok(text.clone()),
        _ => Err(format!("{name} must be a string")),
    }
}

fn object_at(args: &[Value], at: usize) -> Value {
    match args.get(at) {
        Some(value @ Value::Object(_)) => value.clone(),
        _ => json!({}),
    }
}

fn number_of(value: &Value, key: &str) -> Option<f64> {
    value.get(key).and_then(Value::as_f64).filter(|n| n.is_finite())
}

fn timeout_of(opts: &Value, default: u64) -> u64 {
    match number_of(opts, "timeoutMs") {
        Some(n) if n > 0.0 => (n as u64).min(MAX_TIMEOUT_MS),
        _ => default,
    }
}

fn located(gm: &GmCtx, raw: &str) -> PathBuf {
    let root = crate::search::absolute(&gm.workspace);
    crate::search::resolve(&root, raw)
}

fn node_error(e: &io::Error, syscall: &str, path: &Path, dest: Option<&Path>) -> String {
    let (code, text) = match e.kind() {
        ErrorKind::NotFound => ("ENOENT", "no such file or directory"),
        ErrorKind::PermissionDenied => ("EACCES", "permission denied"),
        ErrorKind::AlreadyExists => ("EEXIST", "file already exists"),
        ErrorKind::IsADirectory => ("EISDIR", "illegal operation on a directory"),
        ErrorKind::NotADirectory => ("ENOTDIR", "not a directory"),
        ErrorKind::DirectoryNotEmpty => ("ENOTEMPTY", "directory not empty"),
        _ => return format!("{e}, {syscall} '{}'", path.display()),
    };
    match dest {
        Some(dest) => format!("{code}: {text}, {syscall} '{}' -> '{}'", path.display(), dest.display()),
        None => format!("{code}: {text}, {syscall} '{}'", path.display()),
    }
}

async fn blocking<T: Send + 'static>(work: impl FnOnce() -> Result<T, String> + Send + 'static) -> Result<T, String> {
    tokio::task::spawn_blocking(work).await.map_err(|e| e.to_string())?
}

async fn run_async(gm: Arc<GmCtx>, held: Arc<Held>, op: &str, args: Vec<Value>) -> Result<Value, String> {
    match op {
        "fs.read" => {
            let path = located(&gm, &text_at(&args, 0, "path")?);
            blocking(move || {
                let bytes = std::fs::read(&path).map_err(|e| node_error(&e, "open", &path, None))?;
                Ok(json!(String::from_utf8_lossy(&bytes)))
            })
            .await
        }
        "fs.readBytes" => {
            let path = located(&gm, &text_at(&args, 0, "path")?);
            blocking(move || {
                let bytes = std::fs::read(&path).map_err(|e| node_error(&e, "open", &path, None))?;
                Ok(json!(base64(&bytes)))
            })
            .await
        }
        "fs.write" => {
            let path = located(&gm, &text_at(&args, 0, "path")?);
            let text = any_text_at(&args, 1, "text")?;
            blocking(move || {
                std::fs::write(&path, text).map_err(|e| node_error(&e, "open", &path, None))?;
                Ok(Value::Null)
            })
            .await
        }
        "fs.stat" => {
            let path = located(&gm, &text_at(&args, 0, "path")?);
            blocking(move || Ok(stat(&path))).await
        }
        "fs.list" => {
            let path = located(&gm, &text_at(&args, 0, "path")?);
            blocking(move || list(&path).map_err(|e| node_error(&e, "scandir", &path, None))).await
        }
        "fs.remove" => {
            let path = located(&gm, &text_at(&args, 0, "path")?);
            blocking(move || {
                let meta = std::fs::symlink_metadata(&path).map_err(|e| node_error(&e, "lstat", &path, None))?;
                let removed = if meta.is_dir() { std::fs::remove_dir_all(&path) } else { std::fs::remove_file(&path) };
                removed.map_err(|e| node_error(&e, "rm", &path, None))?;
                Ok(Value::Null)
            })
            .await
        }
        "fs.rename" => {
            let from = located(&gm, &text_at(&args, 0, "from")?);
            let to = located(&gm, &text_at(&args, 1, "to")?);
            blocking(move || {
                std::fs::rename(&from, &to).map_err(|e| node_error(&e, "rename", &from, Some(&to)))?;
                Ok(Value::Null)
            })
            .await
        }
        "fs.mkdirp" => {
            let path = located(&gm, &text_at(&args, 0, "path")?);
            blocking(move || {
                std::fs::create_dir_all(&path).map_err(|e| node_error(&e, "mkdir", &path, None))?;
                Ok(Value::Null)
            })
            .await
        }
        "fs.realpath" => {
            let path = located(&gm, &text_at(&args, 0, "path")?);
            blocking(move || {
                let real = std::fs::canonicalize(&path).map_err(|e| node_error(&e, "realpath", &path, None))?;
                Ok(json!(path_text(&real)))
            })
            .await
        }
        "proc.run" => {
            let file = text_at(&args, 0, "file")?;
            let list: Vec<String> = match args.get(1) {
                Some(Value::Array(items)) => items
                    .iter()
                    .map(|v| v.as_str().map(str::to_string).ok_or_else(|| "args must be an array of strings".to_string()))
                    .collect::<Result<_, _>>()?,
                None | Some(Value::Null) => Vec::new(),
                Some(_) => return Err("args must be an array of strings".to_string()),
            };
            let opts = object_at(&args, 2);
            let mut cmd = Command::new(&file);
            cmd.args(&list);
            own_group(&mut cmd);
            run_process(&gm, cmd, &opts).await
        }
        "proc.shell" => {
            let command = text_at(&args, 0, "command")?;
            let opts = object_at(&args, 1);
            let cmd = crate::exec::shell(&command, &gm.workspace);
            run_process(&gm, cmd, &opts).await
        }
        "http" => {
            let url = text_at(&args, 0, "url")?;
            let opts = object_at(&args, 1);
            blocking(move || http(&url, &opts)).await
        }
        "log.append" => {
            let kind = text_at(&args, 0, "kind")?;
            let fields = object_at(&args, 1);
            let run = gm.run.clone();
            blocking(move || Ok(json!(run.event(&kind, fields)))).await
        }
        "log.read" => {
            let filter = object_at(&args, 0);
            let run = gm.run.clone();
            blocking(move || read_log(run.as_ref(), filter)).await
        }
        "jobs.start" => {
            let name = text_at(&args, 0, "name")?;
            let command = text_at(&args, 1, "command")?;
            let opts = object_at(&args, 2);
            let cwd = match opts.get("cwd").and_then(Value::as_str) {
                Some(dir) => located(&gm, dir),
                None => gm.workspace.clone(),
            };
            let reply = gm.jobs.start(gm.run.clone(), &name, &command, &cwd, timeout_of(&opts, DEFAULT_TIMEOUT_MS));
            settled(reply).map(|mut value| {
                value["started"] = json!(true);
                value
            })
        }
        "jobs.status" => {
            let name = text_at(&args, 0, "name")?;
            let opts = object_at(&args, 1);
            let wait_ms = number_of(&opts, "waitMs").filter(|n| *n > 0.0).map_or(0, |n| n as u64);
            let offset = number_of(&opts, "offset").filter(|n| *n >= 0.0).map(|n| n as u64);
            let status = if crate::runcode::is_background(&name) {
                crate::runcode::job_status(&name, wait_ms.min(30_000), offset.map(|o| o as usize)).await
            } else {
                gm.jobs.status(&name, wait_ms, offset).await
            };
            status.ok_or_else(|| format!("no job named {name}"))
        }
        "jobs.stop" => {
            let name = text_at(&args, 0, "name")?;
            let reason = args.get(1).and_then(Value::as_str).unwrap_or("cancelled").to_string();
            let reply = if crate::runcode::is_background(&name) {
                crate::runcode::job_stop(&gm, &name, &reason).ok_or_else(|| format!("no job named {name}"))?
            } else {
                gm.jobs.stop(&name).await
            };
            settled(reply)
        }
        "lock.acquire" => acquire(&gm, &held, &args).await,
        "mcp.call" => {
            let server = text_at(&args, 0, "server")?;
            let tool = text_at(&args, 1, "tool")?;
            gm.hub.call(&server, &tool, object_at(&args, 2)).await
        }
        _ => Err(format!("unknown host function: {op}")),
    }
}

const LOG_READ_PAGES: usize = 10_000;

fn read_log(src: &dyn crate::log::LogSource, mut filter: Value) -> Result<Value, String> {
    let whole = filter.get("last").is_none() && filter.get("js").is_none();
    let mut events = Vec::new();
    for _ in 0..LOG_READ_PAGES {
        let reply = crate::log::query_log(src, &filter);
        if reply.get("ok") != Some(&Value::Bool(true)) {
            return Err(reply.get("error").and_then(Value::as_str).unwrap_or("log read failed").to_string());
        }
        let text = reply.get("output").and_then(Value::as_str).unwrap_or("[]");
        let page: Value = serde_json::from_str(text).map_err(|_| format!("log read is cut: {}", text.len()))?;
        if !whole {
            return Ok(page);
        }
        if let Value::Array(items) = page {
            events.extend(items);
        }
        let Some(next) = reply.get("next").and_then(Value::as_u64) else { break };
        filter["after"] = json!(next);
    }
    Ok(Value::Array(events))
}

fn run_sync(gm: &GmCtx, held: &Held, op: &str, args: &[Value]) -> Result<Value, String> {
    match op {
        "hash.sha256" => Ok(json!(crate::run::sha_hex(any_text_at(args, 0, "text")?.as_bytes()))),
        "parseJs" => {
            let source = any_text_at(args, 0, "source")?;
            let filename = args.get(1).and_then(Value::as_str).unwrap_or("input.mjs");
            Ok(match crate::code::file_defs_and_calls(filename, &source) {
                Some(parsed) => json!({
                    "defs": parsed.defs,
                    "calls": parsed.calls.into_iter().map(|(name, line, member)| json!({ "name": name, "line": line, "member": member })).collect::<Vec<_>>(),
                    "specs": parsed.specs,
                }),
                None => Value::Null,
            })
        }
        "jobs.list" => {
            let mut jobs = gm.jobs.list();
            jobs.extend(crate::runcode::jobs_open());
            Ok(json!(jobs))
        }
        "lock.release" => {
            let token = args.first().and_then(Value::as_u64).ok_or_else(|| "token must be a lock token".to_string())?;
            match lock(&held.table).remove(&token) {
                Some(release) => {
                    let _ = release.send(());
                    Ok(Value::Null)
                }
                None => Err(format!("no lock held with token {token}")),
            }
        }
        "mcp.tools" => Ok(json!(gm
            .hub
            .tools()
            .into_iter()
            .map(|t| json!({ "server": t.server, "tool": t.tool, "description": t.description, "schema": t.schema }))
            .collect::<Vec<_>>())),
        "now" => Ok(json!({ "mono": gm.run.now(), "iso": crate::log::iso_now() })),
        _ => Err(format!("unknown host function: {op}")),
    }
}

fn settled(reply: Value) -> Result<Value, String> {
    if reply.get("ok") == Some(&Value::Bool(false)) {
        return Err(reply.get("error").and_then(Value::as_str).unwrap_or("failed").to_string());
    }
    Ok(reply)
}

fn stat(path: &Path) -> Value {
    let Ok(meta) = std::fs::symlink_metadata(path) else {
        return Value::Null;
    };
    let kind = if meta.file_type().is_symlink() {
        "symlink"
    } else if meta.is_dir() {
        "dir"
    } else if meta.is_file() {
        "file"
    } else {
        "other"
    };
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0);
    json!({ "type": kind, "size": meta.len(), "mtimeMs": mtime })
}

fn list(path: &Path) -> io::Result<Value> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        names.push(if entry.file_type()?.is_dir() { format!("{name}/") } else { name });
    }
    names.sort();
    Ok(json!(names))
}

fn base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16) | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8) | u32::from(*chunk.get(2).unwrap_or(&0));
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { TABLE[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { TABLE[n as usize & 63] as char } else { '=' });
    }
    out
}

#[cfg(unix)]
fn own_group(cmd: &mut Command) {
    cmd.process_group(0);
}

#[cfg(windows)]
fn own_group(cmd: &mut Command) {
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    cmd.creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
}

async fn read_capped<R: AsyncRead + Unpin>(pipe: Option<R>) -> Vec<u8> {
    let Some(mut pipe) = pipe else { return Vec::new() };
    let mut kept = Vec::new();
    let mut chunk = [0u8; PIPE_CHUNK];
    while let Ok(n) = pipe.read(&mut chunk).await {
        if n == 0 {
            break;
        }
        let room = OUTPUT_CAP.saturating_sub(kept.len()).min(n);
        kept.extend_from_slice(&chunk[..room]);
    }
    kept
}

struct Group(u32);

impl Drop for Group {
    fn drop(&mut self) {
        crate::exec::kill_group_now(self.0);
    }
}

async fn run_process(gm: &GmCtx, mut cmd: Command, opts: &Value) -> Result<Value, String> {
    let cwd = match opts.get("cwd").and_then(Value::as_str) {
        Some(dir) => located(gm, dir),
        None => gm.workspace.clone(),
    };
    let stdin = opts.get("stdin").and_then(Value::as_str).map(str::to_string);
    let timeout_ms = timeout_of(opts, DEFAULT_TIMEOUT_MS);
    cmd.current_dir(&cwd)
        .env_remove("ANTHROPIC_API_KEY")
        .stdin(if stdin.is_some() { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| e.to_string())?;
    let pid = child.id().unwrap_or(0);
    let group = Group(pid);
    let writer = child.stdin.take().map(|mut pipe| {
        let text = stdin.unwrap_or_default();
        tokio::spawn(async move {
            let _ = pipe.write_all(text.as_bytes()).await;
            let _ = pipe.shutdown().await;
        })
    });
    let out = tokio::spawn(read_capped(child.stdout.take()));
    let err = tokio::spawn(read_capped(child.stderr.take()));
    let (status, timed_out) = match tokio::time::timeout(Duration::from_millis(timeout_ms), child.wait()).await {
        Ok(status) => (status.ok(), false),
        Err(_) => {
            crate::exec::kill_group(pid).await;
            (child.wait().await.ok(), true)
        }
    };
    drop(group);
    if let Some(writer) = writer {
        writer.abort();
    }
    let stdout = out.await.unwrap_or_default();
    let stderr = err.await.unwrap_or_default();
    Ok(json!({
        "code": status.as_ref().and_then(|s| s.code()),
        "signal": status.as_ref().and_then(crate::exec::signal_name),
        "stdout": String::from_utf8_lossy(&stdout),
        "stderr": String::from_utf8_lossy(&stderr),
        "timedOut": timed_out,
    }))
}

fn agent() -> &'static ureq::Agent {
    static AGENT: OnceLock<ureq::Agent> = OnceLock::new();
    AGENT.get_or_init(|| {
        let config = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .max_redirects(0)
            .max_redirects_will_error(false)
            .build();
        ureq::Agent::new_with_config(config)
    })
}

fn http_failure(err: ureq::Error, timeout_ms: u64) -> String {
    match err {
        ureq::Error::Timeout(_) => format!("timed out after {timeout_ms} ms"),
        ureq::Error::Io(io) => {
            let code = match io.kind() {
                ErrorKind::ConnectionRefused => Some("ECONNREFUSED"),
                ErrorKind::ConnectionReset => Some("ECONNRESET"),
                ErrorKind::ConnectionAborted => Some("ECONNABORTED"),
                ErrorKind::TimedOut => Some("ETIMEDOUT"),
                _ => None,
            };
            format!("request failed: {}", code.map(str::to_string).unwrap_or_else(|| io.to_string()))
        }
        ureq::Error::HostNotFound => "request failed: ENOTFOUND".to_string(),
        other => format!("request failed: {other}"),
    }
}

fn next_url(base: &str, location: &str) -> String {
    if location.contains("://") {
        return location.to_string();
    }
    let scheme_end = base.find("://").map_or(0, |at| at + 3);
    let host_end = base[scheme_end..].find('/').map_or(base.len(), |at| scheme_end + at);
    if let Some(rest) = location.strip_prefix("//") {
        return format!("{}{rest}", &base[..scheme_end]);
    }
    if location.starts_with('/') {
        return format!("{}{location}", &base[..host_end]);
    }
    let path = base[host_end..].split(['?', '#']).next().unwrap_or("");
    let dir = path.rfind('/').map_or("/", |at| &path[..=at]);
    format!("{}{dir}{location}", &base[..host_end])
}

fn http(start: &str, opts: &Value) -> Result<Value, String> {
    let timeout_ms = match number_of(opts, "timeoutMs") {
        Some(n) if n > 0.0 => (n as u64).min(MAX_TIMEOUT_MS),
        _ => HTTP_TIMEOUT_MS,
    };
    let mut method = opts.get("method").and_then(Value::as_str).unwrap_or("GET").to_ascii_uppercase();
    let mut body = opts.get("body").and_then(Value::as_str).map(str::to_string);
    let headers: Vec<(String, String)> = opts
        .get("headers")
        .and_then(Value::as_object)
        .map(|map| map.iter().filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string()))).collect())
        .unwrap_or_default();
    let mut current = start.to_string();
    let mut redirects: u64 = 0;
    loop {
        let mut builder = ureq::http::Request::builder().method(method.as_str()).uri(current.as_str());
        if !headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("user-agent")) {
            builder = builder.header("user-agent", USER_AGENT);
        }
        for (key, value) in &headers {
            builder = builder.header(key.as_str(), value.as_str());
        }
        let request = builder.body(body.clone().unwrap_or_default()).map_err(|e| format!("request failed: {e}"))?;
        let request = agent().configure_request(request).timeout_global(Some(Duration::from_millis(timeout_ms))).build();
        let mut response = agent().run(request).map_err(|e| http_failure(e, timeout_ms))?;
        let status = response.status().as_u16();
        let location = response.headers().get("location").and_then(|v| v.to_str().ok()).map(str::to_string);
        if let Some(location) = location.filter(|_| (300..=399).contains(&status)) {
            redirects += 1;
            if redirects > MAX_REDIRECTS {
                return Err(format!("request failed: more than {MAX_REDIRECTS} redirects"));
            }
            current = next_url(&current, &location);
            if status == 303 || ((status == 301 || status == 302) && method == "POST") {
                method = "GET".to_string();
                body = None;
            }
            continue;
        }
        let mut shown = serde_json::Map::new();
        for (key, value) in response.headers() {
            if let Ok(text) = value.to_str() {
                shown.insert(key.as_str().to_string(), json!(text));
            }
        }
        let bytes = response
            .body_mut()
            .with_config()
            .limit(HTTP_BODY_CAP)
            .read_to_vec()
            .map_err(|e| http_failure(e, timeout_ms))?;
        return Ok(json!({
            "status": status,
            "headers": Value::Object(shown),
            "body": String::from_utf8_lossy(&bytes),
            "finalUrl": current,
            "redirects": redirects,
        }));
    }
}

async fn acquire(gm: &GmCtx, held: &Arc<Held>, args: &[Value]) -> Result<Value, String> {
    let mode = text_at(args, 0, "mode")?;
    if !matches!(mode.as_str(), "read" | "write" | "shell") {
        return Err("mode must be read, write or shell".to_string());
    }
    let mut paths: Vec<PathBuf> = match args.get(1) {
        Some(Value::Array(items)) => items.iter().filter_map(Value::as_str).map(|p| located(gm, p)).collect(),
        _ => Vec::new(),
    };
    paths.sort();
    paths.dedup();
    let locks = gm.locks.clone();
    let (ready_tx, ready_rx) = oneshot::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        if mode == "shell" {
            let _all = locks.exclusive().await;
            let _ = ready_tx.send(());
            let _ = release_rx.await;
            return;
        }
        let _shared = locks.shared().await;
        let mut guards = Vec::new();
        if mode == "write" {
            for path in &paths {
                guards.push(locks.path(path).await);
            }
        }
        let _ = ready_tx.send(());
        let _ = release_rx.await;
        drop(guards);
    });
    ready_rx.await.map_err(|_| "lock acquire failed".to_string())?;
    let token = held.next.fetch_add(1, Ordering::Relaxed) + 1;
    lock(&held.table).insert(token, release_tx);
    Ok(json!(token))
}
