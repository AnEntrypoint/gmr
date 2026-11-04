use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use regex::Regex;
use rquickjs::promise::PromiseState;
use rquickjs::{Context, Ctx, Exception, Function, Module, Object, Promise, Runtime, Value as JsValue};
use serde_json::{json, Value};
use tokio::runtime::Handle;
use tokio::sync::{oneshot, watch, Notify, Semaphore};

use crate::types::Ctx as GmCtx;

pub const DEFAULT_TIMEOUT_MS: u64 = 180_000;
pub const MAX_TIMEOUT_MS: u64 = 300_000;
pub const BODY_SLOTS: usize = 4;
const OUTPUT_CAP: usize = 20_000;
const BODY_MEMORY_BYTES: usize = 128 << 20;
const BODY_JS_STACK_BYTES: usize = 1 << 20;
const BODY_THREAD_STACK_BYTES: usize = 8 << 20;
const LOOP_TICK: Duration = Duration::from_millis(100);
const BODY_CALLS_CAP: usize = 256;
const NODE_BUILTINS: &[&str] = &[
    "assert", "buffer", "child_process", "cluster", "crypto", "dgram", "dns", "events", "fs", "fs/promises", "http", "http2", "https", "module",
    "net", "os", "path", "perf_hooks", "process", "querystring", "readline", "stream", "string_decoder", "timers", "tls", "tty", "url", "util",
    "v8", "vm", "worker_threads", "zlib",
];

fn slots() -> Arc<Semaphore> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SLOTS.get_or_init(|| Arc::new(Semaphore::new(BODY_SLOTS))).clone()
}

struct BodyState {
    seq: u64,
    stop: AtomicBool,
    stop_notice: Notify,
    queued: AtomicBool,
    calls: Mutex<Vec<Value>>,
}

impl BodyState {
    fn stopped(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    fn request_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.stop_notice.notify_one();
    }

    fn record(&self, call: Value) {
        let mut calls = self.calls.lock().unwrap_or_else(|e| e.into_inner());
        if calls.len() >= BODY_CALLS_CAP {
            calls.remove(0);
        }
        calls.push(call);
    }

    fn calls_text(&self) -> String {
        let calls = self.calls.lock().unwrap_or_else(|e| e.into_inner());
        calls.iter().map(Value::to_string).collect::<Vec<_>>().join("\n")
    }

    fn logged(&self) -> Vec<Value> {
        let calls = self.calls.lock().unwrap_or_else(|e| e.into_inner());
        calls.iter().map(|c| json!({ "seq": c["seq"], "tool": c["tool"], "ok": c["ok"] })).collect()
    }
}

struct BackgroundBody {
    body: Arc<BodyState>,
    settled: watch::Sender<Option<Value>>,
    status_file: PathBuf,
    started: Instant,
}

fn table() -> &'static Mutex<HashMap<String, Arc<BackgroundBody>>> {
    static TABLE: OnceLock<Mutex<HashMap<String, Arc<BackgroundBody>>>> = OnceLock::new();
    TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn background(name: &str) -> Option<Arc<BackgroundBody>> {
    table().lock().unwrap_or_else(|e| e.into_inner()).get(name).cloned()
}

fn state_of(job: &BackgroundBody) -> String {
    match &*job.settled.borrow() {
        Some(fields) => fields["state"].as_str().unwrap_or("exited").to_string(),
        None if job.body.queued.load(Ordering::SeqCst) => "queued".to_string(),
        None => "running".to_string(),
    }
}

fn settle(ctx: &GmCtx, name: &str, job: &BackgroundBody, fields: Value) -> bool {
    let changed = job.settled.send_if_modified(|slot| {
        if slot.is_some() {
            return false;
        }
        *slot = Some(fields.clone());
        true
    });
    if changed {
        let mut status = json!({ "job": name, "body": job.body.seq });
        merge(&mut status, &fields);
        write_status(&job.status_file, &status);
        let mut event = json!({ "job": name, "body": job.body.seq });
        merge(&mut event, &fields);
        if let Some(map) = event.as_object_mut() {
            map.remove("value");
            map.remove("error");
        }
        ctx.run.event("job", event);
    }
    changed
}

fn merge(into: &mut Value, from: &Value) {
    if let (Some(target), Some(source)) = (into.as_object_mut(), from.as_object()) {
        for (key, value) in source {
            target.insert(key.clone(), value.clone());
        }
    }
}

fn write_status(file: &Path, status: &Value) {
    if let Some(dir) = file.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let _ = std::fs::write(file, status.to_string());
}

pub fn is_background(name: &str) -> bool {
    background(name).is_some()
}

pub fn background_running() -> usize {
    table().lock().unwrap_or_else(|e| e.into_inner()).values().filter(|job| job.settled.borrow().is_none()).count()
}

pub fn jobs_open() -> Vec<Value> {
    let jobs: Vec<(String, Arc<BackgroundBody>)> =
        table().lock().unwrap_or_else(|e| e.into_inner()).iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    jobs.into_iter()
        .filter(|(_, job)| job.settled.borrow().is_none())
        .map(|(name, job)| json!({ "job": name, "body": job.body.seq, "state": state_of(&job), "runningMs": job.started.elapsed().as_millis() as u64 }))
        .collect()
}

pub async fn job_status(name: &str, wait_ms: u64, offset: Option<usize>) -> Option<Value> {
    let job = background(name)?;
    if wait_ms > 0 {
        let mut watcher = job.settled.subscribe();
        let _ = tokio::time::timeout(Duration::from_millis(wait_ms), watcher.wait_for(|v| v.is_some())).await;
    }
    let text = job.body.calls_text();
    let total = text.chars().count();
    let from = offset.unwrap_or(total.saturating_sub(OUTPUT_CAP)).min(total);
    let end = total.min(from + OUTPUT_CAP);
    let output: String = text.chars().skip(from).take(end - from).collect();
    let mut status = json!({ "ok": true, "job": name, "body": job.body.seq, "state": state_of(&job), "exitCode": null, "signal": null });
    if let Some(fields) = &*job.settled.borrow() {
        merge(&mut status, fields);
    }
    merge(
        &mut status,
        &json!({ "ok": true, "output": output, "total": total, "truncated": from > 0 || end < total, "offset": from, "next": (end < total).then_some(end), "logPath": null }),
    );
    Some(status)
}

pub fn job_stop(ctx: &GmCtx, name: &str, reason: &str) -> Option<Value> {
    let job = background(name)?;
    if job.settled.borrow().is_some() {
        return Some(json!({ "ok": false, "job": name, "state": state_of(&job), "stopped": false, "error": format!("job {name} already ended ({})", state_of(&job)) }));
    }
    job.body.request_stop();
    let tail = job.body.calls_text();
    settle(ctx, name, &job, json!({ "state": "killed", "reason": reason }));
    ctx.run.event("job-killed", json!({ "job": name, "reason": reason, "tail": tail_of(&tail, 2000) }));
    Some(json!({ "ok": true, "job": name, "state": "killed", "stopped": true }))
}

pub fn stop_all(ctx: &GmCtx, reason: &str) -> Vec<String> {
    let names: Vec<String> = table().lock().unwrap_or_else(|e| e.into_inner()).keys().cloned().collect();
    names.into_iter().filter(|name| job_stop(ctx, name, reason).map(|v| v["stopped"] == json!(true)).unwrap_or(false)).collect()
}

fn tail_of(text: &str, cap: usize) -> String {
    let count = text.chars().count();
    text.chars().skip(count.saturating_sub(cap)).collect()
}

async fn call_verb(ctx: Arc<GmCtx>, body: Arc<BodyState>, name: String, args: Value) -> Value {
    if body.stopped() {
        return json!({ "ok": false, "error": "the body was stopped" });
    }
    let action = if crate::verbs::is_read_only(&name) {
        None
    } else {
        Some(ctx.run.event("action", json!({ "tool": name, "args": args, "body": body.seq })))
    };
    let result = crate::verbs::call(ctx.clone(), &name, args.clone()).await;
    let shown = crate::mcp::logged_for(&name, &result);
    let ok = shown.get("ok") == Some(&Value::Bool(true));
    let mut event = json!({ "tool": name, "ok": ok, "args": args, "body": body.seq });
    for key in ["exitCode", "signal", "output", "outputChars", "outputBytes", "outputDigest", "error"] {
        if let Some(v) = shown.get(key) {
            event[key] = v.clone();
        }
    }
    if let Some(seq) = action {
        event["actionSeq"] = json!(seq);
    }
    let seq = ctx.run.event("code-call", event);
    let mut call = json!({ "seq": seq, "tool": name, "ok": ok });
    for key in ["output", "error"] {
        if let Some(v) = shown.get(key).filter(|v| !v.is_null()) {
            call[key] = v.clone();
        }
    }
    body.record(call);
    result
}

fn inside(root: &Path, at: &Path) -> bool {
    at.starts_with(root)
}

pub fn format_of(project: &Path, file: &Path) -> &'static str {
    let name = file.to_string_lossy();
    if name.ends_with(".mjs") {
        return "esm";
    }
    if name.ends_with(".cjs") || name.ends_with(".json") {
        return "cjs";
    }
    let top = if inside(project, file) { Some(project) } else { None };
    let mut dir = file.parent();
    while let Some(at) = dir {
        let pkg = at.join("package.json");
        if pkg.is_file() {
            let kind = std::fs::read_to_string(&pkg)
                .ok()
                .and_then(|text| serde_json::from_str::<Value>(&text).ok())
                .and_then(|v| v.get("type").and_then(Value::as_str).map(str::to_string));
            return if kind.as_deref() == Some("module") { "esm" } else { "cjs" };
        }
        if Some(at) == top {
            return "cjs";
        }
        dir = at.parent();
    }
    "cjs"
}

fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for part in path.components() {
        match part {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

fn resolve_from(base: &Path, spec: &str) -> PathBuf {
    normalize(&base.join(spec))
}

fn is_path_spec(spec: &str) -> bool {
    spec.starts_with("./") || spec.starts_with("../") || spec == "." || spec == ".." || spec.starts_with('/') || Path::new(spec).is_absolute()
}

fn mask_strings(text: &str) -> Vec<u8> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut state = 0u8;
    let mut close = b'"';
    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i];
        let next = bytes.get(i + 1).copied().unwrap_or(0);
        match state {
            0 => {
                if ch == b'\'' || ch == b'"' || ch == b'`' {
                    state = 3;
                    close = ch;
                    out.push(ch);
                } else if ch == b'/' && next == b'/' {
                    state = 1;
                    out.extend_from_slice(b"  ");
                    i += 1;
                } else if ch == b'/' && next == b'*' {
                    state = 2;
                    out.extend_from_slice(b"  ");
                    i += 1;
                } else {
                    out.push(ch);
                }
            }
            1 => {
                if ch == b'\n' {
                    state = 0;
                    out.push(b'\n');
                } else {
                    out.push(b' ');
                }
            }
            2 => {
                if ch == b'*' && next == b'/' {
                    state = 0;
                    out.extend_from_slice(b"  ");
                    i += 1;
                } else {
                    out.push(if ch == b'\n' { b'\n' } else { b' ' });
                }
            }
            _ => {
                if ch == b'\\' {
                    out.push(b' ');
                    if i + 1 < bytes.len() {
                        out.push(b' ');
                    }
                    i += 1;
                } else if ch == close {
                    state = 0;
                    out.push(ch);
                } else {
                    out.push(if ch == b'\n' { b'\n' } else { b' ' });
                }
            }
        }
        i += 1;
    }
    out.truncate(bytes.len());
    out
}

fn literal_at(text: &str, at: usize) -> Option<(usize, String)> {
    let bytes = text.as_bytes();
    let quote = *bytes.get(at)?;
    if quote != b'\'' && quote != b'"' {
        return None;
    }
    let rest = &text[at + 1..];
    let end = rest.find(|c: char| c as u32 == quote as u32 || c == '\n')?;
    if rest.as_bytes()[end] != quote || end == 0 {
        return None;
    }
    let spec = &rest[..end];
    if spec.contains('\'') || spec.contains('"') {
        return None;
    }
    Some((at + end + 2, spec.to_string()))
}

struct Split {
    imports: String,
    code: String,
    loads: Vec<(PathBuf, &'static str)>,
}

fn regex(pattern: &'static str, slot: &'static OnceLock<Regex>) -> &'static Regex {
    slot.get_or_init(|| Regex::new(pattern).expect("static regex"))
}

fn split_body(body: &str, project: &Path) -> Result<Split, (bool, String)> {
    static CLOSED: OnceLock<Regex> = OnceLock::new();
    static IMPORT_LINE: OnceLock<Regex> = OnceLock::new();
    static STATIC_SPEC: OnceLock<Regex> = OnceLock::new();
    let closed = regex(r#"['"][^'"\n]+['"]\s*;?\s*$"#, &CLOSED);
    let import_line = regex(r"^\s*import\s", &IMPORT_LINE);
    let static_spec = regex(r#"(['"])(\.{0,2}/[^'"\n]*)['"]"#, &STATIC_SPEC);
    let mut statements: Vec<String> = Vec::new();
    let mut code: Vec<&str> = Vec::new();
    let mut pending: Option<String> = None;
    let mut started = false;
    for line in body.split('\n') {
        if let Some(open) = pending.as_mut() {
            open.push('\n');
            open.push_str(line);
            if closed.is_match(line) {
                statements.push(pending.take().unwrap_or_default());
            }
            continue;
        }
        if !started && import_line.is_match(line) {
            if closed.is_match(line) {
                statements.push(line.to_string());
            } else {
                pending = Some(line.to_string());
            }
            continue;
        }
        let trimmed = line.trim();
        if !trimmed.is_empty() && !trimmed.starts_with("//") {
            started = true;
        }
        code.push(line);
    }
    if pending.is_some() {
        return Err((false, "an import statement is not closed".to_string()));
    }
    let mut loads: Vec<(PathBuf, &'static str)> = Vec::new();
    let mut imports = Vec::new();
    for text in statements {
        let rewritten = static_spec.replace_all(&text, |caps: &regex::Captures| {
            let at = resolve_from(project, &caps[2]);
            let name = module_name(&at);
            loads.push((at, "import"));
            json!(name).to_string()
        });
        imports.push(rewritten.into_owned());
    }
    let mut rest = code.join("\n");
    let mut refusal: Option<&'static str> = None;
    for keyword in ["import", "require"] {
        let masked = mask_strings(&rest);
        let mut edits: Vec<(usize, usize, String)> = Vec::new();
        for start in keyword_calls(&masked, keyword.as_bytes()) {
            match literal_call(&rest, start, keyword.len()) {
                None => {
                    refusal = refusal.or(Some(if keyword == "import" { "a dynamic import() needs a literal specifier" } else { "a require() needs a literal path" }));
                }
                Some((end, spec)) => {
                    if spec.starts_with("./") || spec.starts_with("../") || spec.starts_with('/') {
                        let at = resolve_from(project, &spec);
                        let name = module_name(&at);
                        loads.push((at, if keyword == "import" { "import()" } else { "require" }));
                        edits.push((start, end, format!("{keyword}({})", json!(name))));
                    }
                }
            }
        }
        for (start, end, text) in edits.into_iter().rev() {
            rest.replace_range(start..end, &text);
        }
    }
    if let Some(reason) = refusal {
        return Err((true, reason.to_string()));
    }
    Ok(Split { imports: imports.join("\n"), code: rest, loads })
}

fn is_ident(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

fn keyword_calls(masked: &[u8], keyword: &[u8]) -> Vec<usize> {
    let mut found = Vec::new();
    let mut i = 0;
    while i + keyword.len() <= masked.len() {
        if &masked[i..i + keyword.len()] == keyword && (i == 0 || !is_ident(masked[i - 1])) {
            let mut j = i + keyword.len();
            if j < masked.len() && is_ident(masked[j]) {
                i += 1;
                continue;
            }
            while j < masked.len() && masked[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < masked.len() && masked[j] == b'(' {
                found.push(i);
            }
        }
        i += 1;
    }
    found
}

fn literal_call(text: &str, start: usize, keyword_len: usize) -> Option<(usize, String)> {
    let bytes = text.as_bytes();
    let mut j = start + keyword_len;
    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    if bytes.get(j) != Some(&b'(') {
        return None;
    }
    j += 1;
    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    let (mut end, spec) = literal_at(text, j)?;
    while end < bytes.len() && bytes[end].is_ascii_whitespace() {
        end += 1;
    }
    if bytes.get(end) != Some(&b')') {
        return None;
    }
    Some((end + 1, spec))
}

fn module_name(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn builtin(spec: &str) -> bool {
    spec.starts_with("node:") || NODE_BUILTINS.contains(&spec)
}

fn package_entry(dir: &Path, esm: bool) -> Option<PathBuf> {
    let text = std::fs::read_to_string(dir.join("package.json")).ok()?;
    let pkg: Value = serde_json::from_str(&text).ok()?;
    let pick = |v: &Value| -> Option<String> {
        match v {
            Value::String(s) => Some(s.clone()),
            Value::Object(map) => {
                let keys: &[&str] = if esm { &["import", "default", "require"] } else { &["require", "default", "import"] };
                keys.iter().find_map(|k| map.get(*k)).and_then(|inner| match inner {
                    Value::String(s) => Some(s.clone()),
                    Value::Object(nested) => nested.get("default").and_then(Value::as_str).map(str::to_string),
                    _ => None,
                })
            }
            _ => None,
        }
    };
    let exported = pkg.get("exports").and_then(|exports| match exports {
        Value::Object(map) if map.contains_key(".") => pick(&map["."]),
        other => pick(other),
    });
    let field = if esm { pkg.get("module").and_then(Value::as_str).map(str::to_string) } else { None };
    let main = pkg.get("main").and_then(Value::as_str).map(str::to_string);
    let entry = exported.or(field).or(main)?;
    Some(resolve_from(dir, &entry))
}

fn as_file(candidate: &Path, esm: bool) -> Option<PathBuf> {
    if candidate.is_file() {
        return Some(candidate.to_path_buf());
    }
    let name = candidate.to_string_lossy().to_string();
    for ext in [".js", ".mjs", ".cjs", ".json"] {
        let with = PathBuf::from(format!("{name}{ext}"));
        if with.is_file() {
            return Some(with);
        }
    }
    if candidate.is_dir() {
        if let Some(entry) = package_entry(candidate, esm) {
            if let Some(found) = as_file(&entry, esm) {
                return Some(found);
            }
        }
        for index in ["index.js", "index.mjs", "index.cjs", "index.json"] {
            let at = candidate.join(index);
            if at.is_file() {
                return Some(at);
            }
        }
    }
    None
}

pub fn resolve_spec(dir: &Path, spec: &str, esm: bool) -> Result<PathBuf, String> {
    if builtin(spec) {
        return Err(format!("{spec} is a Node builtin, which the QuickJS engine does not have; use the gm verbs"));
    }
    if is_path_spec(spec) {
        let at = resolve_from(dir, spec);
        return as_file(&at, esm).ok_or_else(|| format!("cannot find module {}", module_name(&at)));
    }
    let mut at = Some(dir);
    while let Some(base) = at {
        let candidate = base.join("node_modules").join(spec);
        if let Some(found) = as_file(&candidate, esm) {
            return Ok(normalize(&found));
        }
        at = base.parent();
    }
    Err(format!("cannot find package {spec} from {}", module_name(dir)))
}

fn link_source(source: &str, dir: &Path) -> Result<(String, Vec<PathBuf>), String> {
    let masked = mask_strings(source);
    let bytes = source.as_bytes();
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut deps = Vec::new();
    let mut i = 0;
    while i < masked.len() {
        let rest = &masked[i..];
        let word = if rest.starts_with(b"import") {
            6
        } else if rest.starts_with(b"from") {
            4
        } else {
            0
        };
        let boundary = i == 0 || (!is_ident(masked[i - 1]) && masked[i - 1] != b'.');
        if word == 0 || !boundary || masked.get(i + word).map(|b| is_ident(*b)).unwrap_or(false) {
            i += 1;
            continue;
        }
        let mut j = i + word;
        while j < masked.len() && masked[j].is_ascii_whitespace() {
            j += 1;
        }
        if word == 6 && masked.get(j) == Some(&b'(') {
            j += 1;
            while j < masked.len() && masked[j].is_ascii_whitespace() {
                j += 1;
            }
        }
        if matches!(bytes.get(j), Some(b'\'') | Some(b'"')) {
            if let Some((end, spec)) = literal_at(source, j) {
                let at = resolve_spec(dir, &spec, true)?;
                edits.push((j, end, json!(module_name(&at)).to_string()));
                deps.push(at);
                i = end;
                continue;
            }
        }
        i += word;
    }
    let mut out = source.to_string();
    for (start, end, text) in edits.into_iter().rev() {
        out.replace_range(start..end, &text);
    }
    Ok((out, deps))
}

fn strip_shebang(source: String) -> String {
    if source.starts_with("#!") {
        format!("//{source}")
    } else {
        source
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
    match thrown.get::<rquickjs::Coerced<String>>() {
        Ok(text) => text.0,
        Err(_) => "the body threw a value that is not text".to_string(),
    }
}

fn declare_tree<'js>(ctx: &Ctx<'js>, project: &Path, file: &Path, seen: &mut HashSet<String>) -> Result<(), String> {
    let name = module_name(file);
    if !seen.insert(name.clone()) {
        return Ok(());
    }
    let source = if format_of(project, file) == "cjs" {
        format!("export default globalThis.__gm_require_file({});\n", json!(name))
    } else {
        let text = std::fs::read_to_string(file).map_err(|e| format!("cannot read {name}: {e}"))?;
        let dir = file.parent().unwrap_or(project);
        let (linked, deps) = link_source(&strip_shebang(text), dir)?;
        for dep in deps {
            declare_tree(ctx, project, &dep, seen)?;
        }
        linked
    };
    Module::declare(ctx.clone(), name.clone(), source).map(|_| ()).map_err(|e| format!("{name}: {}", caught(ctx, e)))
}

const PRELUDE: &str = r#"(function (project, verbsText) {
  const native = { call: globalThis.__gm_call, read: globalThis.__gm_read, resolve: globalThis.__gm_resolve, format: globalThis.__gm_format, timer: globalThis.__gm_timer, clear: globalThis.__gm_clear };
  for (const key of ['__gm_call', '__gm_read', '__gm_resolve', '__gm_format', '__gm_timer', '__gm_clear']) delete globalThis[key];
  const noop = () => {};
  globalThis.console = { log: noop, info: noop, warn: noop, error: noop, debug: noop, trace: noop, dir: noop, table: noop, time: noop, timeEnd: noop };
  globalThis.setTimeout = (fn, ms, ...args) => native.timer(() => fn(...args), Number(ms) || 0, false);
  globalThis.setInterval = (fn, ms, ...args) => native.timer(() => fn(...args), Number(ms) || 0, true);
  globalThis.clearTimeout = (id) => { if (typeof id === 'number') native.clear(id); };
  globalThis.clearInterval = globalThis.clearTimeout;
  if (typeof globalThis.queueMicrotask !== 'function') globalThis.queueMicrotask = (fn) => { Promise.resolve().then(fn); };
  const verbs = JSON.parse(verbsText);
  const gm = { project, verbs: () => verbs.map((verb) => ({ ...verb })) };
  for (const { name } of verbs) gm[name] = (args = {}) => native.call(name, JSON.stringify(args === undefined || args === null ? {} : args)).then((text) => JSON.parse(text));
  gm.try = async (fn) => {
    try {
      return await fn();
    } catch (err) {
      return { ok: false, error: err !== null && typeof err === 'object' && 'message' in err ? String(err.message) : String(err) };
    }
  };
  const dirOf = (file) => {
    const at = file.lastIndexOf('/');
    return at <= 0 ? '/' : file.slice(0, at);
  };
  const cache = new Map();
  const load = (file) => {
    const hit = cache.get(file);
    if (hit !== undefined) return hit.exports;
    if (native.format(file) === 'esm') throw new Error(`require() cannot load an ES module: ${file}`);
    const module = { exports: {}, filename: file, id: file, loaded: false };
    cache.set(file, module);
    const source = native.read(file);
    if (file.endsWith('.json')) {
      module.exports = JSON.parse(source);
    } else {
      const dir = dirOf(file);
      const wrapped = (0, eval)(`(function (exports, require, module, __filename, __dirname) {${source.startsWith('#!') ? '//' + source : source}\n})`);
      wrapped.call(module.exports, module.exports, requireFrom(dir), module, file, dir);
    }
    module.loaded = true;
    return module.exports;
  };
  const requireFrom = (dir) => (spec) => {
    if (typeof spec !== 'string') throw new TypeError('require needs a string path');
    return load(native.resolve(dir, spec));
  };
  globalThis.__gm_require_file = load;
  return { gm, require: requireFrom(project) };
})"#;

const MAIN: &str = r#"(async (body, env) => {
  const value = await body(env.gm, env.require);
  const text = JSON.stringify(value);
  return text === undefined ? 'undefined' : text;
})"#;

struct Timer<'js> {
    id: u32,
    due: Instant,
    every: Option<Duration>,
    func: Function<'js>,
}

struct Engine {
    gm: Arc<GmCtx>,
    body: Arc<BodyState>,
    handle: Handle,
    project: PathBuf,
    verbs_json: String,
}

struct Loop<'js> {
    pending: Rc<RefCell<HashMap<u64, Function<'js>>>>,
    timers: Rc<RefCell<Vec<Timer<'js>>>>,
    replies: mpsc::Receiver<(u64, String)>,
}

fn throw<'js>(ctx: &Ctx<'js>, message: &str) -> rquickjs::Error {
    Exception::throw_message(ctx, message)
}

fn install<'js>(ctx: &Ctx<'js>, engine: &Engine) -> rquickjs::Result<Loop<'js>> {
    let (tx, replies) = mpsc::channel::<(u64, String)>();
    let pending: Rc<RefCell<HashMap<u64, Function<'js>>>> = Rc::new(RefCell::new(HashMap::new()));
    let timers: Rc<RefCell<Vec<Timer<'js>>>> = Rc::new(RefCell::new(Vec::new()));
    let globals = ctx.globals();
    let next_call = Rc::new(AtomicU64::new(1));
    {
        let pending = pending.clone();
        let gm = engine.gm.clone();
        let body = engine.body.clone();
        let handle = engine.handle.clone();
        let call = Function::new(ctx.clone(), move |c: Ctx<'js>, name: String, args: String| -> rquickjs::Result<Promise<'js>> {
            let (promise, resolve, _reject) = c.promise()?;
            let id = next_call.fetch_add(1, Ordering::Relaxed);
            pending.borrow_mut().insert(id, resolve);
            let args: Value = serde_json::from_str(&args).unwrap_or_else(|_| json!({}));
            let tx = tx.clone();
            let future = call_verb(gm.clone(), body.clone(), name, args);
            handle.spawn(async move {
                let value = future.await;
                let _ = tx.send((id, value.to_string()));
            });
            Ok(promise)
        })?;
        globals.set("__gm_call", call)?;
    }
    {
        let read = Function::new(ctx.clone(), |c: Ctx<'js>, path: String| -> rquickjs::Result<String> {
            std::fs::read_to_string(&path).map_err(|e| throw(&c, &format!("cannot read {path}: {e}")))
        })?;
        globals.set("__gm_read", read)?;
    }
    {
        let resolve = Function::new(ctx.clone(), |c: Ctx<'js>, dir: String, spec: String| -> rquickjs::Result<String> {
            resolve_spec(Path::new(&dir), &spec, false).map(|at| module_name(&at)).map_err(|e| throw(&c, &e))
        })?;
        globals.set("__gm_resolve", resolve)?;
    }
    {
        let project = engine.project.clone();
        let format = Function::new(ctx.clone(), move |path: String| -> String { format_of(&project, Path::new(&path)).to_string() })?;
        globals.set("__gm_format", format)?;
    }
    {
        let timers = timers.clone();
        let next_timer = Rc::new(std::cell::Cell::new(1u32));
        let timer = Function::new(ctx.clone(), move |func: Function<'js>, ms: f64, repeat: bool| -> u32 {
            let id = next_timer.get();
            next_timer.set(id.wrapping_add(1));
            let wait = Duration::from_millis(if ms.is_finite() && ms > 0.0 { ms as u64 } else { 0 });
            timers.borrow_mut().push(Timer { id, due: Instant::now() + wait, every: if repeat { Some(wait.max(Duration::from_millis(1))) } else { None }, func });
            id
        })?;
        globals.set("__gm_timer", timer)?;
    }
    {
        let timers = timers.clone();
        let clear = Function::new(ctx.clone(), move |id: u32| {
            timers.borrow_mut().retain(|t| t.id != id);
        })?;
        globals.set("__gm_clear", clear)?;
    }
    Ok(Loop { pending, timers, replies })
}

fn drive<'js>(ctx: &Ctx<'js>, state: &Loop<'js>, body: &BodyState, promise: &Promise<'js>) -> Result<JsValue<'js>, String> {
    loop {
        while ctx.execute_pending_job() {}
        if body.stopped() {
            return Err("the body was stopped".to_string());
        }
        match promise.state() {
            PromiseState::Resolved => return promise.result::<JsValue>().unwrap_or(Ok(JsValue::new_undefined(ctx.clone()))).map_err(|e| caught(ctx, e)),
            PromiseState::Rejected => {
                return match promise.result::<JsValue>() {
                    Some(Err(e)) => Err(caught(ctx, e)),
                    _ => Err("the body failed".to_string()),
                }
            }
            PromiseState::Pending => {}
        }
        let now = Instant::now();
        let due: Vec<Function<'js>> = {
            let mut timers = state.timers.borrow_mut();
            let mut fired = Vec::new();
            let mut kept = Vec::with_capacity(timers.len());
            for mut timer in timers.drain(..) {
                if timer.due <= now {
                    fired.push(timer.func.clone());
                    if let Some(every) = timer.every {
                        timer.due = now + every;
                        kept.push(timer);
                    }
                } else {
                    kept.push(timer);
                }
            }
            *timers = kept;
            fired
        };
        if !due.is_empty() {
            for func in due {
                func.call::<_, ()>(()).map_err(|e| caught(ctx, e))?;
            }
            continue;
        }
        let in_flight = !state.pending.borrow().is_empty();
        let next_due = state.timers.borrow().iter().map(|t| t.due).min();
        if !in_flight && next_due.is_none() {
            return Err("the body stopped before it returned: it awaits a promise that nothing settles".to_string());
        }
        let wait = next_due.map(|due| due.saturating_duration_since(now)).unwrap_or(LOOP_TICK).min(LOOP_TICK);
        let mut replies = Vec::new();
        match state.replies.recv_timeout(wait) {
            Ok(reply) => replies.push(reply),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {}
        }
        while let Ok(reply) = state.replies.try_recv() {
            replies.push(reply);
        }
        for (id, text) in replies {
            let resolve = state.pending.borrow_mut().remove(&id);
            if let Some(resolve) = resolve {
                resolve.call::<_, ()>((text,)).map_err(|e| caught(ctx, e))?;
            }
        }
    }
}

fn execute<'js>(ctx: Ctx<'js>, engine: &Engine, kind: &str, split: &Split) -> Result<String, String> {
    let state = install(&ctx, engine).map_err(|e| caught(&ctx, e))?;
    let prelude: Function = ctx.eval(PRELUDE).map_err(|e| format!("the body does not load: {}", caught(&ctx, e)))?;
    let project_name = module_name(&engine.project);
    let env: Object = prelude.call((project_name.clone(), engine.verbs_json.clone())).map_err(|e| caught(&ctx, e))?;
    let body_fn: Function = if kind == "esm" {
        let source = format!("{}\nexport default async function body(gm, require) {{\n{}\n}}\n", split.imports, split.code);
        let (linked, deps) = link_source(&source, &engine.project).map_err(|e| format!("the body does not load: {e}"))?;
        let mut seen = HashSet::new();
        for dep in deps {
            declare_tree(&ctx, &engine.project, &dep, &mut seen).map_err(|e| format!("the body does not load: {e}"))?;
        }
        let module = Module::declare(ctx.clone(), format!("{}/[body]", project_name), linked)
            .map_err(|e| format!("the body does not load: {}", caught(&ctx, e)))?;
        let (module, evaluated) = module.eval().map_err(|e| format!("the body does not load: {}", caught(&ctx, e)))?;
        drive(&ctx, &state, &engine.body, &evaluated).map_err(|e| format!("the body does not load: {e}"))?;
        module.get("default").map_err(|e| format!("the body does not load: {}", caught(&ctx, e)))?
    } else {
        let (linked, deps) = link_source(&split.code, &engine.project).map_err(|e| format!("the body does not load: {e}"))?;
        let mut seen = HashSet::new();
        for dep in deps {
            declare_tree(&ctx, &engine.project, &dep, &mut seen).map_err(|e| format!("the body does not load: {e}"))?;
        }
        let source = format!("(async function body(gm, require) {{\n{}\n}})", linked);
        ctx.eval(source).map_err(|e| format!("the body does not load: {}", caught(&ctx, e)))?
    };
    let main: Function = ctx.eval(MAIN).map_err(|e| caught(&ctx, e))?;
    let running: Promise = main.call((body_fn, env)).map_err(|e| caught(&ctx, e))?;
    let value = drive(&ctx, &state, &engine.body, &running)?;
    value.get::<String>().map_err(|e| caught(&ctx, e))
}

fn run_thread(engine: Engine, kind: String, split: Split) -> Result<String, String> {
    let runtime = Runtime::new().map_err(|e| e.to_string())?;
    runtime.set_memory_limit(BODY_MEMORY_BYTES);
    runtime.set_max_stack_size(BODY_JS_STACK_BYTES);
    let stop = engine.body.clone();
    runtime.set_interrupt_handler(Some(Box::new(move || stop.stopped())));
    let context = Context::full(&runtime).map_err(|e| e.to_string())?;
    context.with(|ctx| execute(ctx, &engine, &kind, &split))
}

fn reply_of(outcome: Result<String, String>) -> Value {
    match outcome {
        Ok(text) => {
            let count = text.chars().count();
            let output: String = if count > OUTPUT_CAP { text.chars().take(OUTPUT_CAP).collect() } else { text };
            json!({ "ok": true, "output": output, "truncated": count > OUTPUT_CAP })
        }
        Err(error) => json!({ "ok": false, "error": error }),
    }
}

fn with_loads(mut reply: Value, loads: &[Value]) -> Value {
    reply["loads"] = Value::Array(loads.to_vec());
    reply
}

pub async fn run_body(ctx: Arc<GmCtx>, body: String, format: Option<String>, timeout_ms: u64) -> Result<Value, String> {
    if body.trim().is_empty() {
        return Ok(json!({ "ok": false, "error": "body must be a non-empty string" }));
    }
    if !(1..=MAX_TIMEOUT_MS).contains(&timeout_ms) {
        return Ok(json!({ "ok": false, "error": format!("timeoutMs must be an integer from 1 to {MAX_TIMEOUT_MS}") }));
    }
    let project = normalize(&ctx.workspace);
    let kind = match format.as_deref() {
        None => format_of(&project, &project.join("body.js")).to_string(),
        Some("esm") => "esm".to_string(),
        Some("cjs") => "cjs".to_string(),
        Some(_) => return Ok(json!({ "ok": false, "error": "format must be esm or cjs" })),
    };
    let split = match split_body(&body, &project) {
        Ok(split) => split,
        Err((true, refusal)) => {
            ctx.run.event("refusal", json!({ "tool": "run_code", "stage": "load", "error": refusal }));
            return Ok(json!({ "ok": false, "error": format!("refused: {refusal}") }));
        }
        Err((false, error)) => return Ok(json!({ "ok": false, "error": format!("the body does not load: {error}") })),
    };
    if kind == "cjs" && !split.imports.is_empty() {
        let error = "a CommonJS body cannot use a static import; load with require()";
        ctx.run.event("refusal", json!({ "tool": "run_code", "stage": "load", "error": error }));
        return Ok(json!({ "ok": false, "error": format!("refused: {error}") }));
    }
    let loads: Vec<Value> = split
        .loads
        .iter()
        .map(|(path, via)| {
            let shown = match path.strip_prefix(&project) {
                Ok(rel) => module_name(rel),
                Err(_) => module_name(path),
            };
            json!({ "path": shown, "via": via, "format": format_of(&project, path) })
        })
        .collect();
    for load in &loads {
        ctx.run.event("code-load", load.clone());
    }
    let seq = ctx.run.event("body", json!({ "format": kind, "timeoutMs": timeout_ms }));
    let state = Arc::new(BodyState {
        seq,
        stop: AtomicBool::new(false),
        stop_notice: Notify::new(),
        queued: AtomicBool::new(false),
        calls: Mutex::new(Vec::new()),
    });
    let (done_tx, mut done_rx) = oneshot::channel::<Value>();
    let handle = Handle::current();
    let engine = Engine {
        gm: ctx.clone(),
        body: state.clone(),
        handle: handle.clone(),
        project: project.clone(),
        verbs_json: Value::Array(crate::verbs::catalog(&ctx.hub)).to_string(),
    };
    {
        let state = state.clone();
        let ctx = ctx.clone();
        handle.spawn(async move {
            let semaphore = slots();
            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    state.queued.store(true, Ordering::SeqCst);
                    let running = BODY_SLOTS - semaphore.available_permits();
                    ctx.run.event("body-queued", json!({ "body": state.seq, "running": running }));
                    let acquired = tokio::select! {
                        permit = semaphore.clone().acquire_owned() => permit.ok(),
                        _ = state.stop_notice.notified() => None,
                    };
                    match acquired {
                        Some(permit) if !state.stopped() => permit,
                        _ => {
                            let _ = done_tx.send(json!({ "ok": false, "error": "the body was stopped before it started" }));
                            return;
                        }
                    }
                }
            };
            let queued = state.queued.swap(false, Ordering::SeqCst);
            if queued {
                ctx.run.event("body-started", json!({ "body": state.seq, "running": BODY_SLOTS - semaphore.available_permits() }));
            }
            let (thread_tx, thread_rx) = oneshot::channel::<Result<String, String>>();
            let spawned = std::thread::Builder::new().name(format!("gm-body-{}", state.seq)).stack_size(BODY_THREAD_STACK_BYTES).spawn(move || {
                let outcome = run_thread(engine, kind, split);
                drop(permit);
                let _ = thread_tx.send(outcome);
            });
            let outcome = match spawned {
                Ok(_) => thread_rx.await.unwrap_or_else(|_| Err("the body stopped before it returned".to_string())),
                Err(e) => Err(format!("the body thread did not start: {e}")),
            };
            if queued {
                ctx.run.event("body-finished", json!({ "body": state.seq, "running": BODY_SLOTS - semaphore.available_permits() }));
            }
            let _ = done_tx.send(reply_of(outcome));
        });
    }
    match tokio::time::timeout(Duration::from_millis(timeout_ms), &mut done_rx).await {
        Ok(Ok(reply)) => return Ok(with_loads(reply, &loads)),
        Ok(Err(_)) => return Ok(with_loads(json!({ "ok": false, "error": "the body stopped before it returned" }), &loads)),
        Err(_) => {}
    }
    let name = {
        let mut jobs = table().lock().unwrap_or_else(|e| e.into_inner());
        let mut name = format!("body-{seq}");
        let mut n = 2;
        while jobs.contains_key(&name) {
            name = format!("body-{seq}-{n}");
            n += 1;
        }
        let (settled, _) = watch::channel(None);
        let job = Arc::new(BackgroundBody {
            body: state.clone(),
            settled,
            status_file: ctx.run.dir.join("jobs").join(format!("{name}.json")),
            started: Instant::now(),
        });
        jobs.insert(name.clone(), job);
        name
    };
    let job_state = if state.queued.load(Ordering::SeqCst) { "queued" } else { "running" };
    if let Some(job) = background(&name) {
        write_status(&job.status_file, &json!({ "job": name, "body": seq, "state": job_state }));
    }
    ctx.run.event("job", json!({ "job": name, "body": seq, "state": job_state }));
    {
        let ctx = ctx.clone();
        let name = name.clone();
        handle.spawn(async move {
            let reply = done_rx.await.unwrap_or_else(|_| json!({ "ok": false, "error": "the body stopped before it returned" }));
            let fields = if reply["ok"] == json!(true) {
                json!({ "state": "exited", "value": reply["output"], "valueTruncated": reply["truncated"] })
            } else {
                json!({ "state": "failed", "error": reply["error"] })
            };
            if let Some(job) = background(&name) {
                settle(&ctx, &name, &job, fields);
            }
        });
    }
    Ok(json!({ "ok": true, "background": name, "state": job_state, "logged": state.logged(), "loads": loads }))
}
