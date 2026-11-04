use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio::sync::watch;

use crate::feed::output_digest;
use crate::log::{self, LogFile, LogSource, LOG_FILE};

pub const DEFAULT_WITNESS: &str = "Finish only with a witness: one string that states the evidence (command output, file content or query result) showing the task is done.";
const DEFAULT_MAX_STEPS: u64 = 50;
const WINDOW_EVENTS: usize = 20;
const MAX_ACTIONS: usize = 16;
const MAX_RETRIES: u64 = 4;
const MAX_HEADERLESS_429_RETRIES: u64 = 2;
const MAX_RETRY_AFTER_MS: f64 = 60000.0;
const BASE_DELAY_MS: f64 = 1000.0;
const MAX_DELAY_MS: f64 = 16000.0;
const JITTER: f64 = 0.2;
const RETRYABLE_NETWORK: [&str; 6] = ["ECONNRESET", "ETIMEDOUT", "UND_ERR_SOCKET", "UND_ERR_CONNECT_TIMEOUT", "UND_ERR_HEADERS_TIMEOUT", "UND_ERR_BODY_TIMEOUT"];
const DEFAULT_TIMEOUT_MS: u64 = 60000;
const MAX_TIMEOUT_MS: u64 = 600000;
const OUTPUT_PAGE: usize = 20000;
const TAIL_CAP: usize = 2000;
const LOGGED_REPLY_CHARS: usize = 1000;
const OUTPUT_CAP_BYTES: usize = 200000;
const KILL_GRACE_MS: u64 = 500;
const MAX_WAIT_MS: f64 = 30000.0;
const JOB_LIFETIME_MS: u64 = 1800000;
const LOCAL_TOOLS_PATH: &[&str] = &["read_file", "write_file", "edit_file", "list_dir", "stat_file"];
const WRITE_TOOLS: &[&str] = &["write_file", "edit_file"];
const READ_ONLY_TOOLS: &[&str] = &["job_status", "search", "wiki_search", "grep", "fetch", "callers", "impact", "review_join"];
const LOGGED_OUTPUT_TOOLS: &[&str] = &["query_log", "job_status", "run_shell", "run_code"];

const DEFAULT_RULES: &str = r#"# Rules

1. Paths may point inside or outside the workspace. An outside path should be absolute.
2. Verify before you finish: run what you built and read its output.
3. Reply with one JSON object {"tool": "<name>", "args": {...}}, or a JSON array of such objects when the actions are independent. Independent actions run together. An action that needs an earlier result goes in a later reply.
4. finish needs a witness: one string that quotes the evidence you observed. finish must be the only action in its reply.
5. Events older than the window are not in this prompt. Query them with query_log. Do not guess.
6. Never print, store or request secrets, environment variables or API keys.
7. If the task cannot be done, finish with a witness that states why, with the evidence.
"#;

const TOOLS_TEXT: &str = r#"read_file {"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}: text of a file.
write_file {"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path","content"]}: write a file, creating parent directories.
list_dir {"type":"object","properties":{"path":{"type":"string"}},"required":[]}: entries of a directory; directories end with "/".
run_shell {"type":"object","properties":{"command":{"type":"string"},"timeoutMs":{"type":"number"},"background":{"type":"boolean"},"job":{"type":"string"}},"required":["command"]}: run a shell command in the workspace (default timeout 60000 ms). Returns exitCode, signal and output. The command process group is killed when the command returns, so background work must use background true with a job name. Read the job files only after job_status shows exited. A job still running when the run ends is killed.
job_status {"type":"object","properties":{"job":{"type":"string"},"waitMs":{"type":"number"}},"required":["job"]}: state (running, exited or killed), exitCode and the last 20000 characters of output. With waitMs it polls every 100 ms until the job leaves running or waitMs passes (max 30000).
query_log {"type":"object","properties":{"js":{"type":"string"}},"required":["js"]}: JavaScript expression over "events", every log event so far (seq, t, mono, kind, ...). Returns JSON. Kinds: start, reply, action, result, refusal, skipped, parse_error, error, retry, job, pending, summary, end.
finish {"type":"object","properties":{"witness":{"type":"string"}},"required":["witness"]}: end the run. The witness must state the evidence."#;

const REPLY_TEXT: &str = "## Reply\n\nOne JSON object {\"tool\": \"...\", \"args\": {...}}, or a JSON array of them when the actions are independent. Nothing else.";

const DEFAULT_SCRIPT: &str = r#"[[{"tool":"write_file","args":{"path":"hello.mjs","content":"console.log(\"gmnew scripted witness\");\n"}},{"tool":"write_file","args":{"path":"notes.txt","content":"two independent actions in one reply\n"}}],{"tool":"run_shell","args":{"command":"node hello.mjs > out.txt"}},{"tool":"read_file","args":{"path":"out.txt"}},{"tool":"read_file","args":{"path":"../outside.txt"}},{"tool":"query_log","args":{"js":"events.filter((e) => e.kind === 'refusal').length"}},{"tool":"finish","args":{"witness":"node hello.mjs wrote \"gmnew scripted witness\" to out.txt; read_file out.txt returned that line; read_file ../outside.txt was refused (see refusal event)."}}]"#;

const SCRIPT_EXHAUSTED: &str = r#"{"tool":"finish","args":{"witness":"scripted model has no further steps"}}"#;

static EPOCH: OnceLock<Instant> = OnceLock::new();

pub fn mono_now() -> f64 {
    let epoch = EPOCH.get_or_init(Instant::now);
    log::round3(epoch.elapsed().as_secs_f64() * 1000.0)
}

#[derive(Clone, Debug)]
pub enum Ojson {
    Null,
    Bool(bool),
    Num(f64),
    Str(String),
    Arr(Vec<Ojson>),
    Obj(Vec<(String, Ojson)>),
}

struct OjParser<'a> {
    s: &'a [u8],
    at: usize,
}

impl OjParser<'_> {
    fn ws(&mut self) {
        while self.at < self.s.len() && matches!(self.s[self.at], b' ' | b'\t' | b'\n' | b'\r') {
            self.at += 1;
        }
    }

    fn eat(&mut self, word: &str) -> bool {
        if self.s[self.at..].starts_with(word.as_bytes()) {
            self.at += word.len();
            true
        } else {
            false
        }
    }

    fn value(&mut self) -> Option<Ojson> {
        self.ws();
        let byte = *self.s.get(self.at)?;
        match byte {
            b'{' => {
                self.at += 1;
                let mut items = Vec::new();
                self.ws();
                if self.s.get(self.at) == Some(&b'}') {
                    self.at += 1;
                    return Some(Ojson::Obj(items));
                }
                loop {
                    self.ws();
                    let Ojson::Str(key) = self.value()? else { return None };
                    self.ws();
                    if self.s.get(self.at) != Some(&b':') {
                        return None;
                    }
                    self.at += 1;
                    let item = self.value()?;
                    items.push((key, item));
                    self.ws();
                    match self.s.get(self.at)? {
                        b',' => self.at += 1,
                        b'}' => {
                            self.at += 1;
                            return Some(Ojson::Obj(items));
                        }
                        _ => return None,
                    }
                }
            }
            b'[' => {
                self.at += 1;
                let mut items = Vec::new();
                self.ws();
                if self.s.get(self.at) == Some(&b']') {
                    self.at += 1;
                    return Some(Ojson::Arr(items));
                }
                loop {
                    items.push(self.value()?);
                    self.ws();
                    match self.s.get(self.at)? {
                        b',' => self.at += 1,
                        b']' => {
                            self.at += 1;
                            return Some(Ojson::Arr(items));
                        }
                        _ => return None,
                    }
                }
            }
            b'"' => {
                let start = self.at;
                self.at += 1;
                while self.at < self.s.len() && self.s[self.at] != b'"' {
                    self.at += if self.s[self.at] == b'\\' { 2 } else { 1 };
                }
                self.at += 1;
                let end = self.at.min(self.s.len());
                serde_json::from_slice::<String>(&self.s[start..end]).ok().map(Ojson::Str)
            }
            b't' if self.eat("true") => Some(Ojson::Bool(true)),
            b'f' if self.eat("false") => Some(Ojson::Bool(false)),
            b'n' if self.eat("null") => Some(Ojson::Null),
            b'-' | b'0'..=b'9' => {
                let start = self.at;
                while self.at < self.s.len() && matches!(self.s[self.at], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9') {
                    self.at += 1;
                }
                std::str::from_utf8(&self.s[start..self.at]).ok()?.parse::<f64>().ok().map(Ojson::Num)
            }
            _ => None,
        }
    }
}

pub fn parse_ojson(text: &str) -> Option<Ojson> {
    let mut parser = OjParser { s: text.as_bytes(), at: 0 };
    let value = parser.value()?;
    parser.ws();
    (parser.at == text.len()).then_some(value)
}

pub fn num_text(n: f64) -> String {
    if !n.is_finite() {
        "null".to_string()
    } else if n == 0.0 {
        "0".to_string()
    } else {
        format!("{n}")
    }
}

pub fn ojson_text(value: &Ojson) -> String {
    let mut out = String::new();
    write_ojson(value, &mut out);
    out
}

fn write_ojson(value: &Ojson, out: &mut String) {
    match value {
        Ojson::Null => out.push_str("null"),
        Ojson::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Ojson::Num(n) => out.push_str(&num_text(*n)),
        Ojson::Str(s) => out.push_str(&json_str(s)),
        Ojson::Arr(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_ojson(item, out);
            }
            out.push(']');
        }
        Ojson::Obj(items) => {
            out.push('{');
            for (i, (key, item)) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&json_str(key));
                out.push(':');
                write_ojson(item, out);
            }
            out.push('}');
        }
    }
}

fn ojson_to_value(value: &Ojson) -> Value {
    match value {
        Ojson::Null => Value::Null,
        Ojson::Bool(b) => Value::Bool(*b),
        Ojson::Num(n) => log::num(*n),
        Ojson::Str(s) => Value::String(s.clone()),
        Ojson::Arr(items) => Value::Array(items.iter().map(ojson_to_value).collect()),
        Ojson::Obj(items) => Value::Object(items.iter().map(|(k, v)| (k.clone(), ojson_to_value(v))).collect()),
    }
}

pub fn json_str(text: &str) -> String {
    serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string())
}

fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}

fn utf16_slice(text: &[u16], from: usize, to: usize) -> String {
    let begin = from.min(text.len());
    let end = to.min(text.len()).max(begin);
    String::from_utf16_lossy(&text[begin..end])
}

pub fn sha256_hex(text: &str) -> String {
    Sha256::digest(text.as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

pub fn render_prefix(rules: &str, task: &str, witness: &str) -> String {
    format!("{}\n\n## Task\n\n{task}\n\n## Witness requirement\n\n{witness}\n\n## Tools\n\n{TOOLS_TEXT}\n\n{REPLY_TEXT}", rules.trim())
}

pub struct Session {
    file: Mutex<LogFile>,
    run_dir: PathBuf,
    replay_of: Option<String>,
    in_flight: AtomicU64,
    groups: Mutex<BTreeSet<u32>>,
}

impl LogSource for Session {
    fn log_path(&self) -> PathBuf {
        self.run_dir.join(LOG_FILE)
    }

    fn log_count(&self) -> u64 {
        self.file.lock().map(|f| f.count()).unwrap_or(0)
    }

    fn log_bounds(&self, from_seq: u64, to_seq: u64) -> Option<(u64, u64)> {
        self.file.lock().ok()?.bounds(from_seq, to_seq)
    }
}

impl Session {
    pub fn open(run_dir: &Path, replay_of: Option<String>) -> Result<Session, String> {
        let (file, _) = LogFile::open(&run_dir.join(LOG_FILE)).map_err(|e| e.to_string())?;
        Ok(Session { file: Mutex::new(file), run_dir: run_dir.to_path_buf(), replay_of, in_flight: AtomicU64::new(0), groups: Mutex::new(BTreeSet::new()) })
    }

    pub fn enter(&self) {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
    }

    pub fn leave(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn append(&self, kind: &str, fields: Map<String, Value>) -> Result<(), String> {
        self.append_at(kind, fields, None)
    }

    pub fn append_at(&self, kind: &str, fields: Map<String, Value>, mono_at: Option<f64>) -> Result<(), String> {
        let mono = mono_at.unwrap_or_else(mono_now);
        let (seq, line) = {
            let mut file = self.file.lock().map_err(|e| e.to_string())?;
            let seq = file.count() + 1;
            let mut line = format!(
                "{{\"seq\":{seq},\"t\":{},\"mono\":{},\"inFlight\":{},\"kind\":{}",
                json_str(&log::iso_now()),
                num_text(mono),
                self.in_flight.load(Ordering::SeqCst),
                json_str(kind)
            );
            if let Some(replay) = &self.replay_of {
                line.push_str(&format!(",\"replayOf\":{}", json_str(replay)));
            }
            for (key, value) in &fields {
                line.push(',');
                line.push_str(&json_str(key));
                line.push(':');
                line.push_str(&value.to_string());
            }
            if let Some(start) = fields.get("startMono").and_then(Value::as_f64) {
                line.push_str(&format!(",\"durMs\":{}", num_text(log::round3(mono - start))));
            }
            line.push_str("}\n");
            file.append(line.as_bytes()).map_err(|e| e.to_string())?;
            (seq, line)
        };
        if log::is_pending_kind(kind) {
            let record = format!("{{\"id\":{seq},{}", &line[1..]);
            log::append_pending(&self.run_dir, record.as_bytes()).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub fn events(&self) -> Result<Vec<Value>, String> {
        let text = fs::read_to_string(self.run_dir.join(LOG_FILE)).map_err(|e| e.to_string())?;
        text.lines().filter(|l| !l.trim().is_empty()).map(|l| serde_json::from_str(l).map_err(|e| e.to_string())).collect()
    }

    pub fn track_group(&self, pid: u32) {
        if let Ok(mut groups) = self.groups.lock() {
            groups.insert(pid);
        }
    }

    pub fn untrack_group(&self, pid: u32) {
        if let Ok(mut groups) = self.groups.lock() {
            groups.remove(&pid);
        }
    }

    pub fn live_groups(&self) -> Vec<u32> {
        self.groups.lock().map(|g| g.iter().copied().collect()).unwrap_or_default()
    }
}

pub fn obj(pairs: Vec<(&str, Value)>) -> Map<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

pub fn fail_at(op: &str, path: &Path, err: io::Error) -> String {
    match err.kind() {
        io::ErrorKind::NotFound => format!("ENOENT: no such file or directory, {op} '{}'", path.display()),
        _ => err.to_string(),
    }
}

#[derive(Clone)]
enum Entry {
    Text(String),
    Json(Ojson),
}

#[derive(Clone)]
struct Fault {
    status: u16,
    message: Option<String>,
    retry_after_s: Option<f64>,
}

pub struct ModelError {
    message: String,
    status: Option<u16>,
    error_type: Option<String>,
    network: bool,
    retry_after_ms: Option<f64>,
}

fn fault_error(fault: &Fault) -> ModelError {
    ModelError {
        message: fault.message.clone().unwrap_or_else(|| format!("scripted HTTP {}", fault.status)),
        status: Some(fault.status),
        error_type: Some(format!("http_{}", fault.status)),
        network: false,
        retry_after_ms: fault.retry_after_s.map(|s| s * 1000.0),
    }
}

fn fault_of(value: &Ojson) -> Fault {
    match value {
        Ojson::Obj(items) => {
            let field = |name: &str| items.iter().find(|(k, _)| k == name).map(|(_, v)| v);
            Fault {
                status: match field("status") {
                    Some(Ojson::Num(n)) => *n as u16,
                    _ => 0,
                },
                message: match field("message") {
                    Some(Ojson::Str(s)) => Some(s.clone()),
                    _ => None,
                },
                retry_after_s: match field("retryAfter") {
                    Some(Ojson::Num(n)) => Some(*n),
                    _ => None,
                },
            }
        }
        Ojson::Num(n) => Fault { status: *n as u16, message: None, retry_after_s: None },
        _ => Fault { status: 0, message: None, retry_after_s: None },
    }
}

pub struct Scripted {
    replies: Vec<Entry>,
    pending: Vec<Fault>,
    turn: usize,
    latency_ms: u64,
}

impl Scripted {
    fn new(replies: Vec<Entry>, faults: Vec<Fault>, latency_ms: u64) -> Scripted {
        Scripted { replies, pending: faults, turn: 0, latency_ms }
    }

    async fn reply(&mut self) -> Result<String, ModelError> {
        if self.latency_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.latency_ms)).await;
        }
        if !self.pending.is_empty() {
            let fault = self.pending.remove(0);
            return Err(fault_error(&fault));
        }
        let entry = match self.replies.get(self.turn) {
            Some(entry) => entry.clone(),
            None => Entry::Json(parse_ojson(SCRIPT_EXHAUSTED).unwrap_or(Ojson::Null)),
        };
        self.turn += 1;
        match entry {
            Entry::Text(text) => Ok(text),
            Entry::Json(value) => {
                if let Ojson::Obj(items) = &value {
                    if let Some((_, fault)) = items.iter().find(|(k, _)| k == "httpError") {
                        return Err(fault_error(&fault_of(fault)));
                    }
                }
                Ok(ojson_text(&value))
            }
        }
    }
}

pub fn default_replies() -> Vec<Entry> {
    match parse_ojson(DEFAULT_SCRIPT) {
        Some(Ojson::Arr(items)) => items.into_iter().map(Entry::Json).collect(),
        _ => Vec::new(),
    }
}

pub fn replies_from_json(value: Ojson) -> Result<Vec<Entry>, String> {
    match value {
        Ojson::Arr(items) => Ok(items.into_iter().map(|item| match item {
            Ojson::Str(text) => Entry::Text(text),
            other => Entry::Json(other),
        }).collect()),
        _ => Err("--script must be a JSON array of replies".to_string()),
    }
}

enum Plan {
    Stop(String),
    Wait { delay_ms: f64, source: &'static str, headerless: bool },
}

fn retry_plan(err: &ModelError, retries: u64, headerless: u64) -> Plan {
    let rate_limited = err.status == Some(429);
    let retryable_status = matches!(err.status, Some(408) | Some(409) | Some(500..=599));
    let network = err.network && err.error_type.as_deref().is_some_and(|t| RETRYABLE_NETWORK.contains(&t));
    if !(rate_limited || retryable_status || network) {
        let where_ = match err.status {
            None => "network".to_string(),
            Some(status) => format!("HTTP {status}"),
        };
        return Plan::Stop(format!("not retryable: {where_} {}", err.error_type.as_deref().unwrap_or("")).trim().to_string());
    }
    if let (true, Some(after)) = (rate_limited, err.retry_after_ms) {
        if after > MAX_RETRY_AFTER_MS {
            return Plan::Stop(format!("retry-after of {} s is over {} s", (after / 1000.0 + 0.5).floor(), MAX_RETRY_AFTER_MS / 1000.0));
        }
        if retries >= MAX_RETRIES {
            return Plan::Stop(format!("{MAX_RETRIES} retries used"));
        }
        return Plan::Wait { delay_ms: after, source: "retry-after", headerless: false };
    }
    if rate_limited && headerless >= MAX_HEADERLESS_429_RETRIES {
        return Plan::Stop(format!("429 without retry-after, after {MAX_HEADERLESS_429_RETRIES} retries"));
    }
    if retries >= MAX_RETRIES {
        return Plan::Stop(format!("{MAX_RETRIES} retries used"));
    }
    Plan::Wait { delay_ms: backoff_ms(retries), source: "backoff", headerless: rate_limited }
}

fn backoff_ms(retries: u64) -> f64 {
    let base = (BASE_DELAY_MS * 2f64.powi(retries as i32)).min(MAX_DELAY_MS);
    let unit = match u64::from_str_radix(&log::random_hex(8), 16) {
        Ok(n) => n as f64 / u64::MAX as f64,
        Err(_) => 0.5,
    };
    let jittered = base * (1.0 + JITTER * (2.0 * unit - 1.0));
    jittered.round().min(MAX_DELAY_MS)
}

#[derive(Clone, Debug)]
pub struct Action {
    tool: String,
    args: Value,
}

fn is_action(item: &Value) -> bool {
    let tool_ok = item.get("tool").is_some_and(Value::is_string);
    let args_ok = match item.get("args") {
        None | Some(Value::Null) => true,
        Some(Value::Object(_)) => true,
        Some(_) => false,
    };
    tool_ok && args_ok
}

pub fn parse_actions(text: &str) -> Result<Vec<Action>, String> {
    let trimmed = text.trim();
    let candidates = [trimmed.to_string(), between(text, '{', '}'), between(text, '[', ']')];
    for candidate in candidates {
        let Ok(value) = serde_json::from_str::<Value>(&candidate) else { continue };
        let list = match value {
            Value::Array(items) => items,
            other => vec![other],
        };
        if list.is_empty() || list.len() > MAX_ACTIONS {
            continue;
        }
        if list.iter().all(is_action) {
            return Ok(list
                .into_iter()
                .map(|item| Action {
                    tool: item.get("tool").and_then(Value::as_str).unwrap_or("").to_string(),
                    args: match item.get("args") {
                        None | Some(Value::Null) => json!({}),
                        Some(args) => args.clone(),
                    },
                })
                .collect());
        }
    }
    Err(format!("reply is not a JSON object or array of 1 to {MAX_ACTIONS} objects, each with a string \"tool\" and an object \"args\""))
}

fn between(text: &str, open: char, close: char) -> String {
    match (text.find(open), text.rfind(close)) {
        (Some(start), Some(end)) if end > start => text[start..=end].to_string(),
        _ => String::new(),
    }
}

#[derive(Clone, PartialEq)]
struct Item {
    path: Vec<String>,
    group: Option<String>,
    write: bool,
}

enum Access {
    Barrier,
    Items(Vec<Item>),
}

fn lexical(root: &Path, rel: &str) -> PathBuf {
    let joined = if Path::new(rel).is_absolute() { PathBuf::from(rel) } else { root.join(rel) };
    let mut out = PathBuf::new();
    for component in joined.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

fn nearest_existing(target: &Path) -> PathBuf {
    let mut current = target.to_path_buf();
    while fs::symlink_metadata(&current).is_err() {
        match current.parent() {
            Some(parent) if parent != current => current = parent.to_path_buf(),
            _ => break,
        }
    }
    current
}

#[cfg(unix)]
fn group_of(file: &Path) -> Option<String> {
    use std::os::unix::fs::MetadataExt;
    fs::metadata(file).ok().map(|m| format!("{}:{}", m.dev(), m.ino()))
}

#[cfg(not(unix))]
fn group_of(file: &Path) -> Option<String> {
    Some(file.display().to_string())
}

fn plan_path(root: &Path, rel: &str) -> Option<(Vec<String>, Option<String>)> {
    if rel.is_empty() {
        return None;
    }
    let target = lexical(root, rel);
    let nearest = nearest_existing(&target);
    let real = fs::canonicalize(&nearest).ok()?;
    let full = real.join(target.strip_prefix(&nearest).ok()?);
    let group = if nearest == target { group_of(&full) } else { None };
    let parts = full.components().map(|c| c.as_os_str().to_string_lossy().into_owned()).collect();
    Some((parts, group))
}

fn access_of(root: &Path, name: &str, args: &Value) -> Access {
    if READ_ONLY_TOOLS.contains(&name) {
        return Access::Items(Vec::new());
    }
    if !LOCAL_TOOLS_PATH.contains(&name) {
        return Access::Barrier;
    }
    let rel = if name == "list_dir" { args.get("path").and_then(Value::as_str).unwrap_or(".") } else { args.get("path").and_then(Value::as_str).unwrap_or("") };
    match plan_path(root, rel) {
        Some((path, group)) => Access::Items(vec![Item { path, group, write: WRITE_TOOLS.contains(&name) }]),
        None => Access::Barrier,
    }
}

fn is_prefix(a: &[String], b: &[String]) -> bool {
    a.len() <= b.len() && a.iter().zip(b).all(|(x, y)| x == y)
}

fn conflicts(touched: &[Item], next: &[Item]) -> bool {
    touched.iter().any(|x| {
        next.iter().any(|y| {
            (x.write || y.write)
                && ((x.group.is_some() && x.group == y.group) || is_prefix(&x.path, &y.path) || is_prefix(&y.path, &x.path))
        })
    })
}

pub fn plan_waves(actions: &[Action], root: &Path, serial: bool) -> Vec<Vec<usize>> {
    if serial {
        return (0..actions.len()).map(|i| vec![i]).collect();
    }
    let mut waves: Vec<Vec<usize>> = Vec::new();
    let mut wave: Vec<usize> = Vec::new();
    let mut touched: Vec<Item> = Vec::new();
    for (i, action) in actions.iter().enumerate() {
        match access_of(root, &action.tool, &action.args) {
            Access::Barrier => {
                if !wave.is_empty() {
                    waves.push(std::mem::take(&mut wave));
                }
                waves.push(vec![i]);
                touched.clear();
            }
            Access::Items(items) => {
                if conflicts(&touched, &items) {
                    waves.push(std::mem::take(&mut wave));
                    touched.clear();
                }
                wave.push(i);
                touched.extend(items);
            }
        }
    }
    if !wave.is_empty() {
        waves.push(wave);
    }
    waves
}

pub struct Job {
    name: String,
    command: String,
    start_mono: f64,
    pid: u32,
    output: PathBuf,
    kill_requested: std::sync::atomic::AtomicBool,
    kill_reason: Mutex<Option<String>>,
    done: watch::Receiver<bool>,
}

impl Job {
    fn settled(&self) -> bool {
        *self.done.borrow()
    }
}

pub struct Tools {
    session: Arc<Session>,
    root: PathBuf,
    run_dir: PathBuf,
    background: bool,
    jobs: Mutex<HashMap<String, Arc<Job>>>,
}

fn refusal(reason: &str) -> Value {
    json!({ "ok": false, "refused": true, "error": format!("refused: {reason}") })
}

fn relative_text(root: &Path, path: &Path) -> String {
    path.strip_prefix(root).unwrap_or(path).display().to_string()
}

fn whole_in(value: Option<&Value>, default: f64, min: f64, max: f64) -> Option<f64> {
    match value {
        None | Some(Value::Null) => Some(default),
        Some(v) => v.as_f64().filter(|n| n.fract() == 0.0 && *n >= min && *n <= max),
    }
}

fn cut_notice(total_bytes: usize, hint: Option<String>) -> Map<String, Value> {
    let mut notice = Map::new();
    notice.insert("truncated".into(), json!(hint.is_some()));
    notice.insert("totalBytes".into(), json!(total_bytes));
    if let Some(hint) = hint {
        notice.insert("hint".into(), json!(hint));
    }
    notice
}

fn valid_job(name: &str) -> bool {
    (1..=64).contains(&name.len()) && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn signal_name(status: &std::process::ExitStatus) -> Value {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        match status.signal() {
            Some(1) => json!("SIGHUP"),
            Some(2) => json!("SIGINT"),
            Some(3) => json!("SIGQUIT"),
            Some(6) => json!("SIGABRT"),
            Some(9) => json!("SIGKILL"),
            Some(11) => json!("SIGSEGV"),
            Some(13) => json!("SIGPIPE"),
            Some(15) => json!("SIGTERM"),
            Some(n) => json!(format!("SIG{n}")),
            None => Value::Null,
        }
    }
    #[cfg(not(unix))]
    {
        let _ = status;
        Value::Null
    }
}

fn shell_command(command: &str, root: &Path) -> tokio::process::Command {
    let mut cmd = if cfg!(windows) {
        let mut cmd = Command::new("cmd");
        cmd.args(["/d", "/s", "/c"]).arg(command);
        cmd
    } else {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(command);
        cmd
    };
    #[cfg(unix)]
    cmd.process_group(0);
    cmd.current_dir(root).env_remove("ANTHROPIC_API_KEY");
    cmd
}

#[cfg(unix)]
fn signal_group(pid: u32, signal: libc::c_int) -> bool {
    unsafe { libc::kill(-(pid as libc::pid_t), signal) == 0 }
}

async fn kill_group(pid: u32) {
    #[cfg(unix)]
    {
        if signal_group(pid, libc::SIGTERM) {
            tokio::time::sleep(Duration::from_millis(KILL_GRACE_MS)).await;
            signal_group(pid, libc::SIGKILL);
        }
    }
    #[cfg(windows)]
    {
        kill_tree_now(pid);
    }
}

pub fn kill_group_now(pid: u32) {
    #[cfg(unix)]
    {
        signal_group(pid, libc::SIGKILL);
    }
    #[cfg(windows)]
    {
        kill_tree_now(pid);
    }
}

#[cfg(windows)]
fn kill_tree_now(pid: u32) {
    let _ = std::process::Command::new("taskkill").args(["/PID", &pid.to_string(), "/T", "/F"]).stdin(std::process::Stdio::null()).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).status();
}

fn pump<R>(mut reader: R, buf: Arc<Mutex<Vec<u8>>>) -> tokio::task::JoinHandle<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut chunk = [0u8; 8192];
        loop {
            match reader.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if let Ok(mut out) = buf.lock() {
                        if out.len() < OUTPUT_CAP_BYTES {
                            out.extend_from_slice(&chunk[..n]);
                        }
                    }
                }
            }
        }
    })
}

fn text_of_buf(buf: &Arc<Mutex<Vec<u8>>>) -> String {
    buf.lock().map(|b| String::from_utf8_lossy(&b).into_owned()).unwrap_or_default()
}

async fn wait_settled(mut done: watch::Receiver<bool>) {
    let _ = done.wait_for(|settled| *settled).await;
}

impl Tools {
    pub fn new(session: Arc<Session>, root: PathBuf, run_dir: PathBuf, background: bool) -> Tools {
        Tools { session, root, run_dir, background, jobs: Mutex::new(HashMap::new()) }
    }

    fn target(&self, value: Option<&Value>) -> Result<PathBuf, Value> {
        match value.and_then(Value::as_str) {
            Some(rel) if !rel.is_empty() => Ok(lexical(&self.root, rel)),
            _ => Err(refusal("path must be a non-empty string")),
        }
    }

    pub async fn run(self: &Arc<Self>, name: &str, args: &Value) -> Value {
        let outcome = match name {
            "read_file" => self.read_file(args),
            "write_file" => self.write_file(args),
            "edit_file" => self.edit_file(args),
            "stat_file" => self.stat_file(args),
            "list_dir" => self.list_dir(args),
            "run_shell" => self.run_shell(args).await,
            "job_status" => self.job_status(args).await,
            "job_stop" => self.job_stop(args).await,
            "query_log" => Ok(log::query_log(self.session.as_ref(), args)),
            "finish" => finish(args),
            _ => return json!({ "ok": false, "error": format!("unknown tool: {name}") }),
        };
        outcome.unwrap_or_else(|error| json!({ "ok": false, "error": error }))
    }

    fn read_file(&self, args: &Value) -> Result<Value, String> {
        let raw = args.get("path").and_then(Value::as_str).unwrap_or("");
        if raw.starts_with("skill://") {
            return Ok(json!({ "ok": false, "error": format!("no served skill file has the uri {raw}") }));
        }
        let path = match self.target(args.get("path")) {
            Ok(path) => path,
            Err(reply) => return Ok(reply),
        };
        let bytes = fs::read(&path).map_err(|e| fail_at("stat", &path, e))?;
        let text = String::from_utf8_lossy(&bytes).into_owned();
        let units: Vec<u16> = text.encode_utf16().collect();
        let total_bytes = bytes.len();
        match args.get("unit").and_then(Value::as_str).unwrap_or("lines") {
            "chars" => {
                let Some(offset) = whole_in(args.get("offset"), 0.0, 0.0, f64::MAX) else {
                    return Ok(json!({ "ok": false, "error": "offset must be a whole number of characters, zero or more" }));
                };
                let Some(limit) = whole_in(args.get("limit"), OUTPUT_PAGE as f64, 1.0, OUTPUT_PAGE as f64) else {
                    return Ok(json!({ "ok": false, "error": format!("limit must be a whole number from 1 to {OUTPUT_PAGE}") }));
                };
                let (offset, limit) = (offset as usize, limit as usize);
                let end = (offset + limit).min(units.len());
                let next = (end < units.len()).then_some(end);
                let hint = next.map(|n| format!("pass unit: \"chars\", offset: {n} and limit: {OUTPUT_PAGE} to read the rest"));
                let mut reply = obj(vec![("ok", json!(true)), ("output", json!(utf16_slice(&units, offset, end))), ("total", json!(units.len())), ("offset", json!(offset)), ("next", json!(next))]);
                reply.extend(cut_notice(total_bytes, hint));
                Ok(Value::Object(reply))
            }
            "lines" => self.read_lines(args, &text, &units, total_bytes),
            _ => Ok(json!({ "ok": false, "error": "unit must be lines or chars" })),
        }
    }

    fn read_lines(&self, args: &Value, text: &str, units: &[u16], total_bytes: usize) -> Result<Value, String> {
        let Some(offset) = whole_in(args.get("offset"), 0.0, 0.0, f64::MAX) else {
            return Ok(json!({ "ok": false, "error": "offset must be a whole number of lines, zero or more" }));
        };
        let limit = match args.get("limit") {
            None | Some(Value::Null) => f64::INFINITY,
            Some(v) => match v.as_f64().filter(|n| n.fract() == 0.0 && *n >= 1.0) {
                Some(n) => n,
                None => return Ok(json!({ "ok": false, "error": "limit must be a whole number of lines, one or more" })),
            },
        };
        let _ = text;
        let offset = offset as usize;
        let mut starts = vec![0usize];
        for (i, unit) in units.iter().enumerate() {
            if *unit == 0x0A {
                starts.push(i + 1);
            }
        }
        if starts.last() == Some(&units.len()) {
            starts.pop();
        }
        let total = starts.len();
        if offset >= total {
            let mut reply = obj(vec![("ok", json!(true)), ("unit", json!("lines")), ("output", json!("")), ("total", json!(total)), ("offset", json!(offset)), ("next", Value::Null)]);
            reply.extend(cut_notice(total_bytes, None));
            return Ok(Value::Object(reply));
        }
        let line_end = |line: usize| if line + 1 < total { starts[line + 1] } else { units.len() };
        let begin = starts[offset];
        if line_end(offset) - begin > OUTPUT_PAGE {
            let next = (offset + 1 < total).then_some(offset + 1);
            let hint = format!("line {offset} is longer than a page: pass unit: \"chars\", offset: {} and limit: {OUTPUT_PAGE} to read the rest", begin + OUTPUT_PAGE);
            let mut reply = obj(vec![("ok", json!(true)), ("unit", json!("lines")), ("output", json!(utf16_slice(units, begin, begin + OUTPUT_PAGE))), ("cutLine", json!(true)), ("total", json!(total)), ("offset", json!(offset)), ("next", json!(next))]);
            reply.extend(cut_notice(total_bytes, Some(hint)));
            return Ok(Value::Object(reply));
        }
        let mut last = offset + 1;
        while last < total && ((last - offset) as f64) < limit && line_end(last) - begin <= OUTPUT_PAGE {
            last += 1;
        }
        let next = (last < total).then_some(last);
        let hint = next.map(|n| format!("pass offset: {n} and limit: {} to read the rest", total - n));
        let mut reply = obj(vec![("ok", json!(true)), ("unit", json!("lines")), ("output", json!(utf16_slice(units, begin, line_end(last - 1)))), ("total", json!(total)), ("offset", json!(offset)), ("next", json!(next))]);
        reply.extend(cut_notice(total_bytes, hint));
        Ok(Value::Object(reply))
    }

    fn write_file(&self, args: &Value) -> Result<Value, String> {
        let path = match self.target(args.get("path")) {
            Ok(path) => path,
            Err(reply) => return Ok(reply),
        };
        let Some(content) = args.get("content").and_then(Value::as_str) else {
            return Ok(json!({ "ok": false, "error": "content must be a string" }));
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| fail_at("mkdir", parent, e))?;
        }
        fs::write(&path, content).map_err(|e| fail_at("open", &path, e))?;
        Ok(json!({ "ok": true, "output": format!("wrote {} ({} chars)", relative_text(&self.root, &path), utf16_len(content)) }))
    }

    fn edit_file(&self, args: &Value) -> Result<Value, String> {
        let path = match self.target(args.get("path")) {
            Ok(path) => path,
            Err(reply) => return Ok(reply),
        };
        let Some(old) = args.get("old").and_then(Value::as_str).filter(|s| !s.is_empty()) else {
            return Ok(json!({ "ok": false, "error": "old must be a non-empty string" }));
        };
        let Some(new) = args.get("new").and_then(Value::as_str) else {
            return Ok(json!({ "ok": false, "error": "new must be a string" }));
        };
        let text = fs::read_to_string(&path).map_err(|e| fail_at("open", &path, e))?;
        let count = text.matches(old).count();
        let rel = args.get("path").and_then(Value::as_str).unwrap_or("");
        if count == 0 {
            return Ok(json!({ "ok": false, "error": format!("old text not found in {rel}") }));
        }
        if count > 1 && args.get("replaceAll") != Some(&Value::Bool(true)) {
            return Ok(json!({ "ok": false, "error": format!("old text occurs {count} times in {rel}; add context to make it unique, or set replaceAll") }));
        }
        fs::write(&path, text.replace(old, new)).map_err(|e| fail_at("open", &path, e))?;
        let plural = if count == 1 { "" } else { "s" };
        Ok(json!({ "ok": true, "output": format!("edited {} ({count} replacement{plural})", relative_text(&self.root, &path)) }))
    }

    fn stat_file(&self, args: &Value) -> Result<Value, String> {
        let path = match self.target(args.get("path")) {
            Ok(path) => path,
            Err(reply) => return Ok(reply),
        };
        let meta = fs::metadata(&path).map_err(|e| fail_at("stat", &path, e))?;
        let kind = if meta.is_dir() {
            "dir"
        } else if meta.is_file() {
            "file"
        } else {
            "other"
        };
        let modified = meta.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).unwrap_or_default();
        let mtime = log::iso_of(modified.as_secs() as i64, modified.subsec_millis());
        let output = json!({ "type": kind, "size": meta.len(), "mtime": mtime }).to_string();
        Ok(json!({ "ok": true, "output": output }))
    }

    fn list_dir(&self, args: &Value) -> Result<Value, String> {
        let path = match args.get("path") {
            None | Some(Value::Null) => self.root.clone(),
            Some(_) => match self.target(args.get("path")) {
                Ok(path) => path,
                Err(reply) => return Ok(reply),
            },
        };
        let entries = fs::read_dir(&path).map_err(|e| fail_at("scandir", &path, e))?;
        let mut names: Vec<String> = Vec::new();
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
            names.push(if is_dir { format!("{name}/") } else { name });
        }
        names.sort();
        Ok(json!({ "ok": true, "output": names.join("\n") }))
    }

    async fn run_shell(self: &Arc<Self>, args: &Value) -> Result<Value, String> {
        let Some(command) = args.get("command").and_then(Value::as_str).filter(|c| !c.trim().is_empty()) else {
            return Ok(json!({ "ok": false, "error": "command must be a non-empty string" }));
        };
        let requested = args.get("timeoutMs").and_then(Value::as_f64).filter(|n| *n != 0.0 && !n.is_nan()).unwrap_or(DEFAULT_TIMEOUT_MS as f64);
        let timeout_ms = requested.min(MAX_TIMEOUT_MS as f64).max(0.0);
        let background = args.get("background") == Some(&Value::Bool(true));
        if !background || !self.background {
            return Ok(self.foreground(command, timeout_ms as u64).await);
        }
        let job = args.get("job").and_then(Value::as_str).unwrap_or("");
        if !valid_job(job) {
            return Ok(json!({ "ok": false, "error": "a background job needs job: 1 to 64 letters, digits, _ or -" }));
        }
        self.start_job(job, command, timeout_ms as u64)
    }

    async fn foreground(&self, command: &str, timeout_ms: u64) -> Value {
        let mut cmd = shell_command(command, &self.root);
        cmd.stdin(std::process::Stdio::null()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped());
        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(err) => return json!({ "ok": false, "error": err.to_string() }),
        };
        let pid = child.id().unwrap_or(0);
        self.session.track_group(pid);
        let buf = Arc::new(Mutex::new(Vec::new()));
        let readers = [child.stdout.take().map(|r| pump(r, buf.clone())), child.stderr.take().map(|r| pump(r, buf.clone()))];
        let exited = tokio::select! {
            status = child.wait() => Some(status),
            _ = tokio::time::sleep(Duration::from_millis(timeout_ms)) => None,
        };
        let output_of = |buf: &Arc<Mutex<Vec<u8>>>| text_of_buf(buf);
        match exited {
            Some(status) => {
                kill_group_now(pid);
                for reader in readers.into_iter().flatten() {
                    let _ = reader.await;
                }
                self.session.untrack_group(pid);
                let status = status.ok();
                let code = status.and_then(|s| s.code());
                let signal = status.as_ref().map(signal_name).unwrap_or(Value::Null);
                json!({ "ok": code == Some(0), "exitCode": code, "signal": signal, "output": output_of(&buf) })
            }
            None => {
                kill_group(pid).await;
                let _ = child.wait().await;
                self.session.untrack_group(pid);
                json!({ "ok": false, "timedOut": true, "error": format!("timed out after {timeout_ms} ms"), "output": output_of(&buf) })
            }
        }
    }

    fn start_job(self: &Arc<Self>, name: &str, command: &str, timeout_ms: u64) -> Result<Value, String> {
        if self.jobs.lock().map_err(|e| e.to_string())?.contains_key(name) {
            return Ok(json!({ "ok": false, "error": format!("job {name} already exists") }));
        }
        let jobs_dir = self.run_dir.join("jobs");
        fs::create_dir_all(&jobs_dir).map_err(|e| e.to_string())?;
        let output = jobs_dir.join(format!("{name}.out"));
        let out = fs::File::create(&output).map_err(|e| e.to_string())?;
        let err_out = out.try_clone().map_err(|e| e.to_string())?;
        let start_mono = mono_now();
        let mut cmd = shell_command(command, &self.root);
        cmd.stdin(std::process::Stdio::null()).stdout(std::process::Stdio::from(out)).stderr(std::process::Stdio::from(err_out));
        let child = cmd.spawn().map_err(|e| e.to_string())?;
        let pid = child.id().unwrap_or(0);
        self.session.track_group(pid);
        let (done_tx, done_rx) = watch::channel(false);
        let job = Arc::new(Job {
            name: name.to_string(),
            command: command.to_string(),
            start_mono,
            pid,
            output: output.clone(),
            kill_requested: std::sync::atomic::AtomicBool::new(false),
            kill_reason: Mutex::new(None),
            done: done_rx,
        });
        self.write_status(name, &obj(vec![("job", json!(name)), ("command", json!(command)), ("state", json!("running")), ("pid", json!(pid)), ("startMono", log::num(start_mono)), ("output", json!(output.display().to_string()))]))?;
        self.jobs.lock().map_err(|e| e.to_string())?.insert(name.to_string(), job.clone());
        tokio::spawn(job_task(self.clone(), job, child, Duration::from_millis(timeout_ms), done_tx));
        self.session.append("job", obj(vec![("job", json!(name)), ("state", json!("running")), ("pid", json!(pid))]))?;
        Ok(json!({ "ok": true, "state": "running", "output": format!("started job {name} (pid {pid})") }))
    }

    fn write_status(&self, name: &str, status: &Map<String, Value>) -> Result<(), String> {
        let dir = self.run_dir.join("jobs");
        let file = dir.join(format!("{name}.json"));
        let temp = dir.join(format!("{name}.json.{}.tmp", std::process::id()));
        fs::write(&temp, format!("{}\n", Value::Object(status.clone()))).map_err(|e| e.to_string())?;
        fs::rename(&temp, &file).map_err(|e| e.to_string())
    }

    fn read_status(&self, name: &str) -> Option<Map<String, Value>> {
        let text = fs::read_to_string(self.run_dir.join("jobs").join(format!("{name}.json"))).ok()?;
        serde_json::from_str::<Value>(&text).ok()?.as_object().cloned()
    }

    async fn job_status(&self, args: &Value) -> Result<Value, String> {
        let name = args.get("job").and_then(Value::as_str).unwrap_or("");
        if !valid_job(name) {
            return Ok(json!({ "ok": false, "error": "job must be a job name" }));
        }
        let wait_ms = args.get("waitMs").and_then(Value::as_f64).filter(|n| !n.is_nan()).unwrap_or(0.0).clamp(0.0, MAX_WAIT_MS);
        let job = self.jobs.lock().map_err(|e| e.to_string())?.get(name).cloned();
        if let Some(job) = job {
            if !job.settled() && wait_ms > 0.0 {
                let _ = tokio::time::timeout(Duration::from_millis(wait_ms as u64), wait_settled(job.done.clone())).await;
            }
        }
        let Some(status) = self.read_status(name) else {
            return Ok(json!({ "ok": false, "error": format!("no job named {name}") }));
        };
        let offset = match args.get("offset") {
            None | Some(Value::Null) => None,
            Some(value) => match value.as_f64().filter(|n| n.fract() == 0.0 && *n >= 0.0) {
                Some(n) => Some(n as usize),
                None => return Ok(json!({ "ok": false, "error": "offset must be a whole number of characters, zero or more" })),
            },
        };
        let units: Vec<u16> = read_text(status.get("output").and_then(Value::as_str)).encode_utf16().collect();
        let from = offset.unwrap_or(units.len().saturating_sub(OUTPUT_PAGE));
        let end = (from + OUTPUT_PAGE).min(units.len());
        let mut reply = status.clone();
        reply.remove("output");
        reply.insert("ok".into(), json!(true));
        reply.insert("exitCode".into(), status.get("exitCode").cloned().unwrap_or(Value::Null));
        reply.insert("signal".into(), status.get("signal").cloned().unwrap_or(Value::Null));
        reply.insert("output".into(), json!(utf16_slice(&units, from, end)));
        reply.insert("total".into(), json!(units.len()));
        reply.insert("truncated".into(), json!(from > 0 || end < units.len()));
        reply.insert("offset".into(), json!(from));
        reply.insert("next".into(), json!((end < units.len()).then_some(end)));
        reply.insert("logPath".into(), status.get("output").cloned().unwrap_or(Value::Null));
        Ok(Value::Object(reply))
    }

    async fn job_stop(&self, args: &Value) -> Result<Value, String> {
        let name = args.get("job").and_then(Value::as_str).unwrap_or("");
        if !valid_job(name) {
            return Ok(json!({ "ok": false, "error": "job must be a job name" }));
        }
        let job = self.jobs.lock().map_err(|e| e.to_string())?.get(name).cloned();
        let Some(job) = job else {
            return Ok(json!({ "ok": false, "error": format!("no job named {name}") }));
        };
        if job.settled() {
            let state = self.read_status(name).and_then(|s| s.get("state").cloned()).map(|v| v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string())).unwrap_or_else(|| "undefined".to_string());
            return Ok(json!({ "ok": false, "error": format!("job {name} already ended ({state})") }));
        }
        job.kill_requested.store(true, Ordering::SeqCst);
        if let Ok(mut reason) = job.kill_reason.lock() {
            reason.get_or_insert_with(|| "cancelled".to_string());
        }
        kill_group(job.pid).await;
        wait_settled(job.done.clone()).await;
        let reason = job.kill_reason.lock().ok().and_then(|r| r.clone());
        Ok(json!({ "ok": true, "state": "killed", "reason": reason, "output": format!("stopped job {name}") }))
    }

    pub async fn close(&self) {
        let running: Vec<Arc<Job>> = self.jobs.lock().map(|jobs| jobs.values().filter(|job| !job.settled()).cloned().collect()).unwrap_or_default();
        for job in &running {
            job.kill_requested.store(true, Ordering::SeqCst);
            if let Ok(mut reason) = job.kill_reason.lock() {
                reason.get_or_insert_with(|| "run-ended".to_string());
            }
        }
        for pid in self.session.live_groups() {
            kill_group(pid).await;
        }
        for job in running {
            wait_settled(job.done.clone()).await;
        }
    }

    pub fn kill_all_now(&self) {
        for pid in self.session.live_groups() {
            kill_group_now(pid);
        }
    }
}

fn finish(args: &Value) -> Result<Value, String> {
    match args.get("witness").and_then(Value::as_str).filter(|w| !w.trim().is_empty()) {
        Some(witness) => Ok(json!({ "ok": true, "finished": true, "output": witness })),
        None => Ok(json!({ "ok": false, "error": "finish needs a non-empty witness string" })),
    }
}

fn read_text(path: Option<&str>) -> String {
    path.and_then(|p| fs::read(p).ok()).map(|b| String::from_utf8_lossy(&b).into_owned()).unwrap_or_default()
}

fn read_tail(path: &Path, cap: usize) -> String {
    let units: Vec<u16> = fs::read(path).map(|b| String::from_utf8_lossy(&b).encode_utf16().collect()).unwrap_or_default();
    let from = units.len().saturating_sub(cap);
    utf16_slice(&units, from, units.len())
}

async fn job_task(tools: Arc<Tools>, job: Arc<Job>, mut child: Child, limit: Duration, done: watch::Sender<bool>) {
    let waited = tokio::select! {
        status = child.wait() => status.ok(),
        _ = tokio::time::sleep(limit) => {
            job.kill_requested.store(true, Ordering::SeqCst);
            kill_group(job.pid).await;
            child.wait().await.ok()
        }
    };
    let code = waited.as_ref().and_then(|s| s.code());
    let signal = waited.as_ref().map(signal_name).unwrap_or(Value::Null);
    let killed = code.is_none() || job.kill_requested.load(Ordering::SeqCst);
    let state = if killed { "killed" } else { "exited" };
    let kill_reason = job.kill_reason.lock().ok().and_then(|r| r.clone());
    let reason = if killed { kill_reason.clone() } else { None };
    let mut ended = obj(vec![("job", json!(job.name)), ("command", json!(job.command)), ("state", json!(state)), ("pid", json!(job.pid)), ("startMono", log::num(job.start_mono)), ("output", json!(job.output.display().to_string())), ("exitCode", json!(code)), ("signal", signal.clone())]);
    if let Some(reason) = &reason {
        ended.insert("reason".into(), json!(reason));
    }
    let _ = tools.write_status(&job.name, &ended);
    let mut fields = obj(vec![("job", json!(job.name)), ("state", json!(state)), ("exitCode", json!(code)), ("signal", signal), ("startMono", log::num(job.start_mono))]);
    if let Some(reason) = &reason {
        fields.insert("reason".into(), json!(reason));
    }
    let _ = tools.session.append("job", fields);
    if let Some(reason) = kill_reason {
        let tail = read_tail(&job.output, TAIL_CAP);
        let _ = tools.session.append("job-killed", obj(vec![("job", json!(job.name)), ("reason", json!(reason)), ("tail", json!(tail))]));
    }
    tools.session.untrack_group(job.pid);
    done.send_replace(true);
}

struct Ctx {
    session: Arc<Session>,
    tools: Arc<Tools>,
    run_dir: PathBuf,
    root: PathBuf,
    task: String,
    witness: String,
    max_steps: u64,
    serial: bool,
    latency_ms: u64,
    overlap: bool,
}

struct Flight {
    step: u64,
    actions: Arc<Vec<Action>>,
    done: Arc<Mutex<Vec<bool>>>,
    handle: tokio::task::JoinHandle<Result<Option<String>, String>>,
}

#[derive(Debug)]
pub struct Outcome {
    status: &'static str,
    steps: u64,
    witness: Option<String>,
}

fn simulated(latency_ms: u64) -> Map<String, Value> {
    let mut fields = Map::new();
    if latency_ms > 0 {
        fields.insert("simulatedMs".into(), json!(latency_ms));
    }
    fields
}

fn with_simulated(mut fields: Map<String, Value>, latency_ms: u64) -> Map<String, Value> {
    fields.extend(simulated(latency_ms));
    fields
}

fn ms(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

fn span_of(event: &Value, kinds: &[&str]) -> Option<(f64, f64)> {
    let kind = event.get("kind").and_then(Value::as_str)?;
    if !kinds.contains(&kind) {
        return None;
    }
    let start = event.get("startMono").and_then(Value::as_f64)?;
    let end = event.get("mono").and_then(Value::as_f64)?;
    Some((start, end))
}

fn merged_total(mut spans: Vec<(f64, f64)>) -> f64 {
    spans.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut merged: Vec<(f64, f64)> = Vec::new();
    for (from, to) in spans {
        match merged.last_mut() {
            Some(last) if from <= last.1 => last.1 = last.1.max(to),
            _ => merged.push((from, to)),
        }
    }
    merged.iter().map(|(from, to)| to - from).sum()
}

fn summarize(events: &[Value], end_mono: f64) -> Map<String, Value> {
    let wall = events.first().and_then(|e| e.get("mono")).and_then(Value::as_f64).map(|first| end_mono - first).unwrap_or(0.0);
    let tool: Vec<(f64, f64)> = events.iter().filter_map(|e| span_of(e, &["result", "refusal"])).collect();
    let model: Vec<(f64, f64)> = events.iter().filter_map(|e| span_of(e, &["reply", "retry", "error"])).collect();
    let max_in_flight = events.iter().map(|e| e.get("inFlight").and_then(Value::as_f64).unwrap_or(0.0)).fold(0.0, f64::max);
    let tool_total: f64 = tool.iter().map(|(a, b)| b - a).sum();
    let mut all = tool.clone();
    all.extend(model.clone());
    let context_calls = events.iter().filter(|e| e.get("kind").and_then(Value::as_str) == Some("reply")).count();
    let mut out = Map::new();
    out.insert("maxInFlight".into(), log::num(max_in_flight));
    out.insert("meanInFlight".into(), log::num(if wall > 0.0 { ms(tool_total / wall) } else { 0.0 }));
    out.insert("modelMs".into(), log::num(ms(merged_total(model))));
    out.insert("toolMs".into(), log::num(ms(merged_total(tool))));
    out.insert("idleMs".into(), log::num(ms((wall - merged_total(all)).max(0.0))));
    out.insert("contextCalls".into(), json!(context_calls));
    out
}

fn write_summary(session: &Session) -> Result<(), String> {
    let events = session.events()?;
    if events.iter().any(|e| e.get("kind").and_then(Value::as_str) == Some("summary")) {
        return Ok(());
    }
    let end_mono = mono_now();
    session.append_at("summary", summarize(&events, end_mono), Some(end_mono))
}

fn failed_of(result: &Value) -> bool {
    result.get("ok") == Some(&Value::Bool(false)) || result.get("exitCode").and_then(Value::as_f64).is_some_and(|code| code != 0.0)
}

fn logged_for(tool: &str, result: &Value) -> Value {
    let Some(output) = result.get("output").and_then(Value::as_str) else { return result.clone() };
    if !LOGGED_OUTPUT_TOOLS.contains(&tool) {
        return result.clone();
    }
    let units: Vec<u16> = output.encode_utf16().collect();
    if units.len() <= LOGGED_REPLY_CHARS {
        return result.clone();
    }
    let mut out = result.as_object().cloned().unwrap_or_default();
    out.insert("output".into(), json!(utf16_slice(&units, 0, LOGGED_REPLY_CHARS)));
    out.insert("outputChars".into(), json!(units.len()));
    out.insert("outputDigest".into(), output_digest(output, failed_of(result)));
    Value::Object(out)
}

async fn finish_action(ctx: Arc<Ctx>, step: u64, action: Action, start_mono: f64, done: Option<(Arc<Mutex<Vec<bool>>>, usize)>) -> Result<Value, String> {
    if ctx.latency_ms > 0 {
        tokio::time::sleep(Duration::from_millis(ctx.latency_ms)).await;
    }
    let result = ctx.tools.run(&action.tool, &action.args).await;
    let refused = result.get("refused") == Some(&Value::Bool(true));
    let mut fields = obj(vec![("step", json!(step)), ("tool", json!(action.tool)), ("startMono", log::num(start_mono))]);
    if refused {
        fields.insert("error".into(), result.get("error").cloned().unwrap_or(Value::Null));
        ctx.session.append("refusal", with_simulated(fields, ctx.latency_ms))?;
    } else {
        if let Value::Object(logged) = logged_for(&action.tool, &result) {
            fields.extend(logged);
        }
        ctx.session.append("result", with_simulated(fields, ctx.latency_ms))?;
    }
    ctx.session.leave();
    if let Some((slots, index)) = done {
        if let Ok(mut slots) = slots.lock() {
            slots[index] = true;
        }
    }
    Ok(result)
}

async fn run_actions(ctx: Arc<Ctx>, step: u64, actions: Arc<Vec<Action>>, done: Option<Arc<Mutex<Vec<bool>>>>) -> Result<Option<String>, String> {
    let waves = plan_waves(actions.as_slice(), &ctx.root, ctx.serial);
    for (i, wave) in waves.iter().enumerate() {
        let mut started = Vec::new();
        for &index in wave {
            let start_mono = mono_now();
            ctx.session.enter();
            let fields = with_simulated(obj(vec![("step", json!(step)), ("tool", json!(actions[index].tool)), ("args", actions[index].args.clone())]), ctx.latency_ms);
            ctx.session.append("action", fields)?;
            started.push((index, start_mono));
        }
        let mut handles = Vec::new();
        for (index, start_mono) in started {
            let slot = done.clone().map(|slots| (slots, index));
            let action = actions[index].clone();
            handles.push(tokio::spawn(finish_action(ctx.clone(), step, action, start_mono, slot)));
        }
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.map_err(|e| e.to_string())??);
        }
        if let Some(finish) = results.iter().find(|r| r.get("finished") == Some(&Value::Bool(true))) {
            let witness = finish.get("output").and_then(Value::as_str).unwrap_or("").to_string();
            for &rest in waves[i + 1..].iter().flatten() {
                let skipped = obj(vec![("step", json!(step)), ("tool", json!(actions[rest].tool)), ("reason", json!("follows finish in the same reply"))]);
                ctx.session.append("skipped", skipped)?;
            }
            return Ok(Some(witness));
        }
    }
    Ok(None)
}

fn start_actions(ctx: &Arc<Ctx>, step: u64, actions: Vec<Action>) -> Flight {
    let actions = Arc::new(actions);
    let done = Arc::new(Mutex::new(vec![false; actions.len()]));
    let handle = tokio::spawn(run_actions(ctx.clone(), step, actions.clone(), Some(done.clone())));
    Flight { step, actions, done, handle }
}

fn log_pending(ctx: &Ctx, flight: &Flight, step: u64) -> Result<(), String> {
    let done = flight.done.lock().map(|d| d.clone()).unwrap_or_default();
    let unfinished: Vec<Value> = flight.actions.iter().enumerate().filter(|(i, _)| !done[*i]).map(|(_, a)| json!({ "tool": a.tool, "args": a.args })).collect();
    if unfinished.is_empty() {
        return Ok(());
    }
    let fields = obj(vec![
        ("step", json!(step)),
        ("fromStep", json!(flight.step)),
        ("actions", Value::Array(unfinished)),
        ("note", json!("no result event yet; each result is appended as a result event when it lands")),
    ]);
    ctx.session.append("pending", fields)
}

async fn settle(flight: &mut Option<Flight>) -> Result<(), String> {
    match flight.take() {
        None => Ok(()),
        Some(flight) => flight.handle.await.map_err(|e| e.to_string())?.map(|_| ()),
    }
}

async fn steps(ctx: &Arc<Ctx>, model: &mut Scripted, flight: &mut Option<Flight>) -> Result<Outcome, String> {
    for step in 1..=ctx.max_steps {
        if let Some(pending) = flight.as_ref() {
            log_pending(ctx, pending, step)?;
        }
        let rules = fs::read_to_string(ctx.run_dir.join("rules.md")).map_err(|e| fail_at("open", &ctx.run_dir.join("rules.md"), e))?;
        let prefix_sha = sha256_hex(&render_prefix(&rules, &ctx.task, &ctx.witness));
        let reply = match ask_model(ctx, model, step, &prefix_sha).await? {
            Ok(reply) => reply,
            Err(call) => {
                let mut fields = obj(vec![("step", json!(step)), ("message", json!(call.error.message)), ("attempts", json!(call.attempts)), ("reason", json!(call.reason)), ("prefixSha", json!(prefix_sha)), ("startMono", log::num(call.start_mono))]);
                if let Some(status) = call.error.status {
                    fields.insert("status".into(), json!(status));
                }
                if let Some(error_type) = &call.error.error_type {
                    fields.insert("errorType".into(), json!(error_type));
                }
                ctx.session.append("error", with_simulated(fields, ctx.latency_ms))?;
                return Ok(Outcome { status: "error", steps: step, witness: None });
            }
        };
        let fields = obj(vec![("step", json!(step)), ("text", json!(reply.text)), ("attempts", json!(reply.attempts)), ("prefixSha", json!(prefix_sha)), ("startMono", log::num(reply.start_mono))]);
        ctx.session.append("reply", with_simulated(fields, ctx.latency_ms))?;
        let actions = match parse_actions(&reply.text) {
            Ok(actions) => actions,
            Err(error) => {
                ctx.session.append("parse_error", obj(vec![("step", json!(step)), ("error", json!(error))]))?;
                continue;
            }
        };
        settle(flight).await?;
        let finishes = actions.iter().any(|a| a.tool == "finish");
        if !ctx.overlap || finishes {
            let outcome = run_actions(ctx.clone(), step, Arc::new(actions), None).await?;
            if let Some(witness) = outcome {
                return Ok(Outcome { status: "finished", steps: step, witness: Some(witness) });
            }
        } else {
            *flight = Some(start_actions(ctx, step, actions));
        }
    }
    Ok(Outcome { status: "max_steps", steps: ctx.max_steps, witness: None })
}

struct CallError {
    error: ModelError,
    attempts: u64,
    reason: String,
    start_mono: f64,
}

struct CallReply {
    text: String,
    attempts: u64,
    start_mono: f64,
}

async fn ask_model(ctx: &Arc<Ctx>, model: &mut Scripted, step: u64, prefix_sha: &str) -> Result<Result<CallReply, CallError>, String> {
    let mut retries = 0;
    let mut headerless = 0;
    let mut attempt: u64 = 1;
    loop {
        let start_mono = mono_now();
        match model.reply().await {
            Ok(text) => return Ok(Ok(CallReply { text, attempts: attempt, start_mono })),
            Err(error) => match retry_plan(&error, retries, headerless) {
                Plan::Stop(reason) => return Ok(Err(CallError { error, attempts: attempt, reason, start_mono })),
                Plan::Wait { delay_ms, source, headerless: rate_limited } => {
                    let mut fields = obj(vec![("step", json!(step)), ("attempt", json!(attempt)), ("delayMs", log::num(delay_ms)), ("source", json!(source)), ("prefixSha", json!(prefix_sha)), ("message", json!(error.message)), ("startMono", log::num(start_mono))]);
                    if let Some(status) = error.status {
                        fields.insert("status".into(), json!(status));
                    }
                    if let Some(error_type) = &error.error_type {
                        fields.insert("errorType".into(), json!(error_type));
                    }
                    ctx.session.append("retry", with_simulated(fields, ctx.latency_ms))?;
                    retries += 1;
                    if rate_limited {
                        headerless += 1;
                    }
                    tokio::time::sleep(Duration::from_secs_f64(delay_ms / 1000.0)).await;
                }
            },
        }
        attempt += 1;
    }
}

async fn drive(ctx: Arc<Ctx>, model: &mut Scripted) -> Result<Outcome, String> {
    ctx.session.append("start", obj(vec![("mode", json!("cli")), ("maxSteps", json!(ctx.max_steps)), ("serial", json!(ctx.serial))]))?;
    let mut flight = None;
    let outcome = steps(&ctx, model, &mut flight).await;
    let waited = settle(&mut flight).await;
    ctx.tools.close().await;
    let outcome = outcome?;
    waited?;
    write_summary(&ctx.session)?;
    let witness = outcome.witness.as_ref().map(|w| json!(w)).unwrap_or(Value::Null);
    ctx.session.append("end", obj(vec![("status", json!(outcome.status)), ("steps", json!(outcome.steps)), ("witness", witness)]))?;
    Ok(outcome)
}

fn cwd() -> Result<PathBuf, String> {
    std::env::current_dir().map_err(|e| e.to_string())
}

fn absolute(value: &str) -> Result<PathBuf, String> {
    Ok(lexical(&cwd()?, value))
}

fn new_run_id() -> String {
    let stamp: String = log::iso_now().chars().take(19).map(|c| if c == 'T' || c == ':' { '-' } else { c }).collect();
    format!("{stamp}-{}", log::random_hex(3))
}

fn pretty_object(entries: &[(&str, String)]) -> String {
    let body: Vec<String> = entries.iter().map(|(key, value)| format!("  {}: {value}", json_str(key))).collect();
    format!("{{\n{}\n}}\n", body.join(",\n"))
}

fn write_run_files(run_dir: &Path, entries: &[(&str, String)]) -> Result<(), String> {
    fs::create_dir_all(run_dir).map_err(|e| e.to_string())?;
    fs::write(run_dir.join("rules.md"), DEFAULT_RULES).map_err(|e| e.to_string())?;
    fs::write(run_dir.join("task.json"), pretty_object(entries)).map_err(|e| e.to_string())
}

fn load_task(value: &str) -> Result<String, String> {
    let path = absolute(value)?;
    if path.is_file() {
        return fs::read_to_string(&path).map_err(|e| fail_at("open", &path, e));
    }
    Ok(value.to_string())
}

fn load_script(file: &str) -> Result<Vec<Entry>, String> {
    let path = absolute(file)?;
    let text = fs::read_to_string(&path).map_err(|e| fail_at("open", &path, e))?;
    let value = parse_ojson(&text).ok_or_else(|| "--script is not valid JSON".to_string())?;
    replies_from_json(value)
}

fn parse_latency(value: &str) -> Result<u64, String> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err("--latency-ms takes a whole number of milliseconds, 0 or more".to_string());
    }
    value.parse::<u64>().map_err(|e| e.to_string())
}

fn parse_faults(value: &str) -> Result<Vec<u16>, String> {
    let statuses: Vec<f64> = value.split(',').map(|item| item.trim().parse::<f64>().unwrap_or(f64::NAN)).collect();
    if statuses.iter().any(|s| s.fract() != 0.0 || !(400.0..=599.0).contains(s)) {
        return Err("--faults takes comma-separated HTTP statuses from 400 to 599, for example 429,429".to_string());
    }
    Ok(statuses.into_iter().map(|s| s as u16).collect())
}

fn faults_json(faults: Option<&Vec<u16>>) -> String {
    match faults {
        None => "null".to_string(),
        Some(list) => {
            let items: Vec<String> = list.iter().map(|f| format!("    {f}")).collect();
            format!("[\n{}\n  ]", items.join(",\n"))
        }
    }
}

fn max_steps_of(value: Option<&str>) -> Result<u64, String> {
    match value {
        None => Ok(DEFAULT_MAX_STEPS),
        Some(v) => v.parse::<u64>().ok().filter(|n| *n >= 1).ok_or_else(|| "--max-steps must be a positive integer".to_string()),
    }
}

fn read_replies(file: &Path) -> Result<Vec<Entry>, String> {
    if !file.exists() {
        return Ok(Vec::new());
    }
    let events = fs::read_to_string(file).map_err(|e| e.to_string())?;
    let mut replies = Vec::new();
    for line in events.lines().filter(|l| !l.trim().is_empty()) {
        let event: Value = serde_json::from_str(line).map_err(|e| e.to_string())?;
        if event.get("kind").and_then(Value::as_str) == Some("reply") {
            replies.push(Entry::Text(event.get("text").and_then(Value::as_str).unwrap_or("").to_string()));
        }
    }
    Ok(replies)
}

#[cfg(unix)]
static INTERRUPT: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);

#[cfg(unix)]
extern "C" fn on_interrupt(signal: libc::c_int) {
    INTERRUPT.store(signal, Ordering::SeqCst);
}

#[cfg(unix)]
fn watch_interrupts(tools: Arc<Tools>, session: Arc<Session>) {
    unsafe {
        libc::signal(libc::SIGINT, on_interrupt as libc::sighandler_t);
        libc::signal(libc::SIGTERM, on_interrupt as libc::sighandler_t);
    }
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let signal = INTERRUPT.load(Ordering::SeqCst);
            if signal != 0 {
                tools.kill_all_now();
                let _ = write_summary(&session);
                let (name, code) = if signal == libc::SIGINT { ("SIGINT", 130) } else { ("SIGTERM", 143) };
                let _ = session.append("end", obj(vec![("status", json!("interrupted")), ("signal", json!(name))]));
                std::process::exit(code);
            }
        }
    });
}

#[cfg(not(unix))]
fn watch_interrupts(_tools: Arc<Tools>, _session: Arc<Session>) {}

fn runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_current_thread().enable_all().build().map_err(|e| e.to_string())
}

pub fn run(flags: &crate::cli::Flags) -> Result<i32, String> {
    mono_now();
    let task_flag = flags.value("task").ok_or_else(|| "--task is required".to_string())?;
    let workspace_flag = flags.value("workspace").ok_or_else(|| "--workspace is required".to_string())?;
    let workspace = absolute(workspace_flag)?;
    if !workspace.is_dir() {
        return Err(format!("workspace is not a directory: {}", workspace.display()));
    }
    let max_steps = max_steps_of(flags.value("max-steps"))?;
    let model_name = flags.value("model").unwrap_or("scripted");
    let script = match flags.value("script") {
        Some(file) => Some(load_script(file)?),
        None => None,
    };
    if script.is_some() && model_name != "scripted" {
        return Err("--script works only with --model scripted".to_string());
    }
    let faults = match flags.value("faults") {
        Some(value) => Some(parse_faults(value)?),
        None => None,
    };
    if faults.is_some() && model_name != "scripted" {
        return Err("--faults works only with --model scripted".to_string());
    }
    if flags.value("latency-ms").is_some() && model_name != "scripted" {
        return Err("--latency-ms works only with --model scripted".to_string());
    }
    let latency_ms = match flags.value("latency-ms") {
        Some(value) => parse_latency(value)?,
        None => 0,
    };
    if model_name != "scripted" {
        return Err(format!("unknown model: {model_name}; gm-new calls no API, so the only model is scripted"));
    }
    let serial = flags.switch("serial");
    let background = !flags.switch("no-background");
    let overlap = flags.switch("overlap");
    let task = load_task(task_flag)?.trim().to_string();
    if task.is_empty() {
        return Err("--task is empty".to_string());
    }
    let script_path = match flags.value("script") {
        Some(file) => Some(absolute(file)?),
        None => None,
    };

    let id = new_run_id();
    let run_dir = workspace.join("runs").join(&id);
    let entries = [
        ("id", json_str(&id)),
        ("task", json_str(&task)),
        ("witness", json_str(DEFAULT_WITNESS)),
        ("workspace", json_str(&workspace.display().to_string())),
        ("model", json_str(model_name)),
        ("maxSteps", max_steps.to_string()),
        ("serial", serial.to_string()),
        ("background", background.to_string()),
        ("overlap", overlap.to_string()),
        ("latencyMs", latency_ms.to_string()),
        ("script", script_path.as_ref().map(|p| json_str(&p.display().to_string())).unwrap_or_else(|| "null".to_string())),
        ("faults", faults_json(faults.as_ref())),
        ("createdAt", json_str(&log::iso_now())),
    ];
    write_run_files(&run_dir, &entries)?;

    let session = Arc::new(Session::open(&run_dir, None)?);
    let tools = Arc::new(Tools::new(session.clone(), workspace.clone(), run_dir.clone(), background));
    let ctx = Arc::new(Ctx {
        session: session.clone(),
        tools: tools.clone(),
        run_dir: run_dir.clone(),
        root: workspace.clone(),
        task,
        witness: DEFAULT_WITNESS.to_string(),
        max_steps,
        serial,
        latency_ms,
        overlap,
    });
    let replies = script.unwrap_or_else(default_replies);
    let fault_list = faults.unwrap_or_default().into_iter().map(|status| Fault { status, message: None, retry_after_s: None }).collect();
    let mut model = Scripted::new(replies, fault_list, latency_ms);
    let rt = runtime()?;
    let outcome = rt.block_on(async {
        watch_interrupts(tools.clone(), session.clone());
        drive(ctx, &mut model).await
    })?;

    println!("run {id}: {} after {} steps", outcome.status, outcome.steps);
    if let Some(witness) = &outcome.witness {
        println!("witness: {witness}");
    }
    if latency_ms > 0 {
        println!("simulated latency {latency_ms} ms: each scripted reply and action waited this long; those events carry simulatedMs");
    }
    println!("log: {}", run_dir.join(LOG_FILE).display());
    Ok(if outcome.status == "finished" { 0 } else { 1 })
}

pub fn replay(flags: &crate::cli::Flags) -> Result<i32, String> {
    mono_now();
    let source = crate::cli::run_dir_of(flags)?;
    let source_id = source.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    let task_file = source.join("task.json");
    let meta_text = fs::read_to_string(&task_file).map_err(|e| fail_at("open", &task_file, e))?;
    let meta: Value = serde_json::from_str(&meta_text).map_err(|e| e.to_string())?;
    let replies = read_replies(&source.join(LOG_FILE))?;
    if replies.is_empty() {
        return Err(format!("run {} has no reply events to replay", flags.value("run").unwrap_or("undefined")));
    }
    let workspace = match flags.value("workspace") {
        Some(value) => absolute(value)?,
        None => absolute(meta.get("workspace").and_then(Value::as_str).unwrap_or(""))?,
    };
    if !workspace.is_dir() {
        return Err(format!("workspace is not a directory: {}", workspace.display()));
    }
    let max_steps = flags
        .value("max-steps")
        .and_then(|v| v.parse::<u64>().ok())
        .or_else(|| meta.get("maxSteps").and_then(Value::as_u64))
        .unwrap_or(DEFAULT_MAX_STEPS);
    let task = meta.get("task").and_then(Value::as_str).unwrap_or("").to_string();
    let witness = meta.get("witness").and_then(Value::as_str).unwrap_or(DEFAULT_WITNESS).to_string();

    let id = new_run_id();
    let run_dir = workspace.join("runs").join(&id);
    let entries = [
        ("id", json_str(&id)),
        ("task", json_str(&task)),
        ("witness", json_str(&witness)),
        ("workspace", json_str(&workspace.display().to_string())),
        ("model", json_str("scripted")),
        ("maxSteps", max_steps.to_string()),
        ("replayOf", json_str(&source_id)),
        ("createdAt", json_str(&log::iso_now())),
    ];
    write_run_files(&run_dir, &entries)?;

    let session = Arc::new(Session::open(&run_dir, Some(source_id.clone()))?);
    let tools = Arc::new(Tools::new(session.clone(), workspace.clone(), run_dir.clone(), true));
    let ctx = Arc::new(Ctx {
        session: session.clone(),
        tools,
        run_dir: run_dir.clone(),
        root: workspace,
        task,
        witness,
        max_steps,
        serial: false,
        latency_ms: 0,
        overlap: false,
    });
    let mut model = Scripted::new(replies, Vec::new(), 0);
    let rt = runtime()?;
    let outcome = rt.block_on(drive(ctx, &mut model))?;

    println!("run {id}: {} after {} steps, replay of {source_id}", outcome.status, outcome.steps);
    if let Some(witness) = &outcome.witness {
        println!("witness: {witness}");
    }
    println!("log: {}", run_dir.join(LOG_FILE).display());
    Ok(if outcome.status == "finished" { 0 } else { 1 })
}
