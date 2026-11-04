use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Map, Value};

use crate::types::{Ctx, Fut, VerbDef, VerbOutcome};

pub const LOG_FILE: &str = "log.jsonl";
pub const INDEX_FILE: &str = "log.index";
const RECORD_BYTES: u64 = 16;
const CHUNK: usize = 1 << 16;

pub struct LogFile {
    path: PathBuf,
    file: File,
    index: File,
    offsets: Vec<u64>,
    size: u64,
}

#[derive(Default)]
pub struct Scan {
    pub count: u64,
    pub last_mono: f64,
    pub steps: u64,
    pub shown: u64,
    pub notice_seq: u64,
    pub ended: Option<String>,
    pub task: Option<String>,
    pub open_actions: Vec<Value>,
}

impl LogFile {
    pub fn open(path: &Path) -> io::Result<(LogFile, Scan)> {
        let dir = path.parent().unwrap_or(Path::new("."));
        let (offsets, size, scan) = scan_log(path)?;
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let index_path = dir.join(INDEX_FILE);
        if !index_matches(&index_path, &offsets, size) {
            let mut records = Vec::with_capacity(offsets.len() * RECORD_BYTES as usize);
            for (i, begin) in offsets.iter().enumerate() {
                let end = offsets.get(i + 1).copied().unwrap_or(size);
                push_record(&mut records, *begin, end - begin);
            }
            fs::write(&index_path, &records)?;
        }
        let index = OpenOptions::new().create(true).append(true).open(&index_path)?;
        Ok((LogFile { path: path.to_path_buf(), file, index, offsets, size }, scan))
    }

    pub fn count(&self) -> u64 {
        self.offsets.len() as u64
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&mut self, line: &[u8]) -> io::Result<()> {
        self.file.write_all(line)?;
        let mut record = Vec::with_capacity(RECORD_BYTES as usize);
        push_record(&mut record, self.size, line.len() as u64);
        self.index.write_all(&record)?;
        self.offsets.push(self.size);
        self.size += line.len() as u64;
        Ok(())
    }

    pub fn bounds(&self, from_seq: u64, to_seq: u64) -> Option<(u64, u64)> {
        let count = self.count();
        if from_seq < 1 || from_seq > to_seq || to_seq > count {
            return None;
        }
        let begin = self.offsets[(from_seq - 1) as usize];
        let end = if to_seq == count { self.size } else { self.offsets[to_seq as usize] };
        Some((begin, end))
    }
}

fn push_record(out: &mut Vec<u8>, begin: u64, len: u64) {
    out.extend_from_slice(&(begin as f64).to_le_bytes());
    out.extend_from_slice(&(len as u32).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
}

fn index_matches(index_path: &Path, offsets: &[u64], size: u64) -> bool {
    let Ok(meta) = fs::metadata(index_path) else { return false };
    if meta.len() != offsets.len() as u64 * RECORD_BYTES {
        return false;
    }
    let Some(last) = offsets.last() else { return size == 0 };
    let Ok(mut f) = File::open(index_path) else { return false };
    let mut record = [0u8; 16];
    if f.seek(SeekFrom::Start(meta.len() - RECORD_BYTES)).is_err() || f.read_exact(&mut record).is_err() {
        return false;
    }
    let begin = f64::from_le_bytes(record[0..8].try_into().unwrap_or([0; 8])) as u64;
    let len = u32::from_le_bytes(record[8..12].try_into().unwrap_or([0; 4])) as u64;
    begin == *last && begin + len == size
}

fn scan_log(path: &Path) -> io::Result<(Vec<u64>, u64, Scan)> {
    let mut scan = Scan::default();
    let mut offsets = Vec::new();
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((offsets, 0, scan)),
        Err(e) => return Err(e),
    };
    let mut reader = BufReader::with_capacity(CHUNK, file);
    let mut line = Vec::new();
    let mut at = 0u64;
    let mut open: BTreeMap<u64, Value> = BTreeMap::new();
    let mut marked = false;
    loop {
        line.clear();
        let n = reader.read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        if line.last() != Some(&b'\n') {
            break;
        }
        offsets.push(at);
        at += n as u64;
        let Ok(event) = serde_json::from_slice::<Value>(&line) else { continue };
        note_scan(&mut scan, &event, &mut open, &mut marked);
    }
    let full = fs::metadata(path)?.len();
    if full > at {
        OpenOptions::new().write(true).open(path)?.set_len(at)?;
    }
    scan.count = offsets.len() as u64;
    scan.open_actions = open.into_values().collect();
    Ok((offsets, at, scan))
}

fn note_scan(scan: &mut Scan, event: &Value, open: &mut BTreeMap<u64, Value>, marked: &mut bool) {
    let kind = event.get("kind").and_then(Value::as_str).unwrap_or("");
    let tool = event.get("tool").and_then(Value::as_str).unwrap_or("");
    let seq = event.get("seq").and_then(Value::as_u64).unwrap_or(0);
    if let Some(mono) = event.get("mono").and_then(Value::as_f64) {
        scan.last_mono = mono;
    }
    if event.get("actionSeq").is_some() || event.get("rpcId").is_some() {
        *marked = true;
    }
    match kind {
        "action" => {
            if tool == "next_context" {
                scan.notice_seq = seq;
            }
            if *marked {
                open.insert(seq, json!({ "seq": seq, "tool": tool, "args": event.get("args").cloned().unwrap_or(Value::Null) }));
            }
        }
        "result" | "refusal" | "code-call" => {
            if let Some(a) = event.get("actionSeq").and_then(Value::as_u64) {
                open.remove(&a);
            }
            if kind == "result" && tool == "next_context" && event.get("ok") == Some(&Value::Bool(true)) {
                scan.steps += 1;
                if let Some(total) = event.get("total").and_then(Value::as_u64) {
                    scan.shown = total;
                }
            }
        }
        "end" => scan.ended = Some(event.get("status").and_then(Value::as_str).unwrap_or("ended").to_string()),
        "task" => scan.task = event.get("text").and_then(Value::as_str).map(str::to_string),
        _ => {}
    }
}

pub fn read_range(path: &Path, begin: u64, end: u64) -> io::Result<Vec<u8>> {
    let mut f = File::open(path)?;
    f.seek(SeekFrom::Start(begin))?;
    let mut buf = vec![0u8; (end - begin) as usize];
    f.read_exact(&mut buf)?;
    Ok(buf)
}

pub fn split_lines(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    bytes.split(|b| *b == b'\n').filter(|l| l.iter().any(|b| !b.is_ascii_whitespace()))
}

pub fn last_line_of(path: &Path) -> Option<Value> {
    let mut f = File::open(path).ok()?;
    let len = f.metadata().ok()?.len();
    let from = len.saturating_sub(CHUNK as u64);
    f.seek(SeekFrom::Start(from)).ok()?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).ok()?;
    let last = split_lines(&buf).last()?;
    serde_json::from_slice(last).ok()
}

pub fn round3(ms: f64) -> f64 {
    (ms * 1000.0).round() / 1000.0
}

pub fn num(v: f64) -> Value {
    if v.fract() == 0.0 && v.abs() < 9.0e15 {
        Value::from(v as i64)
    } else {
        json!(v)
    }
}

pub fn iso_now() -> String {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    iso_of(d.as_secs() as i64, d.subsec_millis())
}

pub fn iso_of(secs: i64, millis: u32) -> String {
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + if month <= 2 { 1 } else { 0 };
    format!("{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{millis:03}Z", rem / 3600, rem % 3600 / 60, rem % 60)
}

static NONCE: AtomicU64 = AtomicU64::new(0);

pub fn random_hex(bytes: usize) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    h.update(now.as_nanos().to_le_bytes());
    h.update(std::process::id().to_le_bytes());
    h.update(NONCE.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    let addr = &h as *const _ as usize;
    h.update(addr.to_le_bytes());
    h.finalize().iter().take(bytes).map(|b| format!("{b:02x}")).collect()
}

pub const PENDING_FILE: &str = "pending.jsonl";
pub const PENDING_KINDS: &[&str] = &["job", "job-killed", "job-stuck", "job-unstuck", "rules-notice", "recovery", "host-idle"];
const LOCK_FILE: &str = "pending.lock";
const LOCK_STALE: Duration = Duration::from_millis(2000);
const LOCK_WAIT: Duration = Duration::from_millis(5000);
const POLL: Duration = Duration::from_millis(200);

pub fn is_pending_kind(kind: &str) -> bool {
    PENDING_KINDS.contains(&kind)
}

fn with_queue_lock<T>(dir: &Path, f: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
    let lock = dir.join(LOCK_FILE);
    let deadline = Instant::now() + LOCK_WAIT;
    loop {
        match OpenOptions::new().write(true).create_new(true).open(&lock) {
            Ok(_) => break,
            Err(e) if matches!(e.kind(), io::ErrorKind::AlreadyExists | io::ErrorKind::PermissionDenied) => {
                if let Ok(age) = fs::metadata(&lock).and_then(|m| m.modified()).map(|t| t.elapsed().unwrap_or_default()) {
                    if age > LOCK_STALE {
                        let _ = fs::remove_file(&lock);
                    }
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::other(format!("the pending queue stayed locked: {}", lock.display())));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(e) => return Err(e),
        }
    }
    let out = f();
    let _ = fs::remove_file(&lock);
    out
}

pub fn append_pending(dir: &Path, record_line: &[u8]) -> io::Result<()> {
    with_queue_lock(dir, || {
        let mut f = OpenOptions::new().create(true).append(true).open(dir.join(PENDING_FILE))?;
        f.write_all(record_line)
    })
}

fn reading_pid(name: &str) -> Option<Option<u32>> {
    let inner = name.strip_prefix("pending.")?.strip_suffix(".reading")?;
    let mut parts = inner.split('-');
    let ms = parts.next()?;
    let hex = parts.next()?;
    if ms.is_empty() || !ms.bytes().all(|b| b.is_ascii_digit()) || hex.is_empty() || !hex.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()) {
        return None;
    }
    match (parts.next(), parts.next()) {
        (None, _) => Some(None),
        (Some(pid), None) if !pid.is_empty() && pid.bytes().all(|b| b.is_ascii_digit()) => Some(pid.parse().ok()),
        _ => None,
    }
}

#[cfg(unix)]
fn reader_gone(pid: Option<u32>) -> bool {
    let Some(pid) = pid else { return true };
    if unsafe { libc::kill(pid as libc::pid_t, 0) } == 0 {
        return false;
    }
    io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}

#[cfg(not(unix))]
fn reader_gone(pid: Option<u32>) -> bool {
    let Some(pid) = pid else { return true };
    match std::process::Command::new("tasklist").args(["/FI", &format!("PID eq {pid}"), "/NH"]).output() {
        Ok(out) => !String::from_utf8_lossy(&out.stdout).split_whitespace().any(|w| w == pid.to_string()),
        Err(_) => false,
    }
}

fn claim(dir: &Path, name: &str) -> io::Result<Option<PathBuf>> {
    let ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
    let copy = dir.join(format!("pending.{ms}-{}-{}.reading", random_hex(4), std::process::id()));
    match fs::rename(dir.join(name), &copy) {
        Ok(()) => Ok(Some(copy)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

pub struct Taken {
    pub records: Vec<String>,
    pub files: Vec<PathBuf>,
}

pub fn take_pending(dir: &Path) -> io::Result<Taken> {
    let mut files = Vec::new();
    let mut names: Vec<(String, Option<u32>)> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            let pid = reading_pid(&name)?;
            Some((name, pid))
        })
        .collect();
    names.sort();
    for (name, pid) in names {
        if !reader_gone(pid) {
            continue;
        }
        if let Some(copy) = claim(dir, &name)? {
            files.push(copy);
        }
    }
    if let Some(copy) = with_queue_lock(dir, || claim(dir, PENDING_FILE))? {
        files.push(copy);
    }
    let mut records = Vec::new();
    for file in &files {
        let text = fs::read_to_string(file)?;
        records.extend(text.lines().filter(|l| !l.trim().is_empty()).map(str::to_string));
    }
    Ok(Taken { records, files })
}

pub fn has_pending(dir: &Path) -> bool {
    if dir.join(PENDING_FILE).exists() {
        return true;
    }
    let Ok(entries) = fs::read_dir(dir) else { return false };
    entries.filter_map(|e| e.ok()).any(|e| {
        e.file_name().to_str().and_then(reading_pid).is_some_and(reader_gone)
    })
}

pub fn clear_pending(files: &[PathBuf]) {
    for f in files {
        let _ = fs::remove_file(f);
    }
}

fn records_text(records: &[String]) -> String {
    let mut out = String::with_capacity(records.iter().map(|r| r.len() + 1).sum());
    for r in records {
        out.push_str(r);
        out.push('\n');
    }
    out
}

pub fn read_clear(dir: &Path) -> String {
    match take_pending(dir) {
        Ok(taken) => {
            clear_pending(&taken.files);
            records_text(&taken.records)
        }
        Err(e) => format!("events: {e}\n"),
    }
}

pub fn wait_pending(dir: &Path, timeout_ms: u64) -> String {
    let started = Instant::now();
    let timeout = Duration::from_millis(timeout_ms);
    loop {
        let taken = match take_pending(dir) {
            Ok(t) => t,
            Err(e) => return format!("events: {e}\n"),
        };
        if !taken.records.is_empty() {
            let text = records_text(&taken.records);
            clear_pending(&taken.files);
            return text;
        }
        clear_pending(&taken.files);
        if started.elapsed() >= timeout {
            return format!("events: timed out after {timeout_ms} ms with no event pending\n");
        }
        std::thread::sleep(POLL.min(timeout.saturating_sub(started.elapsed())).max(Duration::from_millis(1)));
    }
}

const QUERY_CAP: usize = 20000;
const QUERY_PAGE_EVENTS: u64 = 50;
const QUERY_REPLY_CHARS: usize = 20000;
const QUERY_WINDOW_EVENTS: usize = 1000;
const QUERY_WINDOW_BYTES: u64 = 4 * 1024 * 1024;
const QUERY_ARGS: &[&str] = &["js", "kind", "fromMono", "toMono", "after", "last", "limit"];
const REPLY_OVERHEAD: usize = r#"{"ok":true,"output":"[]"}"#.len();

pub struct Line {
    pub seq: u64,
    pub kind: String,
    pub text: String,
}

struct Filter {
    kind: Option<String>,
    from_mono: Option<f64>,
    to_mono: Option<f64>,
}

impl Filter {
    fn line(&self, text: &str) -> Option<(Line, bool)> {
        let v: Value = serde_json::from_str(text).ok()?;
        let kind = v.get("kind").and_then(Value::as_str).unwrap_or("");
        let mono = v.get("mono").and_then(Value::as_f64).unwrap_or(f64::NAN);
        let ok = self.kind.as_deref().is_none_or(|k| k == kind)
            && self.from_mono.is_none_or(|m| mono >= m)
            && self.to_mono.is_none_or(|m| mono <= m);
        let seq = v.get("seq").and_then(Value::as_u64).unwrap_or(0);
        Some((Line { seq, kind: kind.to_string(), text: text.to_string() }, ok))
    }
}

pub trait LogSource: Send + Sync {
    fn log_path(&self) -> PathBuf;
    fn log_count(&self) -> u64;
    fn log_bounds(&self, from_seq: u64, to_seq: u64) -> Option<(u64, u64)>;
}

fn window(src: &dyn LogSource, after: u64, limit: usize, filter: &Filter, budget: u64) -> io::Result<(Vec<Line>, u64)> {
    let count = src.log_count();
    if after >= count {
        return Ok((Vec::new(), after));
    }
    let Some((begin, end)) = src.log_bounds(after + 1, count) else { return Ok((Vec::new(), after)) };
    let mut f = File::open(src.log_path())?;
    f.seek(SeekFrom::Start(begin))?;
    let mut reader = BufReader::with_capacity(CHUNK, f.take(end - begin));
    let mut out = Vec::new();
    let mut last = after;
    let mut read = 0u64;
    let mut buf = Vec::new();
    while out.len() < limit && (read < budget || last == after) {
        buf.clear();
        let n = reader.read_until(b'\n', &mut buf)?;
        if n == 0 {
            break;
        }
        read += n as u64;
        let text = String::from_utf8_lossy(&buf);
        let text = text.trim_end_matches(['\n', '\r']);
        if text.trim().is_empty() {
            continue;
        }
        if let Some((line, ok)) = filter.line(text) {
            last = line.seq;
            if ok {
                out.push(line);
            }
        }
    }
    Ok((out, last))
}

fn tail(src: &dyn LogSource, after: u64, limit: usize, filter: &Filter) -> io::Result<Vec<Line>> {
    let count = src.log_count();
    let mut out = Vec::new();
    let mut f = File::open(src.log_path())?;
    let mut seq = count;
    let mut buf = Vec::new();
    while seq > after && out.len() < limit {
        if let Some((begin, end)) = src.log_bounds(seq, seq) {
            f.seek(SeekFrom::Start(begin))?;
            buf.resize((end - begin) as usize, 0);
            f.read_exact(&mut buf)?;
            let text = String::from_utf8_lossy(&buf);
            if let Some((line, ok)) = filter.line(text.trim_end()) {
                if ok {
                    out.push(line);
                }
            }
        }
        seq -= 1;
    }
    out.reverse();
    Ok(out)
}

fn escaped_len(text: &str) -> usize {
    serde_json::to_string(text).map(|s| s.len() - 2).unwrap_or(text.len())
}

fn reply_chars(lines: &[&str]) -> usize {
    REPLY_OVERHEAD + lines.iter().map(|l| escaped_len(l)).sum::<usize>() + lines.len().saturating_sub(1)
}

fn clipped(line: &Line) -> String {
    let mut size = line.text.len().min(QUERY_REPLY_CHARS);
    loop {
        let mut cut = size;
        while !line.text.is_char_boundary(cut) {
            cut -= 1;
        }
        let shown = json!({ "seq": line.seq, "kind": line.kind, "truncated": &line.text[..cut] }).to_string();
        if reply_chars(&[&shown]) <= QUERY_REPLY_CHARS || size == 0 {
            return shown;
        }
        size = size * 4 / 5;
    }
}

fn page_of(lines: &[Line], limit: usize) -> Vec<String> {
    let mut page: Vec<String> = Vec::new();
    let mut chars = REPLY_OVERHEAD;
    for line in lines {
        if page.len() == limit {
            break;
        }
        let add = escaped_len(&line.text) + usize::from(!page.is_empty());
        if chars + add <= QUERY_REPLY_CHARS {
            chars += add;
            page.push(line.text.clone());
            continue;
        }
        if page.is_empty() {
            page.push(clipped(line));
        }
        break;
    }
    page
}

fn newest_of(lines: &[Line]) -> (Vec<String>, usize) {
    let costs: Vec<usize> = lines.iter().map(|l| escaped_len(&l.text)).collect();
    let mut from = 0;
    let total = |from: usize| REPLY_OVERHEAD + costs[from..].iter().sum::<usize>() + (lines.len() - from).saturating_sub(1);
    while from + 1 < lines.len() && total(from) > QUERY_REPLY_CHARS {
        from += 1;
    }
    if lines.is_empty() || total(from) <= QUERY_REPLY_CHARS {
        return (lines[from..].iter().map(|l| l.text.clone()).collect(), from);
    }
    (vec![clipped(&lines[lines.len() - 1])], lines.len() - 1)
}

fn array_text<S: AsRef<str>>(items: &[S]) -> String {
    let mut out = String::with_capacity(items.iter().map(|s| s.as_ref().len() + 1).sum::<usize>() + 2);
    out.push('[');
    for (i, s) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(s.as_ref());
    }
    out.push(']');
    out
}

fn capped(text: String) -> Value {
    if text.len() <= QUERY_CAP {
        return json!({ "ok": true, "output": text });
    }
    let mut cut = QUERY_CAP;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    json!({ "ok": true, "output": format!("{} ...[cut at {QUERY_CAP} chars, narrow the filter]", &text[..cut]) })
}

pub fn run_query(events_json: String, js: Option<&str>) -> Value {
    let Some(js) = js else { return capped(events_json) };
    match eval_js(&events_json, js) {
        Ok(text) => capped(text),
        Err(e) => json!({ "ok": false, "error": e }),
    }
}

const QUERY_SCRIPT: &str = "(function(){ var f; try { f = new Function('events', 'return (' + __src + ');'); } catch (e) { f = new Function('events', __src); } var s = JSON.stringify(f(JSON.parse(__events))); return s === undefined ? 'undefined' : s; })()";

fn eval_js(events_json: &str, js: &str) -> Result<String, String> {
    let rt = rquickjs::Runtime::new().map_err(|e| e.to_string())?;
    rt.set_memory_limit(256 << 20);
    rt.set_max_stack_size(1 << 20);
    let ctx = rquickjs::Context::full(&rt).map_err(|e| e.to_string())?;
    ctx.with(|ctx| {
        let globals = ctx.globals();
        globals.set("__src", js).map_err(|e| e.to_string())?;
        globals.set("__events", events_json).map_err(|e| e.to_string())?;
        match ctx.eval::<String, _>(QUERY_SCRIPT) {
            Ok(s) => Ok(s),
            Err(rquickjs::Error::Exception) => {
                let caught = ctx.catch();
                if let Some(msg) = caught.as_exception().and_then(|x| x.message()) {
                    return Err(msg);
                }
                Err(caught.as_string().and_then(|s| s.to_string().ok()).unwrap_or_else(|| "query failed".to_string()))
            }
            Err(e) => Err(e.to_string()),
        }
    })
}

fn refusal(reason: String) -> Value {
    json!({ "ok": false, "refused": true, "error": format!("refused: {reason}") })
}

fn whole(v: &Value) -> Option<i64> {
    let f = v.as_f64()?;
    (f.is_finite() && f.fract() == 0.0).then_some(f as i64)
}

pub fn query_log(src: &dyn LogSource, args: &Value) -> Value {
    let empty = Map::new();
    let args = args.as_object().unwrap_or(&empty);
    let unknown: Vec<&str> = args.keys().map(String::as_str).filter(|k| !QUERY_ARGS.contains(k)).collect();
    if !unknown.is_empty() {
        return refusal(format!("unknown argument {}; query_log takes {}", unknown.join(", "), QUERY_ARGS.join(", ")));
    }
    let js = match args.get("js") {
        None => None,
        Some(Value::String(s)) => Some(s.as_str()),
        Some(_) => return json!({ "ok": false, "error": "js must be a string" }),
    };
    let kind = match args.get("kind") {
        None => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return json!({ "ok": false, "error": "kind must be a string" }),
    };
    for name in ["after", "fromMono", "toMono"] {
        if let Some(v) = args.get(name) {
            if !v.as_f64().is_some_and(f64::is_finite) {
                return json!({ "ok": false, "error": format!("{name} must be a number") });
            }
        }
    }
    let last = match args.get("last") {
        None => None,
        Some(v) => match whole(v) {
            Some(n) if (1..=QUERY_PAGE_EVENTS as i64).contains(&n) => Some(n as usize),
            _ => return json!({ "ok": false, "error": format!("last must be a whole number from 1 to {QUERY_PAGE_EVENTS}") }),
        },
    };
    let mut limit = QUERY_PAGE_EVENTS as usize;
    if let Some(v) = args.get("limit") {
        if last.is_some() {
            return refusal("limit cannot be used with last, which already sets how many events a reply returns".to_string());
        }
        if js.is_some() {
            return refusal("limit cannot be used with js, whose expression sees the whole window of matching events".to_string());
        }
        match whole(v) {
            Some(n) if (1..=QUERY_PAGE_EVENTS as i64).contains(&n) => limit = n as usize,
            _ => return json!({ "ok": false, "error": format!("limit must be a whole number from 1 to {QUERY_PAGE_EVENTS}") }),
        }
    }
    let after = args.get("after").and_then(Value::as_f64).map(|f| f.floor().max(0.0) as u64).unwrap_or(0);
    let filter = Filter {
        kind,
        from_mono: args.get("fromMono").and_then(Value::as_f64),
        to_mono: args.get("toMono").and_then(Value::as_f64),
    };
    let fail = |e: io::Error| json!({ "ok": false, "error": e.to_string() });
    if let Some(last) = last {
        let newest = match tail(src, after, last, &filter) {
            Ok(v) => v,
            Err(e) => return fail(e),
        };
        if js.is_some() {
            return run_query(array_text(&newest.iter().map(|l| l.text.as_str()).collect::<Vec<_>>()), js);
        }
        let (kept, omitted) = newest_of(&newest);
        let mut out = run_query(array_text(&kept), None);
        if omitted > 0 {
            out["omitted"] = json!(omitted);
        }
        return out;
    }
    let (events, scanned_last) = match window(src, after, QUERY_WINDOW_EVENTS, &filter, QUERY_WINDOW_BYTES) {
        Ok(v) => v,
        Err(e) => return fail(e),
    };
    let more = scanned_last < src.log_count();
    if js.is_some() {
        let mut out = run_query(array_text(&events.iter().map(|l| l.text.as_str()).collect::<Vec<_>>()), js);
        if more {
            out["next"] = json!(scanned_last);
        }
        return out;
    }
    let page = page_of(&events, limit);
    let next = if page.len() < events.len() {
        Some(events[page.len() - 1].seq)
    } else if more {
        Some(scanned_last)
    } else {
        None
    };
    let mut out = run_query(array_text(&page), None);
    if let Some(n) = next {
        out["next"] = json!(n);
    }
    out
}

fn query_log_verb(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    Box::pin(async move {
        let run = ctx.run.clone();
        tokio::task::spawn_blocking(move || query_log(run.as_ref(), &args)).await.map_err(|e| e.to_string())
    })
}

pub const QUERY_LOG_DESCRIPTION: &str = "Read the run events, as JSON. after is a seq: only the events after it are read (since), so after the last seq you saw reads on from there. kind keeps events of one kind, and fromMono and toMono keep events in a time window (monotonic milliseconds). last N returns the newest N matching events instead, oldest first, at most 50, and has no next. js is a JavaScript expression over the matching events, and defaults to returning them: js sees one window of at most 1000 events or 4 MB of log. Without js, a reply holds at most 50 events and about 20 KB; limit (1 to 50) lowers that count, and is refused with last or js. When events remain, next is the seq to pass back as after; with js, next is set when the window ends before the end of the log. An unknown argument is refused, and the reply names the arguments query_log takes.";

pub const VERBS: &[VerbDef] = &[VerbDef {
    name: "query_log",
    description: QUERY_LOG_DESCRIPTION,
    schema: r#"{"type":"object","properties":{"js":{"type":"string"},"kind":{"type":"string"},"fromMono":{"type":"number"},"toMono":{"type":"number"},"after":{"type":"number"},"last":{"type":"number"},"limit":{"type":"number"}},"required":[]}"#,
    call: query_log_verb,
}];
