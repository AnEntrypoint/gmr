use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use crate::log::{self, LogFile, LogSource};

pub const MCP_RULES: &str = "# Rules

1. Paths may point inside or outside the workspace. An outside path should be absolute.
2. Call next_context at the start of every step. Each call is one step; stepsLeft counts the steps the run has left.
3. Verify before you finish: run what you built and read its output.
4. finish needs a witness: one string that quotes the evidence you observed. Calls sent with finish may be refused.
5. Events older than the window are not in next_context. Query them with query_log. Do not guess.
6. Never print, store or request secrets, environment variables or API keys.
7. If the task cannot be done, finish with a witness that states why, with the evidence.
";

pub const DEFAULT_WITNESS: &str =
    "Finish only with a witness: one string that states the evidence (command output, file content or query result) showing the task is done.";

pub const DEFAULT_MAX_STEPS: u64 = 100_000;
pub const DEFAULT_MAX_INFLIGHT: u64 = 8;
const RESERVED: &[&str] = &["seq", "t", "mono", "inFlight", "kind"];

pub struct Run {
    pub id: String,
    pub dir: PathBuf,
    pub workspace: PathBuf,
    pub replaced: Option<String>,
    log: Mutex<LogFile>,
    log_path: PathBuf,
    started: Instant,
    mono_base: f64,
    in_flight: AtomicI64,
    steps: AtomicU64,
    max_steps: u64,
    shown: AtomicU64,
    notice_seq: AtomicU64,
    ended: Mutex<Option<String>>,
    end_logged: std::sync::atomic::AtomicBool,
    task: Mutex<String>,
    tools_text: Mutex<String>,
    recovery: Mutex<Option<Value>>,
    delivered: Mutex<Vec<PathBuf>>,
    notify: tokio::sync::Notify,
}

pub fn new_run_id() -> String {
    let iso = log::iso_now();
    let stamp: String = iso[..19].chars().map(|c| if c == ':' || c == 'T' { '-' } else { c }).collect();
    format!("{stamp}-{}", log::random_hex(3))
}

pub fn current_run_dir(run_dir: &Path) -> PathBuf {
    let mut seen = std::collections::HashSet::new();
    let mut dir = run_dir.to_path_buf();
    while !seen.contains(&dir) {
        let Ok(name) = fs::read_to_string(dir.join("successor")) else { break };
        let name = name.trim();
        if name.is_empty() {
            break;
        }
        seen.insert(dir.clone());
        dir = dir.parent().map(|p| p.join(name)).unwrap_or_else(|| PathBuf::from(name));
    }
    dir
}

pub fn set_successor(run_dir: &Path, successor: &Path) -> std::io::Result<()> {
    let file = run_dir.join("successor");
    let tmp = run_dir.join("successor.tmp");
    let name = successor.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    fs::write(&tmp, name)?;
    fs::rename(tmp, file)
}

pub fn load_task(value: &str) -> String {
    let path = Path::new(value);
    if path.is_file() {
        if let Ok(text) = fs::read_to_string(path) {
            return text;
        }
    }
    value.to_string()
}

fn hash12(text: &str) -> String {
    sha_hex(text.as_bytes())[..12].to_string()
}

pub fn sha_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|b| format!("{b:02x}")).collect()
}

fn absolute(p: &Path) -> PathBuf {
    std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf())
}

fn sweep_ended_code(runs_root: &Path, current: &Path) {
    let Ok(entries) = fs::read_dir(runs_root) else { return };
    for entry in entries.filter_map(|e| e.ok()) {
        let dir = entry.path();
        if dir == current || !dir.join("code").exists() {
            continue;
        }
        let ended = log::last_line_of(&dir.join(log::LOG_FILE)).is_some_and(|e| e.get("kind").and_then(Value::as_str) == Some("end"));
        if ended {
            let _ = fs::remove_dir_all(dir.join("code"));
        }
    }
}

impl Run {
    pub fn open(workspace: &Path, run_dir: Option<&Path>, task: Option<&str>) -> Result<Run, String> {
        Run::open_with(workspace, run_dir, task, DEFAULT_MAX_STEPS, DEFAULT_MAX_INFLIGHT)
    }

    pub fn open_with(workspace: &Path, run_dir: Option<&Path>, task: Option<&str>, max_steps: u64, max_inflight: u64) -> Result<Run, String> {
        let workspace = absolute(workspace);
        if !workspace.is_dir() {
            return Err(format!("workspace is not a directory: {}", workspace.display()));
        }
        let task = match task {
            Some(t) => {
                let text = load_task(t).trim().to_string();
                if text.is_empty() {
                    return Err("--task is empty".to_string());
                }
                Some(text)
            }
            None => None,
        };
        let requested = match run_dir {
            Some(d) => absolute(d),
            None => workspace.join("runs").join(new_run_id()),
        };
        let dir = current_run_dir(&requested);
        let log_path = dir.join(log::LOG_FILE);
        let run = if log_path.exists() {
            let (file, scan) = LogFile::open(&log_path).map_err(|e| format!("{}: {e}", log_path.display()))?;
            match scan.ended.clone() {
                Some(_) => {
                    let closed_id = name_of(&dir);
                    let (text, from) = match task {
                        Some(t) => (t, "task file".to_string()),
                        None => {
                            let closed = scan.task.clone().unwrap_or_else(|| task_json(&dir));
                            let from = if closed.is_empty() { "session prompt".to_string() } else { format!("closed run {closed_id}") };
                            (closed, from)
                        }
                    };
                    drop(file);
                    let next = dir.parent().unwrap_or(Path::new(".")).join(new_run_id());
                    let mut run = Run::create(&workspace, &next, &text, &from, true, max_steps, max_inflight)?;
                    set_successor(&dir, &next).map_err(|e| e.to_string())?;
                    run.replaced = Some(closed_id);
                    run
                }
                None => {
                    let text = task.or(scan.task.clone()).unwrap_or_else(|| task_json(&dir));
                    let run = Run::assemble(&workspace, &dir, file, &scan, text, max_steps);
                    run.refresh_rules();
                    run.event("restart", json!({ "reason": std::env::var("GMNEW_RESTART_REASON").unwrap_or_else(|_| "restarted without a reason".to_string()) }));
                    if !scan.open_actions.is_empty() {
                        let last_seq = scan.count;
                        run.event("recovery", json!({ "lastSeq": last_seq, "open": scan.open_actions }));
                        *run.recovery.lock().unwrap() = Some(json!({ "lastSeq": last_seq, "open": scan.open_actions }));
                    }
                    run
                }
            }
        } else {
            let from = if task.is_some() { "task file" } else { "session prompt" };
            Run::create(&workspace, &dir, task.as_deref().unwrap_or(""), from, false, max_steps, max_inflight)?
        };
        if let Some(root) = run.dir.parent() {
            sweep_ended_code(root, &run.dir);
        }
        Ok(run)
    }

    fn create(workspace: &Path, dir: &Path, task: &str, task_from: &str, log_task: bool, max_steps: u64, max_inflight: u64) -> Result<Run, String> {
        let log_path = dir.join(log::LOG_FILE);
        if log_path.exists() {
            return Err(format!("run directory already has a log: {}", dir.display()));
        }
        fs::create_dir_all(dir.join("jobs")).map_err(|e| e.to_string())?;
        fs::write(dir.join("rules.md"), MCP_RULES).map_err(|e| e.to_string())?;
        let meta = json!({
            "id": name_of(dir), "mode": "mcp", "task": task, "witness": DEFAULT_WITNESS,
            "workspace": workspace.to_string_lossy(), "maxSteps": max_steps, "maxInflight": max_inflight, "createdAt": log::iso_now(),
        });
        let text = serde_json::to_string_pretty(&meta).map_err(|e| e.to_string())?;
        fs::write(dir.join("task.json"), format!("{text}\n")).map_err(|e| e.to_string())?;
        let (file, scan) = LogFile::open(&log_path).map_err(|e| e.to_string())?;
        let run = Run::assemble(workspace, dir, file, &scan, task.to_string(), max_steps);
        run.event("start", json!({ "mode": "mcp", "maxSteps": max_steps, "maxInflight": max_inflight, "taskFrom": task_from }));
        if log_task && !task.is_empty() {
            run.event("task", json!({ "from": task_from, "text": task }));
        }
        Ok(run)
    }

    #[allow(clippy::too_many_arguments)]
    fn assemble(workspace: &Path, dir: &Path, file: LogFile, scan: &log::Scan, task: String, max_steps: u64) -> Run {
        Run {
            id: name_of(dir),
            dir: dir.to_path_buf(),
            workspace: workspace.to_path_buf(),
            replaced: None,
            log_path: file.path().to_path_buf(),
            log: Mutex::new(file),
            started: Instant::now(),
            mono_base: scan.last_mono,
            in_flight: AtomicI64::new(0),
            steps: AtomicU64::new(scan.steps),
            max_steps,
            shown: AtomicU64::new(scan.shown),
            notice_seq: AtomicU64::new(scan.notice_seq),
            ended: Mutex::new(None),
            end_logged: std::sync::atomic::AtomicBool::new(false),
            task: Mutex::new(task),
            tools_text: Mutex::new(String::new()),
            recovery: Mutex::new(None),
            delivered: Mutex::new(Vec::new()),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn refresh_rules(&self) {
        let path = self.dir.join("rules.md");
        let before = fs::read_to_string(&path).ok();
        if before.as_deref() == Some(MCP_RULES) {
            return;
        }
        if fs::write(&path, MCP_RULES).is_ok() {
            self.event("rules-updated", json!({ "from": before.as_deref().map(hash12), "to": hash12(MCP_RULES) }));
        }
    }

    pub fn now(&self) -> f64 {
        log::round3(self.mono_base + self.started.elapsed().as_secs_f64() * 1000.0)
    }

    pub fn enter(&self) -> i64 {
        self.in_flight.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn leave(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn event(&self, kind: &str, fields: Value) -> u64 {
        self.event_at(kind, fields, None)
    }

    pub fn event_at(&self, kind: &str, fields: Value, mono_at: Option<f64>) -> u64 {
        let mono = mono_at.unwrap_or_else(|| self.now());
        let empty = Map::new();
        let map = fields.as_object().unwrap_or(&empty);
        let dur = map.get("startMono").and_then(Value::as_f64).map(|s| log::round3(mono - s));
        let mut log = self.log.lock().unwrap_or_else(|p| p.into_inner());
        let seq = log.count() + 1;
        let mut line = String::with_capacity(128);
        line.push_str("{\"seq\":");
        line.push_str(&seq.to_string());
        line.push_str(",\"t\":\"");
        line.push_str(&log::iso_now());
        line.push_str("\",\"mono\":");
        line.push_str(&log::num(mono).to_string());
        line.push_str(",\"inFlight\":");
        line.push_str(&self.in_flight.load(Ordering::SeqCst).to_string());
        line.push_str(",\"kind\":");
        line.push_str(&Value::from(kind).to_string());
        for (k, v) in map.iter().filter(|(k, _)| !RESERVED.contains(&k.as_str()) && !(dur.is_some() && k.as_str() == "durMs")) {
            line.push(',');
            line.push_str(&Value::from(k.as_str()).to_string());
            line.push(':');
            line.push_str(&v.to_string());
        }
        if let Some(d) = dur {
            line.push_str(",\"durMs\":");
            line.push_str(&log::num(d).to_string());
        }
        line.push_str("}\n");
        if let Err(e) = log.append(line.as_bytes()) {
            eprintln!("gmnew: log append failed: {e}");
            return 0;
        }
        drop(log);
        if log::is_pending_kind(kind) {
            let record = format!("{{\"id\":{seq},{}", &line[1..]);
            if let Err(e) = log::append_pending(&self.dir, record.as_bytes()) {
                eprintln!("gmnew: pending append failed: {e}");
            }
            self.notify.notify_waiters();
        }
        seq
    }

    pub fn count(&self) -> u64 {
        self.log.lock().unwrap_or_else(|p| p.into_inner()).count()
    }

    pub fn read_lines(&self, from_seq: u64, to_seq: u64) -> Vec<Value> {
        let Some((begin, end)) = self.log_bounds(from_seq, to_seq) else { return Vec::new() };
        let Ok(bytes) = log::read_range(&self.log_path, begin, end) else { return Vec::new() };
        log::split_lines(&bytes).filter_map(|l| serde_json::from_slice(l).ok()).collect()
    }

    pub fn at(&self, seq: u64) -> Option<Value> {
        self.read_lines(seq, seq).into_iter().next()
    }

    pub fn for_each_event(&self, mut f: impl FnMut(&Value)) {
        use std::io::BufRead;
        let count = self.count();
        let Some((_, end)) = self.log_bounds(1, count) else { return };
        let Ok(file) = fs::File::open(&self.log_path) else { return };
        let mut reader = std::io::BufReader::with_capacity(1 << 16, std::io::Read::take(file, end));
        let mut buf = Vec::new();
        while reader.read_until(b'\n', &mut buf).map(|n| n > 0).unwrap_or(false) {
            if let Ok(v) = serde_json::from_slice::<Value>(&buf) {
                f(&v);
            }
            buf.clear();
        }
    }

    pub fn step(&self) -> u64 {
        self.steps.load(Ordering::SeqCst)
    }

    pub fn max_steps(&self) -> u64 {
        self.max_steps
    }

    pub fn begin_step(&self) -> Result<u64, String> {
        let step = self.steps.fetch_add(1, Ordering::SeqCst) + 1;
        if step > self.max_steps {
            self.steps.fetch_sub(1, Ordering::SeqCst);
            return Err(self.step_limit_text());
        }
        Ok(step)
    }

    pub fn step_limit_text(&self) -> String {
        format!("step limit {} reached; a restart of the server opens a new run", self.max_steps)
    }

    pub fn shown(&self) -> u64 {
        self.shown.load(Ordering::SeqCst)
    }

    pub fn set_shown(&self, total: u64) {
        self.shown.store(total, Ordering::SeqCst);
    }

    pub fn swap_notice_seq(&self, now: u64) -> u64 {
        self.notice_seq.swap(now, Ordering::SeqCst)
    }

    pub fn take_recovery(&self) -> Option<Value> {
        self.recovery.lock().unwrap_or_else(|p| p.into_inner()).take()
    }

    pub fn task(&self) -> String {
        self.task.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    pub fn set_task(&self, text: &str, from: &str) {
        *self.task.lock().unwrap_or_else(|p| p.into_inner()) = text.to_string();
        self.event("task", json!({ "from": from, "text": text }));
    }

    pub fn tools_text(&self) -> String {
        self.tools_text.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    pub fn set_tools_text(&self, text: String) {
        *self.tools_text.lock().unwrap_or_else(|p| p.into_inner()) = text;
    }

    pub fn ended(&self) -> Option<String> {
        self.ended.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    pub fn ended_reason(&self, child: bool) -> String {
        match self.ended().as_deref() {
            Some("max_steps") => self.step_limit_text(),
            Some("closed") => "stdin closed before the call started".to_string(),
            Some("restarting") => "the server is restarting; repeat the call".to_string(),
            _ if child => format!("run {} has ended; the session that opened it calls next_context to open the next run", self.id),
            _ => format!("run {} has ended; call next_context to open a new run", self.id),
        }
    }

    pub fn set_delivered(&self, files: Vec<PathBuf>) {
        let mut d = self.delivered.lock().unwrap_or_else(|p| p.into_inner());
        log::clear_pending(&d);
        *d = files;
    }

    pub fn clear_delivered(&self) {
        let files = std::mem::take(&mut *self.delivered.lock().unwrap_or_else(|p| p.into_inner()));
        log::clear_pending(&files);
    }

    pub async fn hold(&self, wait_ms: u64) {
        if wait_ms == 0 {
            return;
        }
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if log::has_pending(&self.dir) {
                return;
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return;
            }
            let slice = (deadline - now).min(Duration::from_millis(200));
            let _ = tokio::time::timeout(slice, notified).await;
        }
    }

    pub fn pause(&self, reason: &str) {
        let mut ended = self.ended.lock().unwrap_or_else(|p| p.into_inner());
        if ended.is_some() {
            return;
        }
        *ended = Some("restarting".to_string());
        drop(ended);
        self.event("paused", json!({ "reason": reason }));
    }

    pub fn end_run(&self, status: &str, witness: Option<&str>, gaps: Option<Value>) {
        if self.end_logged.swap(true, Ordering::SeqCst) {
            return;
        }
        *self.ended.lock().unwrap_or_else(|p| p.into_inner()) = Some(status.to_string());
        self.write_summary();
        let mut fields = json!({ "status": status, "steps": self.step(), "witness": witness });
        if let Some(g) = gaps {
            fields["gaps"] = g;
        }
        self.event("end", fields);
        let _ = fs::remove_dir_all(self.dir.join("code"));
    }

    fn write_summary(&self) {
        let mut has = false;
        let mut first_mono: Option<f64> = None;
        let mut max_in_flight = 0i64;
        let mut tool = Vec::new();
        let mut model = Vec::new();
        let mut mcp = false;
        let mut start_seen = false;
        let mut context_calls = 0u64;
        let mut replies = 0u64;
        self.for_each_event(|e| {
            let kind = e.get("kind").and_then(Value::as_str).unwrap_or("");
            if kind == "summary" {
                has = true;
            }
            let mono = e.get("mono").and_then(Value::as_f64).unwrap_or(0.0);
            first_mono.get_or_insert(mono);
            max_in_flight = max_in_flight.max(e.get("inFlight").and_then(Value::as_i64).unwrap_or(0));
            if kind == "start" && !start_seen {
                start_seen = true;
                mcp = e.get("mode").and_then(Value::as_str) == Some("mcp");
            }
            if kind == "result" && e.get("tool").and_then(Value::as_str) == Some("next_context") {
                context_calls += 1;
            }
            if kind == "reply" {
                replies += 1;
            }
            if let Some(s) = e.get("startMono").and_then(Value::as_f64) {
                match kind {
                    "result" | "refusal" => tool.push((s, mono)),
                    "reply" | "retry" | "error" => model.push((s, mono)),
                    _ => {}
                }
            }
        });
        if has {
            return;
        }
        let end = self.now();
        let wall = first_mono.map(|f| end - f).unwrap_or(0.0);
        let total = |v: &[(f64, f64)]| v.iter().map(|(a, b)| b - a).sum::<f64>();
        let tool_raw = total(&tool);
        let mut both: Vec<(f64, f64)> = tool.iter().chain(model.iter()).copied().collect();
        let tool_m = total(&merge(&mut tool));
        let model_m = total(&merge(&mut model));
        let both_m = total(&merge(&mut both));
        let summary = json!({
            "maxInFlight": max_in_flight,
            "meanInFlight": log::num(if wall > 0.0 { log::round3(tool_raw / wall) } else { 0.0 }),
            "modelMs": log::num(log::round3(model_m)),
            "toolMs": log::num(log::round3(tool_m)),
            "idleMs": log::num(log::round3((wall - both_m).max(0.0))),
            "contextCalls": if mcp { context_calls } else { replies },
        });
        self.event_at("summary", summary, Some(end));
    }

    pub fn finish(&self, args: &Value, background_running: usize, stop_background_runs: impl FnOnce()) -> Value {
        let refuse = |reason: String| json!({ "ok": false, "refused": true, "error": format!("refused: {reason}") });
        let witness = match args.get("witness").and_then(Value::as_str) {
            Some(w) if !w.trim().is_empty() => w,
            _ => return refuse("finish needs a non-empty witness string".to_string()),
        };
        let gaps = args.get("gaps");
        if let Some(g) = gaps {
            let valid = g.as_array().is_some_and(|a| a.iter().all(|x| x.as_str().is_some_and(|s| !s.trim().is_empty())));
            if !valid {
                return refuse("gaps must be an array of non-empty strings".to_string());
            }
        }
        let stop_background = args.get("stopBackground") == Some(&Value::Bool(true));
        if background_running > 0 && !stop_background {
            return refuse(format!(
                "{background_running} background run(s) still running; job_status watches them, and finish with stopBackground: true kills them"
            ));
        }
        let open: Vec<(String, String)> = match crate::rows::open_rows(&self.workspace) {
            Ok(rows) => rows.iter().map(|row| (row_text(row, "id"), row_text(row, "title"))).collect(),
            Err(e) => return json!({ "ok": false, "error": e }),
        };
        let stop_rows = args.get("stopRows") == Some(&Value::Bool(true));
        if !open.is_empty() && !stop_rows {
            let names: Vec<String> = open.iter().take(5).map(|(id, title)| format!("{id} {title}")).collect();
            let more = if open.len() > 5 { "; ..." } else { "" };
            return refuse(format!(
                "{} row(s) open: {}{more}. Work them, or finish with stopRows: true to end with them listed as gaps",
                open.len(),
                names.join("; ")
            ));
        }
        let gaps = if stop_rows {
            let mut all: Vec<Value> = gaps.and_then(Value::as_array).cloned().unwrap_or_default();
            all.extend(open.iter().map(|(id, title)| Value::from(format!("open row {id}: {title}"))));
            Some(Value::Array(all))
        } else {
            gaps.cloned()
        };
        if stop_background {
            stop_background_runs();
        }
        let lint = crate::search::lint_wiki(&self.workspace);
        if !lint.skipped {
            self.event("lint", json!({ "problems": lint.problems, "pages": lint.pages }));
        }
        self.end_run("finished", Some(witness), gaps);
        json!({ "ok": true, "finished": true, "output": witness })
    }
}

fn merge(intervals: &mut [(f64, f64)]) -> Vec<(f64, f64)> {
    intervals.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut out: Vec<(f64, f64)> = Vec::new();
    for &(from, to) in intervals.iter() {
        match out.last_mut() {
            Some(last) if from <= last.1 => last.1 = last.1.max(to),
            _ => out.push((from, to)),
        }
    }
    out
}

fn name_of(dir: &Path) -> String {
    dir.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default()
}

fn task_json(dir: &Path) -> String {
    fs::read_to_string(dir.join("task.json"))
        .ok()
        .and_then(|t| serde_json::from_str::<Value>(&t).ok())
        .and_then(|v| v.get("task").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_default()
}

fn row_text(row: &Value, key: &str) -> String {
    row.get(key).and_then(Value::as_str).unwrap_or_default().to_string()
}

impl LogSource for Run {
    fn log_path(&self) -> PathBuf {
        self.log_path.clone()
    }

    fn log_count(&self) -> u64 {
        self.count()
    }

    fn log_bounds(&self, from_seq: u64, to_seq: u64) -> Option<(u64, u64)> {
        self.log.lock().unwrap_or_else(|p| p.into_inner()).bounds(from_seq, to_seq)
    }
}

pub const VERBS: &[crate::types::VerbDef] = &[];
