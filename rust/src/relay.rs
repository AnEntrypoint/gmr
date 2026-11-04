use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::hot;
use crate::update;

const RETRY_MIN: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);
const EXIT_WAIT: Duration = Duration::from_secs(5);
const HANDSHAKE_WAIT: Duration = Duration::from_secs(10);
const TICK: Duration = Duration::from_secs(1);
const MEMORY_TICKS: u32 = 10;
const HANDSHAKE_ID: &str = "gm-new-supervisor-handshake";
const HOST_CHANGED: [&str; 3] = [
    "notifications/tools/list_changed",
    "notifications/prompts/list_changed",
    "notifications/resources/list_changed",
];
const TAIL_KEEP: usize = 4000;
const TAIL_SHOWN: usize = 1500;
const LINES_QUEUED: usize = 64;

static STOP: AtomicBool = AtomicBool::new(false);

fn log(message: &str) {
    eprintln!("gmnew relay: {message}");
}

#[cfg(unix)]
extern "C" fn on_signal(_: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

fn install_signals() {
    #[cfg(unix)]
    unsafe {
        let handler = on_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
        libc::signal(libc::SIGTERM, handler);
        libc::signal(libc::SIGINT, handler);
    }
}

pub fn own_group(cmd: &mut Command) {
    #[cfg(unix)]
    {
        cmd.process_group(0);
    }
    #[cfg(target_os = "linux")]
    unsafe {
        cmd.pre_exec(|| {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
            Ok(())
        });
    }
    #[cfg(windows)]
    {
        cmd.creation_flags(0x0000_0200);
    }
}

pub fn signal_group(pid: u32, force: bool) {
    if pid == 0 {
        return;
    }
    #[cfg(unix)]
    unsafe {
        libc::kill(-(pid as libc::pid_t), if force { libc::SIGKILL } else { libc::SIGTERM });
    }
    #[cfg(windows)]
    {
        if force {
            let _ = std::process::Command::new("taskkill")
                .args(["/PID", &pid.to_string(), "/T", "/F"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

pub fn describe_exit(status: Option<ExitStatus>) -> String {
    let Some(status) = status else { return "unknown".to_string() };
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(sig) = status.signal() {
            return match sig {
                libc::SIGTERM => "SIGTERM".to_string(),
                libc::SIGKILL => "SIGKILL".to_string(),
                libc::SIGINT => "SIGINT".to_string(),
                libc::SIGABRT => "SIGABRT".to_string(),
                libc::SIGSEGV => "SIGSEGV".to_string(),
                libc::SIGBUS => "SIGBUS".to_string(),
                libc::SIGHUP => "SIGHUP".to_string(),
                libc::SIGPIPE => "SIGPIPE".to_string(),
                other => format!("signal {other}"),
            };
        }
    }
    match status.code() {
        Some(code) => format!("code {code}"),
        None => "unknown".to_string(),
    }
}

async fn pump<W: AsyncWrite + Unpin>(mut sink: W, mut rx: mpsc::Receiver<String>, name: &'static str, keep_on_error: bool) {
    let mut broken = false;
    while let Some(line) = rx.recv().await {
        if broken {
            continue;
        }
        let mut result = sink.write_all(line.as_bytes()).await;
        while result.is_ok() {
            match rx.try_recv() {
                Ok(more) => result = sink.write_all(more.as_bytes()).await,
                Err(_) => break,
            }
        }
        if result.is_ok() {
            result = sink.flush().await;
        }
        if let Err(e) = result {
            log(&format!("{name}: {e}"));
            if !keep_on_error {
                return;
            }
            broken = true;
        }
    }
}

async fn read_lines<R: tokio::io::AsyncRead + Unpin, T>(source: R, tx: mpsc::Sender<T>, wrap: impl Fn(Option<String>) -> T) {
    let mut reader = BufReader::new(source);
    loop {
        let mut buf = Vec::new();
        match reader.read_until(b'\n', &mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                while matches!(buf.last(), Some(b'\n' | b'\r')) {
                    buf.pop();
                }
                if buf.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                let line = String::from_utf8(buf).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
                if tx.send(wrap(Some(line))).await.is_err() {
                    return;
                }
            }
        }
    }
    let _ = tx.send(wrap(None)).await;
}

fn tail_text(tail: &Mutex<String>, keep: usize) -> String {
    let text = tail.lock().map(|t| t.clone()).unwrap_or_default();
    let mut start = text.len().saturating_sub(keep);
    while !text.is_char_boundary(start) {
        start += 1;
    }
    text[start..].to_string()
}

async fn copy_stderr(mut source: tokio::process::ChildStderr, tail: Arc<Mutex<String>>) {
    let mut buf = vec![0u8; 4096];
    loop {
        let n = match source.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        {
            use std::io::Write;
            let _ = std::io::stderr().write_all(&buf[..n]);
        }
        if let Ok(mut t) = tail.lock() {
            t.push_str(&String::from_utf8_lossy(&buf[..n]));
            if t.len() > TAIL_KEEP * 2 {
                let mut cut = t.len() - TAIL_KEEP;
                while !t.is_char_boundary(cut) {
                    cut += 1;
                }
                t.drain(..cut);
            }
        }
    }
}

fn parse_head(line: &str) -> (Option<Value>, Option<String>) {
    match serde_json::from_str::<Value>(line) {
        Ok(Value::Object(mut map)) => {
            let id = map.remove("id").filter(|id| !id.is_null());
            let method = match map.remove("method") {
                Some(Value::String(m)) => Some(m),
                _ => None,
            };
            (id, method)
        }
        _ => (None, None),
    }
}

fn id_key(id: &Value) -> String {
    id.to_string()
}

fn error_reply(id: &Value, message: &str) -> String {
    let mut line = json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32000, "message": message } }).to_string();
    line.push('\n');
    line
}

enum WorkerEvent {
    Line(u64, String),
    End(u64),
}

struct Worker {
    child: Child,
    pid: u32,
    gen: u64,
    stdin: mpsc::Sender<String>,
}

struct Relay {
    exe: PathBuf,
    args: Vec<String>,
    out: mpsc::Sender<String>,
    events: mpsc::Sender<WorkerEvent>,
    worker: Option<Worker>,
    gen: u64,
    inflight: HashMap<String, Value>,
    init: Option<Value>,
    failure: Option<String>,
    retry_delay: Duration,
    retry_at: Option<Instant>,
    want: Option<String>,
    watch: hot::Watch,
    tail: Arc<Mutex<String>>,
    heard: bool,
    memory_limit_mb: u64,
}

impl Relay {
    async fn to_host(&self, mut line: String) {
        line.push('\n');
        let _ = self.out.send(line).await;
    }

    fn spawn_worker(&mut self, reason: Option<&str>) -> Result<(), String> {
        let mut cmd = Command::new(&self.exe);
        cmd.arg("worker").args(&self.args);
        if let Some(reason) = reason {
            cmd.arg("--restart-reason").arg(reason);
        }
        cmd.arg("--supervised").arg("yes");
        cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
        own_group(&mut cmd);
        let mut child = cmd.spawn().map_err(|e| format!("could not start {}: {e}", self.exe.display()))?;
        self.gen += 1;
        let gen = self.gen;
        let pid = child.id().unwrap_or(0);
        let (stdin_tx, stdin_rx) = mpsc::channel::<String>(LINES_QUEUED);
        if let Some(stdin) = child.stdin.take() {
            tokio::spawn(pump(stdin, stdin_rx, "worker input", false));
        }
        if let Some(stdout) = child.stdout.take() {
            let tx = self.events.clone();
            tokio::spawn(read_lines(stdout, tx, move |line| match line {
                Some(line) => WorkerEvent::Line(gen, line),
                None => WorkerEvent::End(gen),
            }));
        }
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(copy_stderr(stderr, self.tail.clone()));
        }
        log(&format!("worker started (pid {pid})"));
        self.heard = false;
        self.worker = Some(Worker { child, pid, gen, stdin: stdin_tx });
        Ok(())
    }

    async fn stop_worker(&mut self) {
        let Some(mut worker) = self.worker.take() else { return };
        if let Ok(Some(_)) = worker.child.try_wait() {
            return;
        }
        #[cfg(unix)]
        signal_group(worker.pid, false);
        drop(worker.stdin);
        if tokio::time::timeout(EXIT_WAIT, worker.child.wait()).await.is_err() {
            signal_group(worker.pid, true);
            let _ = worker.child.start_kill();
            let _ = worker.child.wait().await;
        }
    }

    async fn fail_with(&mut self, reason: String) {
        log(&reason);
        let failure = format!("{reason}. Last stderr of the worker:\n{}", tail_text(&self.tail, TAIL_SHOWN));
        for (_, id) in self.inflight.drain() {
            let _ = self.out.send(error_reply(&id, &failure)).await;
        }
        self.inflight.shrink_to(16);
        self.failure = Some(failure);
        let delay = self.retry_delay;
        self.retry_delay = (self.retry_delay * 2).min(RETRY_MAX);
        log(&format!("retrying the restart in {} ms", delay.as_millis()));
        self.retry_at = Some(Instant::now() + delay);
    }

    async fn on_host_line(&mut self, line: String) {
        let (id, method) = parse_head(&line);
        if method.as_deref() == Some("initialize") {
            if let Ok(Value::Object(mut map)) = serde_json::from_str::<Value>(&line) {
                self.init = Some(map.remove("params").unwrap_or_else(|| json!({})));
            }
        }
        let request = match (&id, &method) {
            (Some(id), Some(_)) => Some(id.clone()),
            _ => None,
        };
        let failure = match (&self.failure, &self.worker) {
            (Some(text), _) => Some(text.clone()),
            (None, None) => Some("the worker is not running".to_string()),
            _ => None,
        };
        if let Some(text) = failure {
            if let Some(id) = request {
                let _ = self.out.send(error_reply(&id, &text)).await;
            }
            return;
        }
        if let Some(id) = request {
            self.inflight.insert(id_key(&id), id);
        }
        let mut line = line;
        line.push('\n');
        if let Some(worker) = &self.worker {
            let _ = worker.stdin.send(line).await;
        }
    }

    async fn on_worker_line(&mut self, line: String) {
        self.heard = true;
        if self.failure.is_none() && self.init.is_none() {
            self.retry_delay = RETRY_MIN;
        }
        let (id, method) = parse_head(&line);
        if let (Some(id), None) = (&id, &method) {
            if id.as_str() == Some(HANDSHAKE_ID) {
                return;
            }
            self.inflight.remove(&id_key(id));
        }
        self.to_host(line).await;
    }

    async fn worker_status(&mut self) -> Option<ExitStatus> {
        let worker = self.worker.as_mut()?;
        match tokio::time::timeout(EXIT_WAIT, worker.child.wait()).await {
            Ok(Ok(status)) => Some(status),
            _ => {
                signal_group(worker.pid, true);
                let _ = worker.child.start_kill();
                worker.child.wait().await.ok()
            }
        }
    }

    async fn handshake(&mut self, events: &mut mpsc::Receiver<WorkerEvent>, params: Value) -> Result<(), String> {
        let gen = self.gen;
        let mut first = json!({ "jsonrpc": "2.0", "id": HANDSHAKE_ID, "method": "initialize", "params": params }).to_string();
        first.push('\n');
        if let Some(worker) = &self.worker {
            let _ = worker.stdin.send(first).await;
        }
        let deadline = Instant::now() + HANDSHAKE_WAIT;
        loop {
            let event = match tokio::time::timeout_at(deadline, events.recv()).await {
                Err(_) | Ok(None) => return Err("the worker did not answer the handshake".to_string()),
                Ok(Some(event)) => event,
            };
            match event {
                WorkerEvent::Line(g, line) if g == gen => {
                    let parsed: Option<Value> = serde_json::from_str(&line).ok();
                    let is_answer = parsed.as_ref().and_then(|v| v.get("id")).and_then(Value::as_str) == Some(HANDSHAKE_ID);
                    if !is_answer {
                        self.on_worker_line(line).await;
                        continue;
                    }
                    if let Some(error) = parsed.as_ref().and_then(|v| v.get("error")) {
                        let message = error.get("message").and_then(Value::as_str).unwrap_or("initialize failed");
                        return Err(message.to_string());
                    }
                    let mut done = json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }).to_string();
                    done.push('\n');
                    if let Some(worker) = &self.worker {
                        let _ = worker.stdin.send(done).await;
                    }
                    return Ok(());
                }
                WorkerEvent::End(g) if g == gen => {
                    let how = describe_exit(self.worker_status().await);
                    return Err(format!("the worker exited ({how}) before the handshake"));
                }
                _ => {}
            }
        }
    }

    async fn restart(&mut self, events: &mut mpsc::Receiver<WorkerEvent>, reason: String) {
        log(&format!("restarting the worker ({reason})"));
        self.want = None;
        self.retry_at = None;
        self.stop_worker().await;
        if let Err(e) = self.spawn_worker(Some(&reason)) {
            self.fail_with(format!("the worker failed to restart ({e})")).await;
            return;
        }
        if let Some(params) = self.init.clone() {
            if let Err(e) = self.handshake(events, params).await {
                self.stop_worker().await;
                self.fail_with(format!("the worker failed to restart ({e})")).await;
                return;
            }
            self.retry_delay = RETRY_MIN;
            self.failure = None;
            for method in HOST_CHANGED {
                self.to_host(json!({ "jsonrpc": "2.0", "method": method }).to_string()).await;
            }
        }
        self.failure = None;
    }

    async fn on_worker_end(&mut self, gen: u64) {
        if self.worker.as_ref().map(|w| w.gen) != Some(gen) {
            return;
        }
        let how = describe_exit(self.worker_status().await);
        self.worker = None;
        self.fail_with(format!("the worker exited unexpectedly ({how})")).await;
    }

    fn resident_mb(&self) -> u64 {
        let Some(worker) = &self.worker else { return 0 };
        let Ok(text) = std::fs::read_to_string(format!("/proc/{}/status", worker.pid)) else { return 0 };
        text.lines()
            .find_map(|l| l.strip_prefix("VmRSS:"))
            .and_then(|v| v.split_whitespace().next())
            .and_then(|kb| kb.parse::<u64>().ok())
            .map_or(0, |kb| kb / 1024)
    }

    fn due(&self) -> Option<String> {
        if self.failure.is_some() {
            if let Some(reason) = &self.want {
                return Some(reason.clone());
            }
            if self.retry_at.is_some_and(|at| at <= Instant::now()) {
                return Some("retry after a failed restart".to_string());
            }
            return None;
        }
        if self.inflight.is_empty() {
            return self.want.clone();
        }
        None
    }
}

pub async fn serve(args: Vec<String>) -> i32 {
    let mut args = args;
    if !args.iter().any(|a| a == "--workspace") {
        if let Ok(cwd) = std::env::current_dir() {
            args.push("--workspace".to_string());
            args.push(cwd.to_string_lossy().into_owned());
        }
    }
    let exe = match std::env::current_exe() {
        Ok(exe) => exe,
        Err(e) => {
            log(&format!("cannot find the executable path: {e}"));
            return 1;
        }
    };
    install_signals();

    let (out_tx, out_rx) = mpsc::channel::<String>(LINES_QUEUED);
    let writer = tokio::spawn(pump(tokio::io::stdout(), out_rx, "host output", true));
    let (host_tx, mut host_rx) = mpsc::channel::<Option<String>>(LINES_QUEUED);
    tokio::spawn(read_lines(tokio::io::stdin(), host_tx, |line| line));
    let (events_tx, mut events) = mpsc::channel::<WorkerEvent>(LINES_QUEUED);

    update::start(&exe);

    let memory_limit_mb = std::env::var("GMNEW_MEMORY_LIMIT_MB").ok().and_then(|v| v.parse().ok()).unwrap_or(512);
    let mut relay = Relay {
        watch: hot::Watch::new(exe.clone()),
        exe,
        args,
        out: out_tx,
        events: events_tx,
        worker: None,
        gen: 0,
        inflight: HashMap::new(),
        init: None,
        failure: None,
        retry_delay: RETRY_MIN,
        retry_at: None,
        want: None,
        tail: Arc::new(Mutex::new(String::new())),
        heard: false,
        memory_limit_mb,
    };
    if let Err(e) = relay.spawn_worker(None) {
        relay.fail_with(format!("the worker failed to start ({e})")).await;
    }

    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ticks: u32 = 0;
    loop {
        let open = relay.want.is_none() || relay.failure.is_some();
        tokio::select! {
            host = host_rx.recv(), if open => match host {
                Some(Some(line)) => relay.on_host_line(line).await,
                _ => {
                    log("host closed the connection; pausing the worker");
                    break;
                }
            },
            event = events.recv() => match event {
                Some(WorkerEvent::Line(gen, line)) => {
                    if relay.worker.as_ref().map(|w| w.gen) == Some(gen) {
                        relay.on_worker_line(line).await;
                    }
                }
                Some(WorkerEvent::End(gen)) => relay.on_worker_end(gen).await,
                None => {}
            },
            _ = tick.tick() => {
                if STOP.load(Ordering::SeqCst) {
                    log("signal received; pausing the worker");
                    break;
                }
                if relay.watch.poll() {
                    relay.want = Some(format!("changed: {}", relay.watch.path().display()));
                }
                ticks = ticks.wrapping_add(1);
                if ticks % MEMORY_TICKS == 0 && relay.want.is_none() && relay.failure.is_none() && relay.heard {
                    let mb = relay.resident_mb();
                    if mb > relay.memory_limit_mb {
                        relay.want = Some(format!("memory limit {} MB passed", relay.memory_limit_mb));
                    }
                }
            }
        }
        if let Some(reason) = relay.due() {
            relay.restart(&mut events, reason).await;
        }
    }

    relay.stop_worker().await;
    update::stop_children();
    drop(relay);
    let _ = tokio::time::timeout(EXIT_WAIT, writer).await;
    0
}
