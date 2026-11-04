use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio::sync::watch;

use crate::fsverbs::{arg, char_total, chars_window, error, lossy, text_arg, whole, OUTPUT_PAGE};
use crate::run::Run;
use crate::types::{Ctx, Fut, VerbDef, VerbOutcome};

const OUTPUT_CAP: usize = 200_000;
const DEFAULT_TIMEOUT_MS: u64 = 60_000;
const MAX_TIMEOUT_MS: u64 = 600_000;
const MAX_WAIT_MS: u64 = 30_000;
const JOB_LIFETIME_MS: u64 = 1_800_000;
const TAIL_CHARS: usize = 2_000;
const TAIL_BYTES: u64 = 8_192;
const PIPE_CHUNK: usize = 8_192;
#[cfg(unix)]
const KILL_GRACE: Duration = Duration::from_millis(500);
#[cfg(windows)]
const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

const DESC_RUN_SHELL: &str = "Run a shell command in the workspace and wait for it. Default timeout 60000 ms. Its process group is killed when it returns, so background work must go through job_start.";
const SCHEMA_RUN_SHELL: &str = r#"{"type":"object","properties":{"command":{"type":"string"},"timeoutMs":{"type":"number"}},"required":["command"]}"#;
const DESC_JOB_START: &str =
    "Start a shell command as a named background job and return at once. Poll it with job_status.";
const SCHEMA_JOB_START: &str = r#"{"type":"object","properties":{"job":{"type":"string"},"command":{"type":"string"},"timeoutMs":{"type":"number"}},"required":["job","command"]}"#;
const DESC_JOB_STATUS: &str = "State (running, exited or killed), exitCode, signal and output of a job as plain fields, with logPath (the job output file). output is the last 20000 characters, with total (its length) and truncated; with offset, output is the 20000 characters from that offset, with next. waitMs polls until the job leaves running, at most 30000 ms. Offsets and lengths count characters, not lines.";
const SCHEMA_JOB_STATUS: &str = r#"{"type":"object","properties":{"job":{"type":"string"},"waitMs":{"type":"number"},"offset":{"type":"number"}},"required":["job"]}"#;
const DESC_JOB_STOP: &str = "Stop a background shell job or a background run_code body (body-<seq>): a shell job's process group is killed, a body is stopped at its next verb call, and the job ends killed with reason cancelled. The job event is logged before the answer. A job that already ended is an error result.";
const SCHEMA_JOB_STOP: &str =
    r#"{"type":"object","properties":{"job":{"type":"string"}},"required":["job"]}"#;

pub const VERBS: &[VerbDef] = &[
    VerbDef {
        name: "run_shell",
        description: DESC_RUN_SHELL,
        schema: SCHEMA_RUN_SHELL,
        call: run_shell,
    },
    VerbDef {
        name: "job_start",
        description: DESC_JOB_START,
        schema: SCHEMA_JOB_START,
        call: job_start,
    },
    VerbDef {
        name: "job_status",
        description: DESC_JOB_STATUS,
        schema: SCHEMA_JOB_STATUS,
        call: job_status,
    },
    VerbDef {
        name: "job_stop",
        description: DESC_JOB_STOP,
        schema: SCHEMA_JOB_STOP,
        call: job_stop,
    },
];

pub struct Jobs {
    dir: Mutex<Option<PathBuf>>,
    table: Mutex<HashMap<String, Arc<Job>>>,
    epoch: Instant,
}

struct Job {
    name: String,
    pid: u32,
    output: PathBuf,
    status_file: PathBuf,
    start_mono: f64,
    status: Mutex<Value>,
    kill: Mutex<Kill>,
    settled: watch::Sender<bool>,
}

#[derive(Default)]
struct Kill {
    requested: bool,
    reason: Option<&'static str>,
}

impl Jobs {
    pub fn new() -> Self {
        Jobs {
            dir: Mutex::new(None),
            table: Mutex::new(HashMap::new()),
            epoch: Instant::now(),
        }
    }

    pub fn set_dir(&self, dir: PathBuf) {
        *lock(&self.dir) = Some(dir);
    }

    pub fn running(&self) -> usize {
        lock(&self.table).values().filter(|job| !job.is_settled()).count()
    }

    pub fn kill_all(&self) {
        let jobs: Vec<Arc<Job>> = lock(&self.table).values().cloned().collect();
        for job in jobs {
            if !job.is_settled() {
                kill_group_now(job.pid);
            }
        }
    }

    fn run_dir(&self) -> Option<PathBuf> {
        lock(&self.dir).clone()
    }

    fn lookup(&self, name: &str) -> Option<Arc<Job>> {
        lock(&self.table).get(name).cloned()
    }

    fn status_of(&self, name: &str) -> Option<Value> {
        if let Some(job) = self.lookup(name) {
            return Some(lock(&job.status).clone());
        }
        let file = self.run_dir()?.join("jobs").join(format!("{name}.json"));
        serde_json::from_str(&fs::read_to_string(file).ok()?).ok()
    }

    fn mono_ms(&self) -> f64 {
        self.epoch.elapsed().as_secs_f64() * 1000.0
    }

    #[cfg(feature = "jit")]
    pub(crate) fn list(&self) -> Vec<Value> {
        lock(&self.table)
            .values()
            .map(|job| json!({ "job": job.name, "state": job.state(), "pid": job.pid, "startMono": job.start_mono }))
            .collect()
    }

    #[cfg(feature = "jit")]
    pub(crate) async fn status(&self, name: &str, wait_ms: u64, offset: Option<u64>) -> Option<Value> {
        if wait_ms > 0 {
            if let Some(job) = self.lookup(name) {
                if !job.is_settled() {
                    let mut settled = job.settled.subscribe();
                    let _ = tokio::time::timeout(Duration::from_millis(wait_ms.min(MAX_WAIT_MS)), settled.wait_for(|done| *done)).await;
                }
            }
        }
        self.status_of(name).map(|status| job_reply(status, offset))
    }

    pub(crate) fn start(
        &self,
        run: Arc<Run>,
        name: &str,
        command: &str,
        cwd: &Path,
        timeout_ms: u64,
    ) -> Value {
        let Some(dir) = self.run_dir() else {
            return error("no run directory for jobs");
        };
        if self.lookup(name).is_some() {
            return error(format!("job {name} already exists"));
        }
        let jobs_dir = dir.join("jobs");
        let output = jobs_dir.join(format!("{name}.out"));
        let spawned = fs::create_dir_all(&jobs_dir)
            .and_then(|()| File::create(&output))
            .and_then(|out| {
                let err = out.try_clone()?;
                shell(command, cwd)
                    .stdout(Stdio::from(out))
                    .stderr(Stdio::from(err))
                    .spawn()
            });
        let child = match spawned {
            Ok(child) => child,
            Err(e) => return error(e.to_string()),
        };
        let pid = child.id().unwrap_or(0);
        let start_mono = self.mono_ms();
        let status_file = jobs_dir.join(format!("{name}.json"));
        let status = json!({ "job": name, "command": command, "state": "running", "pid": pid, "startMono": start_mono, "output": output.display().to_string() });
        if let Err(e) = write_status(&status_file, &status) {
            kill_group_now(pid);
            return error(e.to_string());
        }
        let (settled, _) = watch::channel(false);
        let job = Arc::new(Job {
            name: name.to_string(),
            pid,
            output,
            status_file,
            start_mono,
            status: Mutex::new(status),
            kill: Mutex::new(Kill::default()),
            settled,
        });
        lock(&self.table).insert(name.to_string(), job.clone());
        run.event(
            "job",
            json!({ "job": name, "state": "running", "pid": pid }),
        );
        let lifetime_killed = JOB_LIFETIME_MS < timeout_ms;
        tokio::spawn(supervise(
            run,
            job,
            child,
            timeout_ms.min(JOB_LIFETIME_MS),
            lifetime_killed,
        ));
        json!({ "ok": true, "state": "running", "output": format!("started job {name} (pid {pid})") })
    }

    pub(crate) async fn stop(&self, name: &str) -> Value {
        let Some(job) = self.lookup(name) else {
            return error(format!("no job named {name}"));
        };
        if job.is_settled() {
            return error(format!("job {name} already ended ({})", job.state()));
        }
        let reason = {
            let mut kill = lock(&job.kill);
            kill.requested = true;
            let reason = *kill.reason.get_or_insert("cancelled");
            reason
        };
        kill_group(job.pid).await;
        let _ = job.settled.subscribe().wait_for(|done| *done).await;
        json!({ "ok": true, "state": "killed", "reason": reason, "output": format!("stopped job {name}") })
    }
}

impl Default for Jobs {
    fn default() -> Self {
        Self::new()
    }
}

impl Job {
    fn is_settled(&self) -> bool {
        *self.settled.borrow()
    }

    fn state(&self) -> String {
        lock(&self.status)
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    }

    fn request_kill(&self, reason: Option<&'static str>) {
        let mut kill = lock(&self.kill);
        kill.requested = true;
        if reason.is_some() {
            kill.reason = reason;
        }
    }

    fn settle(&self, run: &Run, code: Option<i32>, signal: Option<String>, lifetime_ms: u64) {
        let kill = lock(&self.kill);
        let state = if code.is_some() && !kill.requested {
            "exited"
        } else {
            "killed"
        };
        let reason = if state == "killed" { kill.reason } else { None };
        {
            let mut status = lock(&self.status);
            if let Some(map) = status.as_object_mut() {
                map.insert("state".into(), json!(state));
                map.insert("exitCode".into(), json!(code));
                map.insert("signal".into(), json!(signal));
                if let Some(reason) = reason {
                    map.insert("reason".into(), json!(reason));
                }
            }
            let _ = write_status(&self.status_file, &status);
        }
        let mut event = json!({ "job": self.name, "state": state, "exitCode": code, "signal": signal, "startMono": self.start_mono });
        if let Some(reason) = reason {
            event["reason"] = json!(reason);
        }
        run.event("job", event);
        if let Some(reason) = kill.reason {
            let mut fields =
                json!({ "job": self.name, "reason": reason, "tail": tail_of(&self.output) });
            if reason == "lifetime" {
                fields["lifetimeMs"] = json!(lifetime_ms);
            }
            run.event("job-killed", fields);
        }
        drop(kill);
        self.settled.send_replace(true);
    }
}

async fn supervise(
    run: Arc<Run>,
    job: Arc<Job>,
    mut child: Child,
    limit_ms: u64,
    lifetime_killed: bool,
) {
    let exit = tokio::select! {
        status = child.wait() => status.ok(),
        _ = tokio::time::sleep(Duration::from_millis(limit_ms)) => {
            job.request_kill(lifetime_killed.then_some("lifetime"));
            kill_group(job.pid).await;
            child.wait().await.ok()
        }
    };
    kill_group_now(job.pid);
    let code = exit.as_ref().and_then(ExitStatus::code);
    let signal = exit.as_ref().and_then(signal_name);
    job.settle(&run, code, signal, JOB_LIFETIME_MS);
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn write_status(file: &Path, status: &Value) -> io::Result<()> {
    let temp = PathBuf::from(format!("{}.{}.tmp", file.display(), std::process::id()));
    fs::write(&temp, format!("{status}\n"))?;
    fs::rename(&temp, file)
}

fn tail_of(path: &Path) -> String {
    let Ok(mut file) = File::open(path) else {
        return String::new();
    };
    let Ok(len) = file.metadata().map(|m| m.len()) else {
        return String::new();
    };
    if file
        .seek(SeekFrom::Start(len.saturating_sub(TAIL_BYTES)))
        .is_err()
    {
        return String::new();
    }
    let mut bytes = Vec::new();
    if file.read_to_end(&mut bytes).is_err() {
        return String::new();
    }
    let mut tail: Vec<char> = String::from_utf8_lossy(&bytes)
        .chars()
        .rev()
        .take(TAIL_CHARS)
        .collect();
    tail.reverse();
    tail.into_iter().collect()
}

pub(crate) fn shell(command: &str, cwd: &Path) -> Command {
    #[cfg(unix)]
    let mut cmd = {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(command).process_group(0);
        cmd
    };
    #[cfg(windows)]
    let mut cmd = {
        use std::os::windows::process::CommandExt;
        let mut cmd = Command::new("cmd");
        cmd.arg("/d").arg("/s").arg("/c");
        cmd.as_std_mut().raw_arg(format!("\"{command}\""));
        cmd.creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
        cmd
    };
    cmd.current_dir(cwd)
        .env_remove("ANTHROPIC_API_KEY")
        .stdin(Stdio::null());
    cmd
}

#[cfg(unix)]
fn signal_group(pid: u32, signal: libc::c_int) -> bool {
    if pid == 0 {
        return false;
    }
    unsafe { libc::killpg(pid as libc::pid_t, signal) == 0 }
}

#[cfg(unix)]
pub(crate) async fn kill_group(pid: u32) {
    if signal_group(pid, libc::SIGTERM) {
        tokio::time::sleep(KILL_GRACE).await;
        signal_group(pid, libc::SIGKILL);
    }
}

#[cfg(unix)]
pub(crate) fn kill_group_now(pid: u32) {
    signal_group(pid, libc::SIGKILL);
}

#[cfg(windows)]
pub(crate) async fn kill_group(pid: u32) {
    if pid != 0 {
        let _ = tokio::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
    }
}

#[cfg(windows)]
pub(crate) fn kill_group_now(pid: u32) {
    if pid != 0 {
        use std::os::windows::process::CommandExt;
        let _ = std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .creation_flags(CREATE_NO_WINDOW)
            .status();
    }
}

#[cfg(unix)]
pub(crate) fn signal_name(status: &ExitStatus) -> Option<String> {
    use std::os::unix::process::ExitStatusExt;
    let name = match status.signal()? {
        libc::SIGHUP => "SIGHUP",
        libc::SIGINT => "SIGINT",
        libc::SIGQUIT => "SIGQUIT",
        libc::SIGILL => "SIGILL",
        libc::SIGTRAP => "SIGTRAP",
        libc::SIGABRT => "SIGABRT",
        libc::SIGBUS => "SIGBUS",
        libc::SIGFPE => "SIGFPE",
        libc::SIGKILL => "SIGKILL",
        libc::SIGUSR1 => "SIGUSR1",
        libc::SIGSEGV => "SIGSEGV",
        libc::SIGUSR2 => "SIGUSR2",
        libc::SIGPIPE => "SIGPIPE",
        libc::SIGALRM => "SIGALRM",
        libc::SIGTERM => "SIGTERM",
        _ => return None,
    };
    Some(name.to_string())
}

#[cfg(not(unix))]
pub(crate) fn signal_name(_: &ExitStatus) -> Option<String> {
    None
}

struct Group(u32);

impl Drop for Group {
    fn drop(&mut self) {
        kill_group_now(self.0);
    }
}

fn number_of(args: &Value, key: &str) -> f64 {
    match args.get(key) {
        Some(Value::Number(n)) => n.as_f64().unwrap_or(f64::NAN),
        Some(Value::String(s)) => s.trim().parse().unwrap_or(f64::NAN),
        _ => f64::NAN,
    }
}

fn timeout_of(args: &Value) -> u64 {
    let n = number_of(args, "timeoutMs");
    if n > 0.0 {
        n.min(MAX_TIMEOUT_MS as f64) as u64
    } else {
        DEFAULT_TIMEOUT_MS
    }
}

fn wait_of(args: &Value) -> u64 {
    let n = number_of(args, "waitMs");
    if n > 0.0 {
        n.min(MAX_WAIT_MS as f64) as u64
    } else {
        0
    }
}

fn command_of(args: &Value) -> Option<&str> {
    text_arg(args, "command").filter(|c| !c.trim().is_empty())
}

fn job_name(args: &Value) -> Option<&str> {
    text_arg(args, "job").filter(|n| {
        (1..=64).contains(&n.len())
            && n.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    })
}

fn append(out: &mut Vec<u8>, chunk: &[u8]) {
    let room = OUTPUT_CAP.saturating_sub(out.len());
    out.extend_from_slice(&chunk[..chunk.len().min(room)]);
}

async fn read_chunk<R: AsyncRead + Unpin>(
    pipe: &mut Option<R>,
    buf: &mut [u8],
) -> io::Result<usize> {
    match pipe {
        Some(pipe) => pipe.read(buf).await,
        None => Ok(0),
    }
}

async fn capture(child: &mut Child, pid: u32, out: &mut Vec<u8>) -> io::Result<ExitStatus> {
    let mut stdout = child.stdout.take();
    let mut stderr = child.stderr.take();
    let mut status = None;
    let mut buf_out = [0u8; PIPE_CHUNK];
    let mut buf_err = [0u8; PIPE_CHUNK];
    loop {
        if let Some(done) = status {
            if stdout.is_none() && stderr.is_none() {
                return Ok(done);
            }
        }
        tokio::select! {
            read = read_chunk(&mut stdout, &mut buf_out), if stdout.is_some() => match read {
                Ok(n) if n > 0 => append(out, &buf_out[..n]),
                _ => stdout = None,
            },
            read = read_chunk(&mut stderr, &mut buf_err), if stderr.is_some() => match read {
                Ok(n) if n > 0 => append(out, &buf_err[..n]),
                _ => stderr = None,
            },
            done = child.wait(), if status.is_none() => {
                kill_group_now(pid);
                status = Some(done?);
            }
        }
    }
}

async fn run_foreground(command: &str, cwd: &Path, timeout_ms: u64) -> Value {
    let mut child = match shell(command, cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return json!({ "ok": false, "error": e.to_string() }),
    };
    let pid = child.id().unwrap_or(0);
    let _group = Group(pid);
    let mut out = Vec::new();
    match tokio::time::timeout(
        Duration::from_millis(timeout_ms),
        capture(&mut child, pid, &mut out),
    )
    .await
    {
        Ok(Ok(status)) => {
            let code = status.code();
            json!({ "ok": code == Some(0), "exitCode": code, "signal": signal_name(&status), "output": lossy(out) })
        }
        Ok(Err(e)) => json!({ "ok": false, "error": e.to_string(), "output": lossy(out) }),
        Err(_) => {
            kill_group(pid).await;
            json!({ "ok": false, "timedOut": true, "error": format!("timed out after {timeout_ms} ms"), "output": lossy(out) })
        }
    }
}

fn run_shell(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    Box::pin(async move {
        let Some(command) = command_of(&args) else {
            return Ok(error("command must be a non-empty string"));
        };
        let timeout_ms = timeout_of(&args);
        let _exclusive = ctx.locks.exclusive().await;
        Ok(run_foreground(command, &ctx.workspace, timeout_ms).await)
    })
}

fn job_start(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    Box::pin(async move {
        let Some(command) = command_of(&args) else {
            return Ok(error("command must be a non-empty string"));
        };
        let timeout_ms = timeout_of(&args);
        let Some(name) = job_name(&args) else {
            return Ok(error(
                "a background job needs job: 1 to 64 letters, digits, _ or -",
            ));
        };
        let _exclusive = ctx.locks.exclusive().await;
        Ok(ctx
            .jobs
            .start(ctx.run.clone(), name, command, &ctx.workspace, timeout_ms))
    })
}

fn job_status(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    Box::pin(async move {
        let Some(name) = job_name(&args) else {
            return Ok(error("job must be a job name"));
        };
        let wait_ms = wait_of(&args);
        let offset = match arg(&args, "offset") {
            None => None,
            Some(v) => match whole(v) {
                Some(offset) => Some(offset),
                None => {
                    return Ok(error(
                        "offset must be a whole number of characters, zero or more",
                    ))
                }
            },
        };
        if crate::runcode::is_background(name) {
            let status = crate::runcode::job_status(name, wait_ms, offset.map(|o| o as usize)).await;
            return Ok(status.unwrap_or_else(|| error(format!("no job named {name}"))));
        }
        if wait_ms > 0 {
            if let Some(job) = ctx.jobs.lookup(name) {
                if !job.is_settled() {
                    let mut settled = job.settled.subscribe();
                    let _ = tokio::time::timeout(
                        Duration::from_millis(wait_ms),
                        settled.wait_for(|done| *done),
                    )
                    .await;
                }
            }
        }
        let _shared = ctx.locks.shared().await;
        let Some(status) = ctx.jobs.status_of(name) else {
            return Ok(error(format!("no job named {name}")));
        };
        Ok(job_reply(status, offset))
    })
}

fn job_reply(status: Value, offset: Option<u64>) -> Value {
    let Value::Object(mut meta) = status else {
        return error("job status is not an object");
    };
    let log_path = meta.remove("output").unwrap_or(Value::Null);
    let path = log_path.as_str().map(PathBuf::from);
    let total = match path.as_deref() {
        None => 0,
        Some(p) => match char_total(p) {
            Ok(total) => total,
            Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
            Err(e) => return error(e.to_string()),
        },
    };
    let from = offset.unwrap_or(total.saturating_sub(OUTPUT_PAGE));
    let end = total.min(from.saturating_add(OUTPUT_PAGE));
    let text = match path.as_deref() {
        Some(p) if from < end => match chars_window(p, from, end - from) {
            Ok(text) => text,
            Err(e) => return error(e.to_string()),
        },
        _ => String::new(),
    };
    meta.entry("exitCode").or_insert(Value::Null);
    meta.entry("signal").or_insert(Value::Null);
    meta.insert("ok".into(), json!(true));
    meta.insert("output".into(), json!(text));
    meta.insert("total".into(), json!(total));
    meta.insert("truncated".into(), json!(from > 0 || end < total));
    meta.insert("offset".into(), json!(from));
    meta.insert("next".into(), json!((end < total).then_some(end)));
    meta.insert("logPath".into(), log_path);
    Value::Object(meta)
}

fn job_stop(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    Box::pin(async move {
        let Some(name) = job_name(&args) else {
            return Ok(error("job must be a job name"));
        };
        if crate::runcode::is_background(name) {
            let stopped = crate::runcode::job_stop(&ctx, name, "cancelled");
            return Ok(stopped.unwrap_or_else(|| error(format!("no job named {name}"))));
        }
        let _shared = ctx.locks.shared().await;
        Ok(ctx.jobs.stop(name).await)
    })
}
