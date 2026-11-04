use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::AsyncReadExt;
use tokio::process::Command;

use crate::relay::{describe_exit, own_group, signal_group};

pub const EVERY: Duration = Duration::from_secs(60);
pub const FIRST: Duration = Duration::from_secs(30);
const SHORT: Duration = Duration::from_secs(10);
const LONG: Duration = Duration::from_secs(60);
const LOCK_STALE: Duration = Duration::from_secs(1800);
const TAIL_KEEP: usize = 4000;

static BUILDING: AtomicBool = AtomicBool::new(false);
static BUILD_PID: AtomicU32 = AtomicU32::new(0);

fn log(message: &str) {
    eprintln!("gmnew update: {message}");
}

fn short(sha: &str) -> &str {
    &sha[..sha.len().min(7)]
}

fn first_line(bytes: &[u8]) -> Option<String> {
    String::from_utf8_lossy(bytes).lines().map(str::trim).find(|l| !l.is_empty()).map(str::to_string)
}

pub fn checkout_of(exe: &Path) -> Option<PathBuf> {
    if let Some(root) = std::env::var_os("GMNEW_ROOT") {
        return Some(PathBuf::from(root));
    }
    exe.ancestors().skip(1).find(|dir| dir.join("rust").join("Cargo.toml").is_file() && dir.join(".git").exists()).map(Path::to_path_buf)
}

pub fn target_dir(checkout: &Path) -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|exe| {
            let release = exe.parent()?;
            (release.file_name()? == "release").then(|| release.parent().map(Path::to_path_buf)).flatten()
        })
        .unwrap_or_else(|| checkout.join("rust").join("target"))
}

pub fn binary_path(checkout: &Path) -> PathBuf {
    target_dir(checkout).join("release").join(format!("gmnew{}", std::env::consts::EXE_SUFFIX))
}

pub fn stop_children() {
    let pid = BUILD_PID.swap(0, Ordering::SeqCst);
    signal_group(pid, true);
}

async fn git(root: &Path, args: &[&str], limit: Duration) -> Result<String, String> {
    let mut cmd = Command::new("git");
    cmd.args(args).current_dir(root).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
    let out = match tokio::time::timeout(limit, cmd.output()).await {
        Err(_) => return Err(format!("git {} timed out after {} ms", args[0], limit.as_millis())),
        Ok(Err(e)) => return Err(format!("git {}: {e}", args[0])),
        Ok(Ok(out)) => out,
    };
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Err(first_line(&out.stderr).unwrap_or_else(|| format!("git {} exited ({})", args[0], describe_exit(Some(out.status)))))
    }
}

fn take_lock(path: &Path) -> bool {
    for _ in 0..2 {
        match std::fs::OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(mut file) => {
                use std::io::Write;
                let _ = write!(file, "{}", std::process::id());
                return true;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let stale = std::fs::metadata(path)
                    .and_then(|m| m.modified())
                    .ok()
                    .and_then(|t| t.elapsed().ok())
                    .is_some_and(|age| age > LOCK_STALE);
                if !stale {
                    return false;
                }
                let _ = std::fs::remove_file(path);
            }
            Err(_) => return false,
        }
    }
    false
}

struct Updater {
    root: PathBuf,
    target: PathBuf,
    attempted: Option<String>,
    last_note: Option<String>,
}

impl Updater {
    fn note(&mut self, message: String) {
        if self.last_note.as_deref() != Some(message.as_str()) {
            log(&message);
            self.last_note = Some(message);
        }
    }

    async fn dirty(&self) -> Result<bool, String> {
        git(&self.root, &["status", "--porcelain", "--untracked-files=no"], SHORT).await.map(|s| !s.is_empty())
    }

    async fn check(&mut self) {
        if BUILDING.load(Ordering::SeqCst) {
            return;
        }
        if git(&self.root, &["remote", "get-url", "origin"], SHORT).await.is_err() {
            self.note("the checkout has no origin, so there is nothing to pull".to_string());
            return;
        }
        if let Err(e) = git(&self.root, &["fetch", "--quiet", "origin", "main"], LONG).await {
            self.note(format!("origin could not be reached, so the check was skipped ({e})"));
            return;
        }
        let (head, target) = match (
            git(&self.root, &["rev-parse", "HEAD"], SHORT).await,
            git(&self.root, &["rev-parse", "origin/main"], SHORT).await,
        ) {
            (Ok(h), Ok(t)) => (h, t),
            (Err(e), _) | (_, Err(e)) => {
                self.note(format!("the check failed: {e}"));
                return;
            }
        };
        if head != target {
            let range = format!("{head}..{target}");
            let ancestor = git(&self.root, &["merge-base", "--is-ancestor", &head, &target], SHORT).await.is_ok();
            if !ancestor {
                self.note("the checkout and origin main have diverged, so nothing was pulled".to_string());
                return;
            }
            let ahead = git(&self.root, &["rev-list", "--count", &range], SHORT).await.unwrap_or_else(|_| "?".to_string());
            match self.dirty().await {
                Ok(false) => {}
                Ok(true) => {
                    self.note(format!("origin main is {ahead} commit(s) ahead, but the checkout has changes to tracked files, so nothing was pulled"));
                    return;
                }
                Err(e) => {
                    self.note(format!("the check failed: {e}"));
                    return;
                }
            }
            if let Err(e) = git(&self.root, &["merge", "--ff-only", "--quiet", &target], LONG).await {
                self.note(format!("the pull failed ({e})"));
                return;
            }
            log(&format!("pulled {ahead} commit(s) to {}", short(&target)));
        }
        self.last_note = None;
        self.maybe_build().await;
    }

    async fn maybe_build(&mut self) {
        let last = match git(&self.root, &["log", "-1", "--format=%H %ct", "--", "rust"], SHORT).await {
            Ok(out) if !out.is_empty() => out,
            _ => return,
        };
        let mut parts = last.split_whitespace();
        let sha = parts.next().unwrap_or_default().to_string();
        let committed: u64 = parts.next().and_then(|t| t.parse().ok()).unwrap_or(0);
        if self.attempted.as_deref() == Some(sha.as_str()) {
            return;
        }
        if self.attempted.is_none() {
            let built = std::fs::metadata(binary_path(&self.root))
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map_or(0, |d| d.as_secs());
            if built >= committed {
                self.attempted = Some(sha);
                return;
            }
        }
        if !matches!(self.dirty().await, Ok(false)) {
            self.note(format!("rust/ changed at {}, but the checkout has changes to tracked files, so it was not built", short(&sha)));
            return;
        }
        if std::fs::create_dir_all(&self.target).is_err() {
            return;
        }
        let lock = self.target.join(".gmnew-build.lock");
        if !take_lock(&lock) {
            return;
        }
        self.attempted = Some(sha.clone());
        BUILDING.store(true, Ordering::SeqCst);
        tokio::spawn(build(self.root.join("rust"), self.target.clone(), sha, lock));
    }
}

async fn build(rust_dir: PathBuf, target: PathBuf, sha: String, lock: PathBuf) {
    log(&format!("building {} (cargo build --release)", short(&sha)));
    let started = SystemTime::now();
    let mut cmd = Command::new("cargo");
    cmd.args(["build", "--release"])
        .current_dir(&rust_dir)
        .env("CARGO_TARGET_DIR", &target)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    own_group(&mut cmd);
    match cmd.spawn() {
        Err(e) => log(&format!("the build of {} could not start ({e}); the old binary stays", short(&sha))),
        Ok(mut child) => {
            BUILD_PID.store(child.id().unwrap_or(0), Ordering::SeqCst);
            let mut tail: Vec<u8> = Vec::new();
            if let Some(mut stderr) = child.stderr.take() {
                let mut buf = vec![0u8; 4096];
                while let Ok(n) = stderr.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                    tail.extend_from_slice(&buf[..n]);
                    if tail.len() > TAIL_KEEP * 2 {
                        tail.drain(..tail.len() - TAIL_KEEP);
                    }
                }
            }
            let status = child.wait().await.ok();
            BUILD_PID.store(0, Ordering::SeqCst);
            let secs = started.elapsed().map_or(0, |d| d.as_secs());
            match status {
                Some(s) if s.success() => log(&format!("built {} in {secs} s; the relay swaps to the new binary when no call is in flight", short(&sha))),
                other => {
                    let text = String::from_utf8_lossy(&tail[tail.len().saturating_sub(TAIL_KEEP)..]).into_owned();
                    log(&format!("the build of {} failed ({}); the old binary stays:\n{}", short(&sha), describe_exit(other), text.trim_end()));
                }
            }
        }
    }
    let _ = std::fs::remove_file(&lock);
    BUILDING.store(false, Ordering::SeqCst);
}

pub async fn run(checkout: PathBuf) {
    let mut updater = Updater { target: target_dir(&checkout), root: checkout, attempted: None, last_note: None };
    tokio::time::sleep(FIRST).await;
    let mut tick = tokio::time::interval(EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        updater.check().await;
    }
}
