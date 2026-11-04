use std::path::Path;
use std::time::Duration;

pub const EVERY: Duration = Duration::from_secs(60);
pub const FIRST: Duration = Duration::from_secs(30);

fn log(message: &str) {
    eprintln!("gmnew update: {message}");
}

pub fn start(exe: &Path) {
    if std::env::var("GMNEW_UPDATE").is_ok_and(|v| v == "off") {
        return;
    }
    #[cfg(feature = "jit")]
    {
        if let Some(checkout) = source::checkout_of(exe) {
            tokio::spawn(source::run(checkout));
        }
    }
    #[cfg(not(feature = "jit"))]
    {
        if let Some(asset) = release::ASSET {
            tokio::spawn(release::run(exe.to_path_buf(), asset));
        }
    }
}

#[cfg(feature = "jit")]
pub use source::stop_children;

#[cfg(not(feature = "jit"))]
pub fn stop_children() {}

#[cfg(feature = "jit")]
mod source {
    use std::path::{Path, PathBuf};
    use std::process::Stdio;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use tokio::io::AsyncReadExt;
    use tokio::process::Command;

    use super::{log, EVERY, FIRST};
    use crate::relay::{describe_exit, own_group, signal_group};

    const SHORT: Duration = Duration::from_secs(10);
    const LONG: Duration = Duration::from_secs(60);
    const LOCK_STALE: Duration = Duration::from_secs(1800);
    const TAIL_KEEP: usize = 4000;

    static BUILDING: AtomicBool = AtomicBool::new(false);
    static BUILD_PID: AtomicU32 = AtomicU32::new(0);

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
}

#[cfg(not(feature = "jit"))]
mod release {
    use std::fs;
    use std::io::{self, Read, Write};
    use std::path::{Path, PathBuf};
    use std::sync::OnceLock;
    use std::time::Duration;

    use sha2::{Digest, Sha256};

    use super::{log, EVERY, FIRST};

    const RELEASE_BASE: &str = "https://github.com/AnEntrypoint/gmr/releases/download/latest";
    const CHECK_TIMEOUT: Duration = Duration::from_secs(20);
    const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(300);
    const BODY_CAP: u64 = 256 << 20;
    const HASH_CHUNK: usize = 1 << 16;
    const USER_AGENT: &str = "gm-new";

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub const ASSET: Option<&str> = Some("gmnew-x86_64-unknown-linux-gnu");
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    pub const ASSET: Option<&str> = Some("gmnew-aarch64-unknown-linux-gnu");
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    pub const ASSET: Option<&str> = Some("gmnew-x86_64-apple-darwin");
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    pub const ASSET: Option<&str> = Some("gmnew-aarch64-apple-darwin");
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    pub const ASSET: Option<&str> = Some("gmnew-x86_64-pc-windows-msvc.exe");
    #[cfg(not(any(
        all(target_os = "linux", target_arch = "x86_64"),
        all(target_os = "linux", target_arch = "aarch64"),
        all(target_os = "macos", target_arch = "x86_64"),
        all(target_os = "macos", target_arch = "aarch64"),
        all(target_os = "windows", target_arch = "x86_64"),
    )))]
    pub const ASSET: Option<&str> = None;

    fn agent() -> &'static ureq::Agent {
        static AGENT: OnceLock<ureq::Agent> = OnceLock::new();
        AGENT.get_or_init(|| ureq::Agent::new_with_config(ureq::Agent::config_builder().http_status_as_error(false).build()))
    }

    fn open(url: &str, timeout: Duration) -> Result<Box<dyn Read>, String> {
        if let Some(path) = url.strip_prefix("file://") {
            let file = fs::File::open(path).map_err(|e| format!("{path}: {e}"))?;
            return Ok(Box::new(file));
        }
        let request = ureq::http::Request::builder()
            .method("GET")
            .uri(url)
            .header("user-agent", USER_AGENT)
            .body(String::new())
            .map_err(|e| format!("{url}: {e}"))?;
        let request = agent().configure_request(request).timeout_global(Some(timeout)).build();
        let response = agent().run(request).map_err(|e| format!("{url}: {e}"))?;
        let status = response.status().as_u16();
        if status != 200 {
            return Err(format!("{url}: HTTP {status}"));
        }
        Ok(Box::new(response.into_body().into_with_config().limit(BODY_CAP).reader()))
    }

    fn read_text(url: &str, timeout: Duration) -> Result<String, String> {
        let mut source = open(url, timeout)?;
        let mut bytes = Vec::new();
        source.read_to_end(&mut bytes).map_err(|e| format!("{url}: {e}"))?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn copy_hashed(source: &mut dyn Read, sink: &mut dyn Write) -> io::Result<String> {
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; HASH_CHUNK];
        loop {
            let n = source.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            sink.write_all(&buf[..n])?;
        }
        Ok(hex(&hasher.finalize()))
    }

    fn hash_file(path: &Path) -> Result<String, String> {
        let mut file = fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
        copy_hashed(&mut file, &mut io::sink()).map_err(|e| format!("{}: {e}", path.display()))
    }

    fn expected_hex(sums: &str, asset: &str) -> Result<String, String> {
        for line in sums.lines() {
            let mut parts = line.split_whitespace();
            let (Some(digest), Some(name)) = (parts.next(), parts.next()) else { continue };
            if name.trim_start_matches('*') == asset && digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Ok(digest.to_ascii_lowercase());
            }
        }
        Err(format!("the release has no sha256 for {asset}"))
    }

    fn download(url: &str, staged: &Path) -> Result<String, String> {
        let mut source = open(url, DOWNLOAD_TIMEOUT)?;
        let mut file = fs::File::create(staged).map_err(|e| format!("{}: {e}", staged.display()))?;
        let digest = copy_hashed(&mut source, &mut file).map_err(|e| format!("{url}: {e}"))?;
        file.sync_all().map_err(|e| format!("{}: {e}", staged.display()))?;
        Ok(digest)
    }

    fn staged_for(binary: &Path) -> PathBuf {
        let name = binary.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|| "gmnew".to_string());
        binary.with_file_name(format!(".{name}.download"))
    }

    #[cfg(unix)]
    fn swap_in(binary: &Path, staged: &Path) -> Result<(), String> {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(staged, fs::Permissions::from_mode(0o755)).map_err(|e| format!("{}: {e}", staged.display()))?;
        fs::rename(staged, binary).map_err(|e| format!("{}: {e}", binary.display()))
    }

    #[cfg(windows)]
    fn swap_in(binary: &Path, staged: &Path) -> Result<(), String> {
        let aside = binary.with_extension("old");
        let _ = fs::remove_file(&aside);
        fs::rename(binary, &aside).map_err(|e| format!("{}: {e}", binary.display()))?;
        fs::rename(staged, binary).map_err(|e| format!("{}: {e}", binary.display()))
    }

    fn release_once(binary: &Path, asset: &str, base: &str) -> Result<Option<String>, String> {
        let sums = read_text(&format!("{base}/{asset}.sha256"), CHECK_TIMEOUT)?;
        let expected = expected_hex(&sums, asset)?;
        if hash_file(binary)? == expected {
            return Ok(None);
        }
        let staged = staged_for(binary);
        let got = match download(&format!("{base}/{asset}"), &staged) {
            Ok(digest) => digest,
            Err(e) => {
                let _ = fs::remove_file(&staged);
                return Err(e);
            }
        };
        if got != expected {
            let _ = fs::remove_file(&staged);
            return Err(format!("the download of {asset} has sha256 {got}, the release says {expected}; the old binary stays"));
        }
        if let Err(e) = swap_in(binary, &staged) {
            let _ = fs::remove_file(&staged);
            return Err(e);
        }
        Ok(Some(expected))
    }

    fn release_base() -> String {
        std::env::var("GMNEW_UPDATE_BASE").unwrap_or_else(|_| RELEASE_BASE.to_string())
    }

    fn report(last: &mut Option<String>, message: String) {
        if last.as_deref() != Some(message.as_str()) {
            log(&format!("release check failed: {message}"));
            *last = Some(message);
        }
    }

    pub async fn run(binary: PathBuf, asset: &'static str) {
        tokio::time::sleep(FIRST).await;
        let mut tick = tokio::time::interval(EVERY);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_failure: Option<String> = None;
        loop {
            tick.tick().await;
            let target = binary.clone();
            let base = release_base();
            match tokio::task::spawn_blocking(move || release_once(&target, asset, &base)).await {
                Ok(Ok(Some(digest))) => {
                    log(&format!("updated {asset} to sha256 {}; the relay swaps when no call is in flight", &digest[..digest.len().min(12)]));
                    last_failure = None;
                }
                Ok(Ok(None)) => last_failure = None,
                Ok(Err(e)) => report(&mut last_failure, e),
                Err(e) => report(&mut last_failure, e.to_string()),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn fixture(name: &str) -> PathBuf {
            let dir = std::env::temp_dir().join(format!("gmnew-update-{name}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).expect("fixture dir");
            dir
        }

        fn base_of(dir: &Path) -> String {
            format!("file://{}", dir.display())
        }

        fn publish(dir: &Path, asset: &str, bytes: &[u8], sums_digest: &str) {
            fs::write(dir.join(asset), bytes).expect("asset");
            fs::write(dir.join(format!("{asset}.sha256")), format!("{sums_digest}  {asset}\n")).expect("sums");
        }

        fn digest_of(bytes: &[u8]) -> String {
            hex(&Sha256::digest(bytes))
        }

        #[test]
        fn accepts_a_verified_download_and_swaps_it_in() {
            let dir = fixture("accept");
            let binary = dir.join("gmnew");
            fs::write(&binary, b"old build").expect("old binary");
            let new_bytes = b"new build bytes";
            publish(&dir, "gmnew-test", new_bytes, &digest_of(new_bytes));
            let outcome = release_once(&binary, "gmnew-test", &base_of(&dir));
            assert_eq!(outcome, Ok(Some(digest_of(new_bytes))));
            assert_eq!(fs::read(&binary).expect("swapped"), new_bytes);
            assert!(!staged_for(&binary).exists());
            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn rejects_a_download_whose_digest_does_not_match_and_keeps_the_old_binary() {
            let dir = fixture("reject");
            let binary = dir.join("gmnew");
            fs::write(&binary, b"old build").expect("old binary");
            let wrong = digest_of(b"something else");
            publish(&dir, "gmnew-test", b"tampered bytes", &wrong);
            let outcome = release_once(&binary, "gmnew-test", &base_of(&dir));
            assert!(outcome.is_err(), "expected a rejection, got {outcome:?}");
            assert_eq!(fs::read(&binary).expect("kept"), b"old build");
            assert!(!staged_for(&binary).exists());
            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn leaves_a_current_binary_alone() {
            let dir = fixture("current");
            let binary = dir.join("gmnew");
            let bytes = b"current build";
            fs::write(&binary, bytes).expect("binary");
            fs::write(dir.join("gmnew-test.sha256"), format!("{}  gmnew-test\n", digest_of(bytes))).expect("sums");
            assert_eq!(release_once(&binary, "gmnew-test", &base_of(&dir)), Ok(None));
            assert_eq!(fs::read(&binary).expect("kept"), bytes);
            let _ = fs::remove_dir_all(&dir);
        }

        #[test]
        fn reports_a_missing_release_without_touching_the_binary() {
            let dir = fixture("missing");
            let binary = dir.join("gmnew");
            fs::write(&binary, b"old build").expect("old binary");
            let outcome = release_once(&binary, "gmnew-test", &base_of(&dir));
            assert!(outcome.is_err());
            assert_eq!(fs::read(&binary).expect("kept"), b"old build");
            let _ = fs::remove_dir_all(&dir);
        }
    }
}
