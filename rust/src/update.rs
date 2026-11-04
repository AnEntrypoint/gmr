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
    if let Some(asset) = release::ASSET {
        tokio::spawn(release::run(exe.to_path_buf(), asset));
    }
}

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
