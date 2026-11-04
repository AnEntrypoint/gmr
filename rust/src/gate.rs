use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::git::{run_command, GIT};

const LS_TIMEOUT: Duration = Duration::from_secs(60);
const CARGO_TIMEOUT: Duration = Duration::from_secs(600);
const BINARY_PROBE: usize = 8000;
const RUNTIME_DIRS: [&str; 4] = ["runs", "node_modules", ".gm", ".agentplug-kv"];
const TAIL_LINES: usize = 12;
const NOT_COVERED: &[&str] = &[
    "semantic correctness of the change: the gate compiles rust/ and reads the docs paths, and does not judge what the change means",
    "behaviour the program does not exercise: the gate runs cargo check, not the program, and has no smoke run",
    "doc content beyond path existence: the docs check finds the repo paths in DESIGN.md, not whether a claim in the docs is true",
    "docs paths in AGENTS.md and README.md: only DESIGN.md is checked",
    "CI: none is configured, and no CI result is read",
];

pub struct GateReport {
    pub failed: Vec<String>,
    pub details: Value,
}

struct Check {
    name: &'static str,
    ok: bool,
    dur_ms: u64,
    detail: String,
}

impl Check {
    fn to_value(&self) -> Value {
        json!({ "name": self.name, "ok": self.ok, "durMs": self.dur_ms, "detail": self.detail })
    }
}

pub fn run_gate_for(root: &Path, scope: Option<&[String]>) -> Result<GateReport, String> {
    let tracked = tracked_files(root)?;
    let mut checks = vec![timed("ascii", || ascii_check(root, &tracked, scope))];
    let mut not_covered: Vec<String> = NOT_COVERED.iter().map(|line| line.to_string()).collect();
    if root.join("DESIGN.md").is_file() {
        checks.push(timed("docs", || docs_check(root, &tracked)));
    } else {
        not_covered.push(format!("docs: no DESIGN.md in {}, so the docs check did not run", root.display()));
    }
    if root.join("skills").join("gm").join("SKILL.md").is_file() {
        checks.push(timed("skills", || skills_check(root)));
        checks.push(timed("wiki", || wiki_check(root)));
    } else {
        not_covered.push(format!("skills, wiki: no skills/gm/SKILL.md in {}, so it is not a gm-new checkout and these checks did not run", root.display()));
    }
    if rust_in_scope(scope) {
        checks.push(timed("cargo", || cargo_check(root)));
    } else {
        not_covered.push("cargo: no rust/ path in this commit, so cargo check of rust/ did not run".into());
    }
    let failed: Vec<String> = checks.iter().filter(|check| !check.ok).map(|check| check.name.to_string()).collect();
    let details = json!({ "checks": checks.iter().map(Check::to_value).collect::<Vec<Value>>(), "notCovered": not_covered });
    Ok(GateReport { failed, details })
}

fn rule_bodies() -> Vec<String> {
    crate::run::MCP_RULES
        .lines()
        .filter_map(|line| {
            let (number, body) = line.split_once(". ")?;
            (!number.is_empty() && number.bytes().all(|b| b.is_ascii_digit())).then(|| body.to_string())
        })
        .collect()
}

fn skills_check(root: &Path) -> Result<String, String> {
    match crate::skills::check(root, &rule_bodies()) {
        (true, detail) => Ok(detail),
        (false, detail) => Err(detail),
    }
}

fn wiki_check(root: &Path) -> Result<String, String> {
    let report = crate::search::wiki_check(root);
    let detail = report["detail"].as_str().unwrap_or_default().to_string();
    if report["ok"] == json!(true) {
        Ok(detail)
    } else {
        Err(detail)
    }
}

fn timed(name: &'static str, check: impl FnOnce() -> Result<String, String>) -> Check {
    let started = Instant::now();
    let (ok, detail) = match check() {
        Ok(detail) => (true, detail),
        Err(detail) => (false, detail),
    };
    Check { name, ok, dur_ms: started.elapsed().as_millis() as u64, detail }
}

fn tracked_files(root: &Path) -> Result<Vec<String>, String> {
    let listing = run_command(GIT, &["ls-files", "-z"], root, LS_TIMEOUT);
    if !listing.ok {
        return Err(listing.error.unwrap_or(listing.output));
    }
    Ok(listing.output.split('\0').filter(|file| !file.is_empty()).map(String::from).collect())
}

fn in_scope(file: &str, scope: Option<&[String]>) -> bool {
    match scope {
        None => true,
        Some(paths) => paths.iter().any(|path| file == path.as_str() || file.strip_prefix(path.as_str()).is_some_and(|rest| rest.starts_with('/'))),
    }
}

fn rust_in_scope(scope: Option<&[String]>) -> bool {
    match scope {
        None => true,
        Some(paths) => paths.iter().any(|path| path == "rust" || path.starts_with("rust/")),
    }
}

fn ascii_check(root: &Path, tracked: &[String], scope: Option<&[String]>) -> Result<String, String> {
    let mut bad: Vec<&str> = Vec::new();
    let mut count = 0usize;
    for rel in tracked.iter().filter(|file| in_scope(file, scope)) {
        let path = root.join(rel);
        let Ok(meta) = fs::symlink_metadata(&path) else { continue };
        if !meta.is_file() {
            continue;
        }
        let bytes = fs::read(&path).map_err(|err| format!("{rel}: {err}"))?;
        if bytes[..bytes.len().min(BINARY_PROBE)].contains(&0) {
            continue;
        }
        count += 1;
        if bytes.iter().any(|byte| *byte >= 0x80) {
            bad.push(rel);
        }
    }
    if bad.is_empty() {
        Ok(format!("{count} text files are ASCII"))
    } else {
        Err(format!("non-ASCII in {}", bad.join(", ")))
    }
}

fn docs_check(root: &Path, tracked: &[String]) -> Result<String, String> {
    let text = fs::read_to_string(root.join("DESIGN.md")).map_err(|err| err.to_string())?;
    let indexed: HashSet<&str> = tracked.iter().map(String::as_str).collect();
    let tops: HashSet<&str> = tracked.iter().filter_map(|file| file.split('/').next()).collect();
    let mut checked = 0usize;
    let mut missing: Vec<&str> = Vec::new();
    for (index, segment) in text.split('`').enumerate() {
        if index % 2 == 0 || segment.is_empty() || segment.chars().any(|c| c.is_whitespace()) {
            continue;
        }
        let token = strip_line_suffix(segment);
        if !is_repo_path(token, &tops) {
            continue;
        }
        checked += 1;
        let exists = if token.ends_with('/') { tracked.iter().any(|file| file.starts_with(token)) } else { indexed.contains(token) };
        if !exists {
            missing.push(token);
        }
    }
    if missing.is_empty() {
        Ok(format!("{checked} paths in DESIGN.md exist"))
    } else {
        let listed: Vec<String> = missing.iter().map(|token| format!("DESIGN.md: {token}")).collect();
        Err(format!("missing: {}", listed.join(" | ")))
    }
}

fn strip_line_suffix(token: &str) -> &str {
    let mut end = token;
    for _ in 0..2 {
        match end.rfind(':') {
            Some(index) if index + 1 < end.len() && end[index + 1..].bytes().all(|b| b.is_ascii_digit()) => end = &end[..index],
            _ => break,
        }
    }
    end
}

fn is_repo_path(token: &str, tops: &HashSet<&str>) -> bool {
    if token.contains(['<', '*', '=']) || token.contains("://") || token.starts_with('/') {
        return false;
    }
    let runtime = RUNTIME_DIRS.iter().any(|dir| token.strip_prefix(dir).is_some_and(|rest| rest.is_empty() || rest.starts_with('/')));
    if runtime {
        return false;
    }
    token.contains('/') && tops.contains(token.split('/').next().unwrap_or(""))
}

fn cargo_check(root: &Path) -> Result<String, String> {
    let crate_dir = root.join("rust");
    if !crate_dir.join("Cargo.toml").is_file() {
        return Err("rust/Cargo.toml is missing".into());
    }
    let run = run_command("cargo", &["check", "--color", "never"], &crate_dir, CARGO_TIMEOUT);
    if run.ok {
        return Ok("cargo check of rust/ passes".into());
    }
    let text = run.error.unwrap_or(run.output);
    let lines: Vec<&str> = text.lines().filter(|line| !line.trim().is_empty()).collect();
    let tail = &lines[lines.len().saturating_sub(TAIL_LINES)..];
    Err(format!("cargo check of rust/ failed: {}", tail.join(" | ")))
}
