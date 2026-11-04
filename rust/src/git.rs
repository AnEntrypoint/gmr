use std::collections::hash_map::RandomState;
use std::ffi::OsStr;
use std::hash::{BuildHasher, Hasher};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use serde_json::{json, Value};

use crate::gate;
use crate::retry::is_transient;
use crate::run::Run;
use crate::types::{Ctx, Fut, VerbDef, VerbOutcome};

pub const GIT: &str = "git";
pub const GIT_TIMEOUT: Duration = Duration::from_secs(120);
pub const CHECK_TIMEOUT: Duration = Duration::from_secs(15);
pub const GH_TIMEOUT: Duration = Duration::from_secs(60);
const CAPTURE_CAP: usize = 16 * 1024 * 1024;
const OUTPUT_CAP: usize = 200_000;
const REF_ERROR: &str = "ref must be a branch, tag or commit name";
const PATHS_ERROR: &str = "paths must be a non-empty array of strings";
const DOT_ERROR: &str = r#"refused: "." names the whole workspace; name each path explicitly"#;
const RETRIES: u64 = 3;
const BASE_MS: u64 = 1000;
const CAP_MS: u64 = 16_000;

pub struct Cmd {
    pub ok: bool,
    pub exit_code: Option<i32>,
    pub output: String,
    pub error: Option<String>,
}

impl Cmd {
    fn failed(error: String) -> Cmd {
        Cmd { ok: false, exit_code: None, output: String::new(), error: Some(error) }
    }
}

pub struct Scope {
    pub root: PathBuf,
    pub dir: PathBuf,
}

pub fn fail(error: impl Into<String>) -> Value {
    json!({ "ok": false, "error": error.into() })
}

pub fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

pub fn root_of(ctx: &Ctx) -> PathBuf {
    normalize(&ctx.workspace)
}

pub fn confine(root: &Path, rel: &str) -> Result<PathBuf, String> {
    if rel.is_empty() {
        return Err("path must be a non-empty string".into());
    }
    if rel.split(['\\', '/']).any(|segment| segment == "..") {
        return Err("path has a .. segment".into());
    }
    let target = normalize(&root.join(rel));
    if !target.starts_with(root) {
        return Err("path is outside the workspace".into());
    }
    let real_root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    let mut probe = target.as_path();
    while std::fs::symlink_metadata(probe).is_err() {
        match probe.parent() {
            Some(parent) => probe = parent,
            None => break,
        }
    }
    let real = std::fs::canonicalize(probe).map_err(|_| "path leads through a link that does not resolve".to_string())?;
    if !real.starts_with(&real_root) {
        return Err("path leads through a link outside the workspace".into());
    }
    Ok(target)
}

pub fn is_sha(text: &str) -> bool {
    (7..=40).contains(&text.len()) && text.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub fn is_repo(text: &str) -> bool {
    let part = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    text.split_once('/').is_some_and(|(owner, name)| part(owner) && part(name))
}

pub fn random_u64() -> u64 {
    RandomState::new().build_hasher().finish()
}

pub fn run_command<S: AsRef<OsStr>>(program: &str, args: &[S], dir: &Path, timeout: Duration) -> Cmd {
    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_LITERAL_PATHSPECS", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    spawn_group(&mut command);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => return Cmd::failed(err.to_string()),
    };
    let pid = child.id();
    let stdout_pipe = child.stdout.take();
    let stderr_pipe = child.stderr.take();
    let (done, watch) = mpsc::channel::<()>();
    let killer = thread::spawn(move || match watch.recv_timeout(timeout) {
        Err(RecvTimeoutError::Timeout) => {
            kill_tree(pid);
            true
        }
        _ => false,
    });
    let stderr_reader = thread::spawn(move || read_capped(stderr_pipe));
    let stdout = read_capped(stdout_pipe);
    let stderr = stderr_reader.join().unwrap_or_default();
    let status = child.wait();
    drop(done);
    if killer.join().unwrap_or(false) {
        return Cmd::failed(format!("timed out after {} ms", timeout.as_millis()));
    }
    let mut output = String::from_utf8_lossy(&stdout).into_owned();
    output.push_str(&String::from_utf8_lossy(&stderr));
    match status {
        Ok(status) => Cmd { ok: status.success(), exit_code: status.code(), output, error: None },
        Err(err) => Cmd::failed(err.to_string()),
    }
}

#[cfg(unix)]
fn spawn_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    command.process_group(0);
}

#[cfg(windows)]
fn spawn_group(command: &mut Command) {
    use std::os::windows::process::CommandExt;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    command.creation_flags(CREATE_NEW_PROCESS_GROUP);
}

#[cfg(unix)]
fn kill_tree(pid: u32) {
    if let Ok(group) = libc::pid_t::try_from(pid) {
        unsafe {
            libc::killpg(group, libc::SIGKILL);
        }
    }
}

#[cfg(windows)]
fn kill_tree(pid: u32) {
    let _ = Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn read_capped<R: Read>(reader: Option<R>) -> Vec<u8> {
    let Some(mut reader) = reader else { return Vec::new() };
    let mut kept = Vec::new();
    let mut chunk = [0u8; 8192];
    while let Ok(n) = reader.read(&mut chunk) {
        if n == 0 {
            break;
        }
        let room = CAPTURE_CAP.saturating_sub(kept.len()).min(n);
        kept.extend_from_slice(&chunk[..room]);
    }
    kept
}

fn capped(mut text: String) -> String {
    if text.len() > OUTPUT_CAP {
        let mut end = OUTPUT_CAP;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}

pub fn verb_result(sub: &str, cmd: Cmd) -> Value {
    let output = capped(cmd.output).trim().to_string();
    if cmd.ok {
        let output = if output.is_empty() { "(no output)".to_string() } else { output };
        return json!({ "ok": true, "output": output });
    }
    let error = cmd.error.unwrap_or_else(|| format!("git {sub} exited {}", exit_text(cmd.exit_code)));
    json!({ "ok": false, "error": error, "output": output })
}

pub fn raw_value(cmd: &Cmd) -> Value {
    json!({ "ok": cmd.ok, "error": cmd.error, "exitCode": cmd.exit_code, "output": cmd.output })
}

fn exit_text(code: Option<i32>) -> String {
    code.map_or_else(|| "null".to_string(), |c| c.to_string())
}

fn failure_text(result: &Value) -> String {
    format!("{}\n{}", result["error"].as_str().unwrap_or(""), result["output"].as_str().unwrap_or(""))
}

pub fn backoff_ms(attempt: u64) -> u64 {
    let base = (BASE_MS << (attempt - 1).min(10)).min(CAP_MS);
    let unit = (random_u64() >> 11) as f64 / (1u64 << 53) as f64;
    (base as f64 * (0.8 + 0.4 * unit)).round() as u64
}

pub fn retried(run: &Run, tool: &str, mut attempt: impl FnMut() -> Value) -> Value {
    let mut n: u64 = 1;
    loop {
        let mut result = attempt();
        let transient = result["ok"] != json!(true) && is_transient(&failure_text(&result));
        if !transient || n > RETRIES {
            result["attempts"] = json!(n);
            if transient {
                result["exhausted"] = json!(true);
            }
            return result;
        }
        let delay = backoff_ms(n);
        run.event("retry", json!({ "tool": tool, "attempt": n, "delayMs": delay }));
        thread::sleep(Duration::from_millis(delay));
        n += 1;
    }
}

pub fn repo_scope(ctx: &Ctx, args: &Value) -> Result<Scope, Value> {
    let root = root_of(ctx);
    let cwd = match args.get("cwd") {
        None => return Ok(Scope { dir: root.clone(), root }),
        Some(Value::String(text)) if !text.is_empty() => text,
        Some(_) => return Err(fail("cwd must be a non-empty path")),
    };
    let dir = normalize(&root.join(cwd));
    if !dir.is_dir() {
        return Err(fail(format!("no such directory: {cwd}")));
    }
    if run_command(GIT, &["rev-parse", "--show-toplevel"], &dir, CHECK_TIMEOUT).ok {
        Ok(Scope { root, dir })
    } else {
        Err(fail(format!("cwd {cwd} is not inside a git repository")))
    }
}

pub fn in_repo(ctx: &Ctx, args: &Value, run: impl FnOnce(&Scope) -> Value) -> Value {
    match repo_scope(ctx, args) {
        Ok(scope) => run(&scope),
        Err(error) => error,
    }
}

pub fn pathspecs(scope: &Scope, paths: &Value) -> Result<Vec<String>, String> {
    let list = match paths.as_array() {
        Some(list) if !list.is_empty() => list,
        _ => return Err(PATHS_ERROR.into()),
    };
    let mut rels = Vec::with_capacity(list.len());
    for item in list {
        let Some(text) = item.as_str() else { return Err(PATHS_ERROR.into()) };
        let at = if scope.dir == scope.root { confine(&scope.root, text)? } else { normalize(&scope.dir.join(text)) };
        let rel = at.strip_prefix(&scope.dir).map_err(|_| format!("path {text} is outside {}", scope.dir.display()))?;
        if rel.as_os_str().is_empty() {
            return Err(DOT_ERROR.into());
        }
        rels.push(rel.components().filter_map(|c| c.as_os_str().to_str()).collect::<Vec<_>>().join("/"));
    }
    Ok(rels)
}

fn spec(head: &[&str], rels: &[String]) -> Vec<String> {
    let mut args: Vec<String> = head.iter().map(|s| s.to_string()).collect();
    if !rels.is_empty() {
        args.push("--".into());
        args.extend(rels.iter().cloned());
    }
    args
}

fn ref_arg(args: &Value, default: Option<&str>) -> Result<String, Value> {
    let found = match (args.get("ref"), default) {
        (None, Some(fallback)) => fallback,
        (Some(Value::String(name)), _) => name.as_str(),
        _ => return Err(fail(REF_ERROR)),
    };
    let valid = !found.is_empty()
        && found.len() <= 200
        && !found.starts_with('-')
        && found.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'/' | b'@' | b'-'));
    if valid {
        Ok(found.to_string())
    } else {
        Err(fail(REF_ERROR))
    }
}

fn current_branch(dir: &Path) -> Option<String> {
    let head = run_command(GIT, &["rev-parse", "--abbrev-ref", "HEAD"], dir, CHECK_TIMEOUT);
    head.ok.then(|| head.output.trim().to_string())
}

fn status(ctx: &Ctx, args: &Value) -> Value {
    in_repo(ctx, args, |s| verb_result("status", run_command(GIT, &["status", "--porcelain=v1", "--branch"], &s.dir, GIT_TIMEOUT)))
}

fn add(ctx: &Ctx, args: &Value) -> Value {
    in_repo(ctx, args, |s| match pathspecs(s, args.get("paths").unwrap_or(&Value::Null)) {
        Ok(rels) => verb_result("add", run_command(GIT, &spec(&["add"], &rels), &s.dir, GIT_TIMEOUT)),
        Err(error) => fail(error),
    })
}

fn rm(ctx: &Ctx, args: &Value) -> Value {
    in_repo(ctx, args, |s| match pathspecs(s, args.get("paths").unwrap_or(&Value::Null)) {
        Ok(rels) => verb_result("rm", run_command(GIT, &spec(&["rm"], &rels), &s.dir, GIT_TIMEOUT)),
        Err(error) => fail(error),
    })
}

fn commit(ctx: &Ctx, args: &Value) -> Value {
    in_repo(ctx, args, |s| {
        let message = match args.get("message").and_then(Value::as_str) {
            Some(text) if !text.trim().is_empty() => text,
            _ => return fail("message must be a non-empty string"),
        };
        let rels = match pathspecs(s, args.get("paths").unwrap_or(&Value::Null)) {
            Ok(rels) => rels,
            Err(error) => return fail(error),
        };
        let report = match gate::run_gate_for(&s.dir, Some(rels.as_slice())) {
            Ok(report) => report,
            Err(error) => return fail(error),
        };
        let checks = report.details["checks"].clone();
        let not_covered = report.details["notCovered"].clone();
        if !report.failed.is_empty() {
            let reasons: Vec<String> = checks
                .as_array()
                .into_iter()
                .flatten()
                .filter(|check| check["ok"] != json!(true))
                .map(|check| format!("{}: {}", check["name"].as_str().unwrap_or(""), check["detail"].as_str().unwrap_or("")))
                .collect();
            return json!({ "ok": false, "error": format!("refused: the commit gate failed: {}", reasons.join("; ")), "gate": checks, "notCovered": not_covered });
        }
        let mut result = verb_result("commit", run_command(GIT, &spec(&["commit", "-m", message], &rels), &s.dir, GIT_TIMEOUT));
        if result["ok"] == json!(true) {
            let head = run_command(GIT, &["rev-parse", "--short", "HEAD"], &s.dir, CHECK_TIMEOUT);
            let sha = if head.ok { Value::String(head.output.trim().to_string()) } else { Value::Null };
            let subject = message.split('\n').next().unwrap_or("");
            ctx.run.event("commit", json!({ "sha": sha, "message": subject, "paths": rels, "gate": checks, "notCovered": not_covered }));
        }
        result["gate"] = checks;
        result["notCovered"] = not_covered;
        result
    })
}

fn push(ctx: &Ctx, args: &Value) -> Value {
    in_repo(ctx, args, |s| {
        let branch = current_branch(&s.dir);
        ctx.run.event("stage", json!({ "tool": "git_push", "stage": "branch", "branch": branch }));
        if branch.as_deref() != Some("main") {
            return fail(format!("refused: git_push sends only main, and the current branch is {}", branch.as_deref().unwrap_or("unknown")));
        }
        let result = retried(&ctx.run, "git_push", || {
            verb_result("push", run_command(GIT, &["push", "--set-upstream", "origin", "main"], &s.dir, GIT_TIMEOUT))
        });
        ctx.run.event("stage", json!({ "tool": "git_push", "stage": "push", "ok": result["ok"] }));
        result
    })
}

fn log(ctx: &Ctx, args: &Value) -> Value {
    in_repo(ctx, args, |s| {
        let count = match args.get("count") {
            None => 20,
            Some(value) => match value.as_f64() {
                Some(c) if c.fract() == 0.0 && (1.0..=200.0).contains(&c) => c as u64,
                _ => return fail("count must be an integer from 1 to 200"),
            },
        };
        let rels = match args.get("path") {
            None => Vec::new(),
            Some(path) => match pathspecs(s, &json!([path])) {
                Ok(rels) => rels,
                Err(error) => return fail(error),
            },
        };
        let n = count.to_string();
        verb_result("log", run_command(GIT, &spec(&["log", "--oneline", "--no-color", "-n", n.as_str()], &rels), &s.dir, GIT_TIMEOUT))
    })
}

fn diff(ctx: &Ctx, args: &Value) -> Value {
    in_repo(ctx, args, |s| {
        let rels = match args.get("paths") {
            None => Vec::new(),
            Some(paths) => match pathspecs(s, paths) {
                Ok(rels) => rels,
                Err(error) => return fail(error),
            },
        };
        let mut head = vec!["diff", "--no-color"];
        if args.get("staged").and_then(Value::as_bool) == Some(true) {
            head.push("--cached");
        }
        verb_result("diff", run_command(GIT, &spec(&head, &rels), &s.dir, GIT_TIMEOUT))
    })
}

fn show(ctx: &Ctx, args: &Value) -> Value {
    in_repo(ctx, args, |s| match ref_arg(args, Some("HEAD")) {
        Ok(name) => verb_result("show", run_command(GIT, &["show", "--no-color", "--stat", name.as_str()], &s.dir, GIT_TIMEOUT)),
        Err(error) => error,
    })
}

fn fetch(ctx: &Ctx, args: &Value) -> Value {
    in_repo(ctx, args, |s| {
        retried(&ctx.run, "git_fetch", || verb_result("fetch", run_command(GIT, &["fetch", "origin"], &s.dir, GIT_TIMEOUT)))
    })
}

fn pull(ctx: &Ctx, args: &Value) -> Value {
    in_repo(ctx, args, |s| verb_result("pull", run_command(GIT, &["pull", "--ff-only"], &s.dir, GIT_TIMEOUT)))
}

fn checkout(ctx: &Ctx, args: &Value) -> Value {
    in_repo(ctx, args, |s| match ref_arg(args, None) {
        Ok(name) => verb_result("checkout", run_command(GIT, &["checkout", name.as_str()], &s.dir, GIT_TIMEOUT)),
        Err(error) => error,
    })
}

fn branch(ctx: &Ctx, args: &Value) -> Value {
    in_repo(ctx, args, |s| verb_result("branch", run_command(GIT, &["branch", "--no-color", "--list"], &s.dir, GIT_TIMEOUT)))
}

pub fn blocking(ctx: Arc<Ctx>, args: Value, run: fn(&Ctx, &Value) -> Value) -> Fut<VerbOutcome> {
    Box::pin(async move { tokio::task::spawn_blocking(move || run(&ctx, &args)).await.map_err(|err| err.to_string()) })
}

pub const VERBS: &[VerbDef] = &[
    VerbDef {
        name: "git_status",
        description: "Working tree status: the branch and the changed paths (porcelain v1). cwd, when given, is the directory whose repository the verb acts on: absolute, or relative to the workspace root; a directory outside a git repository is refused. Omitted, the workspace is used.",
        schema: r#"{"type":"object","properties":{"cwd":{"type":"string"}},"required":[]}"#,
        call: |ctx, args| blocking(ctx, args, status),
    },
    VerbDef {
        name: "git_add",
        description: "Stage the given workspace paths. Each path is explicit; \".\" is refused. cwd, when given, is the directory whose repository the verb acts on: absolute, or relative to the workspace root; a directory outside a git repository is refused. Omitted, the workspace is used.",
        schema: r#"{"type":"object","properties":{"cwd":{"type":"string"},"paths":{"type":"array","items":{"type":"string"}}},"required":["paths"]}"#,
        call: |ctx, args| blocking(ctx, args, add),
    },
    VerbDef {
        name: "git_rm",
        description: "Remove the given workspace paths from the disk and the index, with git rm, so the removal is staged. Each path is explicit; \".\" is refused. Commit it with git_commit, which runs the commit gate. cwd, when given, is the directory whose repository the verb acts on: absolute, or relative to the workspace root; a directory outside a git repository is refused. Omitted, the workspace is used.",
        schema: r#"{"type":"object","properties":{"cwd":{"type":"string"},"paths":{"type":"array","items":{"type":"string"}}},"required":["paths"]}"#,
        call: |ctx, args| blocking(ctx, args, rm),
    },
    VerbDef {
        name: "git_commit",
        description: "Commit only the given paths, with the message. Stage new files with git_add first. The commit gate runs first: ASCII of every tracked text file in the paths, every path that DESIGN.md names exists, and cargo check of rust/ when rust/ is committed. A failed check refuses the commit. gate reports each check with its time, and notCovered lists what the gate does not check. cwd, when given, is the directory whose repository the verb acts on: absolute, or relative to the workspace root; a directory outside a git repository is refused. Omitted, the workspace is used.",
        schema: r#"{"type":"object","properties":{"cwd":{"type":"string"},"message":{"type":"string"},"paths":{"type":"array","items":{"type":"string"}}},"required":["message","paths"]}"#,
        call: |ctx, args| blocking(ctx, args, commit),
    },
    VerbDef {
        name: "git_push",
        description: "Push main to origin and set its upstream. Refused unless the current branch is main. cwd, when given, is the directory whose repository the verb acts on: absolute, or relative to the workspace root; a directory outside a git repository is refused. Omitted, the workspace is used.",
        schema: r#"{"type":"object","properties":{"cwd":{"type":"string"}},"required":[]}"#,
        call: |ctx, args| blocking(ctx, args, push),
    },
    VerbDef {
        name: "git_log",
        description: "The last count commits, one line each, optionally only for one path (count 1 to 200, default 20). cwd, when given, is the directory whose repository the verb acts on: absolute, or relative to the workspace root; a directory outside a git repository is refused. Omitted, the workspace is used.",
        schema: r#"{"type":"object","properties":{"cwd":{"type":"string"},"count":{"type":"number"},"path":{"type":"string"}},"required":[]}"#,
        call: |ctx, args| blocking(ctx, args, log),
    },
    VerbDef {
        name: "git_diff",
        description: "Diff of the given paths (default: all), or of the staged index when staged is true. cwd, when given, is the directory whose repository the verb acts on: absolute, or relative to the workspace root; a directory outside a git repository is refused. Omitted, the workspace is used.",
        schema: r#"{"type":"object","properties":{"cwd":{"type":"string"},"paths":{"type":"array","items":{"type":"string"}},"staged":{"type":"boolean"}},"required":[]}"#,
        call: |ctx, args| blocking(ctx, args, diff),
    },
    VerbDef {
        name: "git_show",
        description: "Commit message and changed files for a ref (default HEAD). cwd, when given, is the directory whose repository the verb acts on: absolute, or relative to the workspace root; a directory outside a git repository is refused. Omitted, the workspace is used.",
        schema: r#"{"type":"object","properties":{"cwd":{"type":"string"},"ref":{"type":"string"}},"required":[]}"#,
        call: |ctx, args| blocking(ctx, args, show),
    },
    VerbDef {
        name: "git_fetch",
        description: "Fetch origin. The working tree is not changed. cwd, when given, is the directory whose repository the verb acts on: absolute, or relative to the workspace root; a directory outside a git repository is refused. Omitted, the workspace is used.",
        schema: r#"{"type":"object","properties":{"cwd":{"type":"string"}},"required":[]}"#,
        call: |ctx, args| blocking(ctx, args, fetch),
    },
    VerbDef {
        name: "git_pull",
        description: "Fast-forward pull of the current branch from its upstream. Never merges. cwd, when given, is the directory whose repository the verb acts on: absolute, or relative to the workspace root; a directory outside a git repository is refused. Omitted, the workspace is used.",
        schema: r#"{"type":"object","properties":{"cwd":{"type":"string"}},"required":[]}"#,
        call: |ctx, args| blocking(ctx, args, pull),
    },
    VerbDef {
        name: "git_checkout",
        description: "Switch to an existing branch, tag or commit. Creates nothing. cwd, when given, is the directory whose repository the verb acts on: absolute, or relative to the workspace root; a directory outside a git repository is refused. Omitted, the workspace is used.",
        schema: r#"{"type":"object","properties":{"cwd":{"type":"string"},"ref":{"type":"string"}},"required":["ref"]}"#,
        call: |ctx, args| blocking(ctx, args, checkout),
    },
    VerbDef {
        name: "git_branch",
        description: "List local branches. The current branch is marked with *. cwd, when given, is the directory whose repository the verb acts on: absolute, or relative to the workspace root; a directory outside a git repository is refused. Omitted, the workspace is used.",
        schema: r#"{"type":"object","properties":{"cwd":{"type":"string"}},"required":[]}"#,
        call: |ctx, args| blocking(ctx, args, branch),
    },
];
