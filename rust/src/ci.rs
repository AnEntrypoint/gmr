use std::path::Path;
use std::sync::OnceLock;

use regex::Regex;
use serde_json::{json, Value};

use crate::git::{blocking, fail, in_repo, is_repo, is_sha, raw_value, retried, run_command, Scope, CHECK_TIMEOUT, GH_TIMEOUT, GIT};
use crate::types::{Ctx, VerbDef};

const ORIGIN_HINT: &str = "origin has no owner/name path; pass repo as owner/name";
const ORIGIN_PATTERN: &str = r"(?i)^(?:[a-z][a-z0-9+.-]*://)?(?:[^@/\s]+@)?([^/:\s]+)(?::\d+)?[/:]([^/\s]+)/([^/\s]+?)(?:\.git)?/?$";

static ORIGIN: OnceLock<Regex> = OnceLock::new();

fn origin_regex() -> &'static Regex {
    ORIGIN.get_or_init(|| Regex::new(ORIGIN_PATTERN).expect("origin pattern is valid"))
}

fn origin_repo(dir: &Path) -> Option<(Option<String>, String)> {
    let url = run_command(GIT, &["remote", "get-url", "origin"], dir, CHECK_TIMEOUT);
    if !url.ok {
        return None;
    }
    let captures = origin_regex().captures(url.output.trim())?;
    let host = captures.get(1)?.as_str().to_string();
    let name = format!("{}/{}", captures.get(2)?.as_str(), captures.get(3)?.as_str());
    Some((Some(host), name))
}

fn js_text(value: Option<&Value>) -> String {
    match value {
        None => "undefined".to_string(),
        Some(Value::Null) => "null".to_string(),
        Some(Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
    }
}

fn run_lines(runs: &[Value]) -> Vec<String> {
    runs.iter()
        .map(|run| {
            let conclusion = match run.get("conclusion") {
                None | Some(Value::Null) => "-".to_string(),
                other => js_text(other),
            };
            format!("{} {} {} {}", js_text(run.get("name")), js_text(run.get("status")), conclusion, js_text(run.get("html_url")))
        })
        .collect()
}

fn ci_for(ctx: &Ctx, scope: &Scope, args: &Value) -> Value {
    let sha = match args.get("sha") {
        None => None,
        Some(Value::String(sha)) if is_sha(sha) => Some(sha.as_str()),
        Some(_) => return fail("sha must be 7 to 40 lowercase hex digits"),
    };
    let repo = match args.get("repo") {
        None => None,
        Some(Value::String(repo)) if is_repo(repo) => Some(repo.clone()),
        Some(_) => return fail("repo must be owner/name"),
    };
    let target = sha.unwrap_or("HEAD");
    let rev = format!("{target}^{{commit}}");
    let head = run_command(GIT, &["rev-parse", "--verify", rev.as_str()], &scope.dir, CHECK_TIMEOUT);
    if !head.ok {
        return fail(format!("no commit {target} in this repository"));
    }
    let (host, name) = match repo {
        Some(name) => (None, name),
        None => match origin_repo(&scope.dir) {
            Some(origin) => origin,
            None => return fail(ORIGIN_HINT),
        },
    };
    if !is_repo(&name) {
        return fail(ORIGIN_HINT);
    }
    let full = head.output.trim().to_string();
    let mut gh: Vec<String> = vec!["api".into()];
    if let Some(host) = host {
        gh.push("--hostname".into());
        gh.push(host);
    }
    gh.push("--paginate".into());
    gh.push("--slurp".into());
    gh.push(format!("repos/{name}/actions/runs?head_sha={full}&per_page=100"));
    let api = retried(&ctx.run, "ci_status", || raw_value(&run_command("gh", &gh, &scope.dir, GH_TIMEOUT)));
    if api["ok"] != json!(true) {
        let error = api["error"].as_str().map(String::from).unwrap_or_else(|| format!("gh api exited {}", api["exitCode"]));
        return json!({ "ok": false, "error": error, "output": api["output"].as_str().unwrap_or("").trim(), "attempts": api["attempts"] });
    }
    let Ok(pages) = serde_json::from_str::<Value>(api["output"].as_str().unwrap_or("")) else {
        return fail("gh api did not return JSON");
    };
    let Some(pages) = pages.as_array() else {
        return fail("gh api did not return JSON");
    };
    let runs: Vec<Value> = pages.iter().flat_map(|page| page["workflow_runs"].as_array().cloned().unwrap_or_default()).collect();
    let mut lines = vec![format!("{name} {full}")];
    let rows = run_lines(&runs);
    if rows.is_empty() {
        lines.push("no workflow runs".into());
    } else {
        lines.extend(rows);
    }
    json!({ "ok": true, "output": lines.join("\n") })
}

fn ci_status(ctx: &Ctx, args: &Value) -> Value {
    in_repo(ctx, args, |scope| ci_for(ctx, scope, args))
}

pub const VERBS: &[VerbDef] = &[VerbDef {
    name: "ci_status",
    description: "GitHub Actions runs for a commit (default HEAD), every page of them, with status and conclusion. repo is owner/name and defaults to origin; when it comes from origin, gh calls origin's host, whatever that host is. cwd, when given, is the directory whose repository the verb acts on: absolute, or relative to the workspace root; a directory outside a git repository is refused. Omitted, the workspace is used.",
    schema: r#"{"type":"object","properties":{"cwd":{"type":"string"},"sha":{"type":"string"},"repo":{"type":"string"}},"required":[]}"#,
    call: |ctx, args| blocking(ctx, args, ci_status),
}];
