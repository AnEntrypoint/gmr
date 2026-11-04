use std::fs::{self, File};
use std::io::{BufRead, BufReader, ErrorKind};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Map, Value};

use crate::git::{blocking, confine, fail, is_sha, random_u64, root_of, run_command, CHECK_TIMEOUT, GIT};
use crate::types::{Ctx, VerbDef};

const ROW_DIR: &str = "work/rows";
const FIELDS: [&str; 9] = ["id", "kind", "title", "body", "status", "createdAt", "witnessEvent", "witnessCommit", "resolvedAt"];
const STATUSES: [&str; 3] = ["open", "resolved", "all"];
const PRD: &str = "prd";
const UNKNOWN: &str = "unknown";
const BASE36: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";

pub fn open_rows(root: &Path) -> Result<Vec<Value>, String> {
    let mut rows: Vec<Value> = read_rows(root)?.into_iter().filter(|row| text(row, "status") == "open").collect();
    sort_by_created(&mut rows);
    Ok(rows)
}

pub fn row_summary(root: &Path, kind: &str, limit: usize) -> Result<Value, String> {
    let mut rows: Vec<Value> = read_rows(root)?.into_iter().filter(|row| text(row, "kind") == kind).collect();
    sort_by_created(&mut rows);
    let open: Vec<&Value> = rows.iter().filter(|row| text(row, "status") == "open").collect();
    let window: Vec<Value> = open.iter().take(limit).map(|row| json!({ "id": text(row, "id"), "title": text(row, "title") })).collect();
    let more = open.len() - window.len();
    Ok(json!({ "total": rows.len(), "open": open.len(), "window": window, "more": more }))
}

fn text<'a>(row: &'a Value, key: &str) -> &'a str {
    row.get(key).and_then(Value::as_str).unwrap_or("")
}

fn sort_by_created(rows: &mut [Value]) {
    rows.sort_by(|a, b| text(a, "createdAt").cmp(text(b, "createdAt")).then_with(|| text(a, "id").cmp(text(b, "id"))));
}

fn valid_id(kind: &str, id: &str) -> bool {
    id.strip_prefix(kind)
        .and_then(|rest| rest.strip_prefix('-'))
        .is_some_and(|tail| !tail.is_empty() && tail.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()))
}

fn row_path(root: &Path, id: &str) -> Result<PathBuf, String> {
    confine(root, &format!("{ROW_DIR}/{id}.yml"))
}

fn read_rows(root: &Path) -> Result<Vec<Value>, String> {
    let dir = confine(root, ROW_DIR)?;
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.to_string()),
    };
    let mut rows = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| err.to_string())?;
        if !entry.file_name().to_string_lossy().ends_with(".yml") {
            continue;
        }
        let source = fs::read_to_string(entry.path()).map_err(|err| err.to_string())?;
        rows.push(parse_row(&source));
    }
    Ok(rows)
}

fn read_row(root: &Path, id: &str) -> Result<Option<Value>, String> {
    let path = row_path(root, id)?;
    match fs::read_to_string(&path) {
        Ok(source) => Ok(Some(parse_row(&source))),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.to_string()),
    }
}

fn write_row(root: &Path, row: &Value) -> Result<(), String> {
    let path = row_path(root, text(row, "id"))?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let temp = PathBuf::from(format!("{}.{}.tmp", path.display(), std::process::id()));
    fs::write(&temp, render_row(row)).map_err(|err| err.to_string())?;
    fs::rename(&temp, &path).map_err(|err| err.to_string())
}

fn render_row(row: &Value) -> String {
    let mut out = String::new();
    for field in FIELDS {
        if let Some(value) = row.get(field) {
            out.push_str(&format!("{field}: {value}\n"));
        }
    }
    out
}

fn parse_row(source: &str) -> Value {
    let mut row = Map::new();
    for line in source.split('\n') {
        let Some((key, raw)) = line.split_once(": ") else { continue };
        if !key.is_empty() && key.bytes().all(|b| b.is_ascii_alphabetic()) {
            row.insert(key.to_string(), serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_string())));
        }
    }
    Value::Object(row)
}

fn base36(mut n: u64) -> String {
    if n == 0 {
        return "0".into();
    }
    let mut digits = Vec::new();
    while n > 0 {
        digits.push(BASE36[(n % 36) as usize]);
        n /= 36;
    }
    digits.reverse();
    digits.into_iter().map(char::from).collect()
}

fn now_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn new_id(kind: &str) -> String {
    format!("{kind}-{}{:04x}", base36(now_millis()), random_u64() & 0xffff)
}

fn iso_now() -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs() as i64;
    let (year, month, day) = civil_from_days(secs.div_euclid(86_400));
    let rem = secs.rem_euclid(86_400);
    format!("{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z", rem / 3600, rem % 3600 / 60, rem % 60, now.subsec_millis())
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

fn event_in_log(run_dir: &Path, seq: u64) -> bool {
    let Ok(file) = File::open(run_dir.join("log.jsonl")) else { return false };
    BufReader::new(file).lines().map_while(Result::ok).any(|line| {
        serde_json::from_str::<Value>(&line).ok().and_then(|entry| entry.get("seq")?.as_u64()) == Some(seq)
    })
}

fn commit_exists(root: &Path, sha: &str) -> bool {
    let target = format!("{sha}^{{commit}}");
    run_command(GIT, &["cat-file", "-e", target.as_str()], root, CHECK_TIMEOUT).ok
}

fn add_row(ctx: &Ctx, kind: &str, args: &Value) -> Value {
    let title = match args.get("title").and_then(Value::as_str) {
        Some(title) if !title.trim().is_empty() => title.trim(),
        _ => return fail("title must be a non-empty string"),
    };
    let body = match args.get("body") {
        None => "",
        Some(Value::String(body)) => body.as_str(),
        Some(_) => return fail("body must be a string"),
    };
    let id = new_id(kind);
    let row = json!({ "id": id, "kind": kind, "title": title, "body": body, "status": "open", "createdAt": iso_now() });
    match write_row(&root_of(ctx), &row) {
        Ok(()) => json!({ "ok": true, "output": id }),
        Err(error) => fail(error),
    }
}

fn list_rows(ctx: &Ctx, kind: &str, args: &Value) -> Value {
    let status = match args.get("status") {
        None => "open".to_string(),
        Some(Value::String(status)) => status.clone(),
        Some(_) => String::new(),
    };
    if !STATUSES.contains(&status.as_str()) {
        return fail(format!("status must be one of {}", STATUSES.join(", ")));
    }
    let root = root_of(ctx);
    let mut rows: Vec<Value> = match read_rows(&root) {
        Ok(rows) => rows.into_iter().filter(|row| text(row, "kind") == kind && (status == "all" || text(row, "status") == status)).collect(),
        Err(error) => return fail(error),
    };
    sort_by_created(&mut rows);
    let output = if rows.is_empty() {
        "(none)".to_string()
    } else {
        rows.iter().map(|row| format!("{} {} {}", text(row, "id"), text(row, "status"), text(row, "title"))).collect::<Vec<_>>().join("\n")
    };
    json!({ "ok": true, "output": output })
}

fn delete_row(ctx: &Ctx, args: &Value) -> Value {
    let id = args.get("id").and_then(Value::as_str).unwrap_or("");
    if !valid_id(PRD, id) {
        return fail("id must name a prd row, with an id that starts prd-");
    }
    let root = root_of(ctx);
    match read_row(&root, id) {
        Err(error) => fail(error),
        Ok(None) => fail(format!("no row {id}")),
        Ok(Some(row)) if text(&row, "status") == "resolved" => fail(format!("row {id} is resolved; a resolved PRD is history and stays")),
        Ok(Some(_)) => match row_path(&root, id).and_then(|path| fs::remove_file(path).map_err(|err| err.to_string())) {
            Ok(()) => json!({ "ok": true, "output": format!("{id} deleted") }),
            Err(error) => fail(error),
        },
    }
}

fn resolve_row(ctx: &Ctx, kind: &str, args: &Value) -> Value {
    let id = args.get("id").and_then(Value::as_str).unwrap_or("");
    if !valid_id(kind, id) {
        return fail(format!("id must name a {kind} row, with an id that starts {kind}-"));
    }
    let Some(event) = args.get("event").and_then(Value::as_u64).filter(|seq| *seq >= 1) else {
        return fail("event must be a log seq number");
    };
    if !event_in_log(&ctx.run.dir, event) {
        return fail(format!("event {event} is not in this run's log"));
    }
    let commit = args.get("commit").and_then(Value::as_str).unwrap_or("");
    if !is_sha(commit) {
        return fail("commit must be 7 to 40 lowercase hex digits");
    }
    let root = root_of(ctx);
    if !commit_exists(&root, commit) {
        return fail(format!("no commit {commit} in this repository"));
    }
    let mut row = match read_row(&root, id) {
        Err(error) => return fail(error),
        Ok(None) => return fail(format!("no row {id}")),
        Ok(Some(row)) => row,
    };
    if text(&row, "status") == "resolved" {
        return fail(format!("row {id} is already resolved"));
    }
    row["status"] = json!("resolved");
    row["witnessEvent"] = json!(event);
    row["witnessCommit"] = json!(commit);
    row["resolvedAt"] = json!(iso_now());
    match write_row(&root, &row) {
        Ok(()) => json!({ "ok": true, "output": format!("{id} resolved by event {event} at commit {commit}") }),
        Err(error) => fail(error),
    }
}

pub const VERBS: &[VerbDef] = &[
    VerbDef {
        name: "prd_add",
        description: "Add a work item row, one file under work/rows. Returns its id.",
        schema: r#"{"type":"object","properties":{"title":{"type":"string"},"body":{"type":"string"}},"required":["title"]}"#,
        call: |ctx, args| blocking(ctx, args, |ctx, args| add_row(ctx, PRD, args)),
    },
    VerbDef {
        name: "prd_list",
        description: "List work item rows, one line each: id, status, title. status is open (default), resolved or all.",
        schema: r#"{"type":"object","properties":{"status":{"type":"string"}},"required":[]}"#,
        call: |ctx, args| blocking(ctx, args, |ctx, args| list_rows(ctx, PRD, args)),
    },
    VerbDef {
        name: "prd_resolve",
        description: "Resolve a work item row. The witness is a seq of this run's log and a commit that exists in this repository.",
        schema: r#"{"type":"object","properties":{"id":{"type":"string"},"event":{"type":"number"},"commit":{"type":"string"}},"required":["id","event","commit"]}"#,
        call: |ctx, args| blocking(ctx, args, |ctx, args| resolve_row(ctx, PRD, args)),
    },
    VerbDef {
        name: "unknown_add",
        description: "Add a open question row, one file under work/rows. Returns its id.",
        schema: r#"{"type":"object","properties":{"title":{"type":"string"},"body":{"type":"string"}},"required":["title"]}"#,
        call: |ctx, args| blocking(ctx, args, |ctx, args| add_row(ctx, UNKNOWN, args)),
    },
    VerbDef {
        name: "unknown_list",
        description: "List open question rows, one line each: id, status, title. status is open (default), resolved or all.",
        schema: r#"{"type":"object","properties":{"status":{"type":"string"}},"required":[]}"#,
        call: |ctx, args| blocking(ctx, args, |ctx, args| list_rows(ctx, UNKNOWN, args)),
    },
    VerbDef {
        name: "unknown_resolve",
        description: "Resolve a open question row. The witness is a seq of this run's log and a commit that exists in this repository.",
        schema: r#"{"type":"object","properties":{"id":{"type":"string"},"event":{"type":"number"},"commit":{"type":"string"}},"required":["id","event","commit"]}"#,
        call: |ctx, args| blocking(ctx, args, |ctx, args| resolve_row(ctx, UNKNOWN, args)),
    },
    VerbDef {
        name: "prd_delete",
        description: "Delete a work item (PRD) row that is not resolved: the work is dropped and its file is removed, and git keeps it. An open question is resolved, not deleted.",
        schema: r#"{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}"#,
        call: |ctx, args| blocking(ctx, args, delete_row),
    },
];
