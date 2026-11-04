use std::collections::HashMap;
use std::fs::{self, File};
use std::sync::Arc;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::run::Run;
use crate::search::{absolute, blocking, fail, git_output, resolve};
use crate::types::{Ctx, Fut, VerbDef, VerbOutcome};

const OUTPUT_PAGE: usize = 20_000;
const VERDICTS: [&str; 3] = ["pass", "fail", "needs-changes"];
const SHOW_LIMIT: usize = 64 * 1024 * 1024;
const SHOW_TIMEOUT: Duration = Duration::from_secs(10);

pub const VERBS: &[VerbDef] = &[
    VerbDef {
        name: "review_packet",
        description: "Open a review of a spec file and the files it refers to. The references must exist. Returns the packet hash that review_record takes. A missing reference, or a spec that is not a file, is refused and logged.",
        schema: r#"{"type":"object","properties":{"spec":{"type":"string"},"refs":{"type":"array","items":{"type":"string"}}},"required":["spec"]}"#,
        call: packet_call,
    },
    VerbDef {
        name: "review_record",
        description: "Record one reviewer verdict (pass, fail or needs-changes) on a packet, with the reviewer and its notes. Refused when the spec changed since the packet, when the packet already has a record, or when the verdict is unknown. Each record says isolation host-attested.",
        schema: r#"{"type":"object","properties":{"packet":{"type":"string"},"verdict":{"type":"string"},"reviewer":{"type":"string"},"notes":{"type":"string"}},"required":["packet","verdict","reviewer"]}"#,
        call: record_call,
    },
    VerbDef {
        name: "review_join",
        description: "Join the review records of this run with the commits and the ends after them: each commit with its gate outcome, and each end with its status and witness. A commit made after its spec changed is unattributed.",
        schema: r#"{"type":"object","properties":{},"required":[]}"#,
        call: join_call,
    },
];

fn packet_call(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    blocking(ctx, args, packet_work)
}

fn record_call(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    blocking(ctx, args, record_work)
}

fn join_call(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    blocking(ctx, args, join_work)
}

fn packet_work(ctx: &Ctx, args: &Value) -> Value {
    packet(&absolute(&ctx.workspace), ctx.run.as_ref(), args)
}

fn record_work(ctx: &Ctx, args: &Value) -> Value {
    record(&absolute(&ctx.workspace), ctx.run.as_ref(), args)
}

fn join_work(ctx: &Ctx, _args: &Value) -> Value {
    join(&absolute(&ctx.workspace), ctx.run.as_ref())
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|b| format!("{b:02x}")).collect()
}

fn js_text(v: Option<&Value>) -> String {
    match v {
        None => "undefined".to_string(),
        Some(Value::Null) => "null".to_string(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

fn truthy(v: Option<&Value>) -> bool {
    match v {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().is_some_and(|f| f != 0.0 && !f.is_nan()),
        Some(Value::String(s)) => !s.is_empty(),
        Some(_) => true,
    }
}

fn file_at(root: &Path, rel: &str) -> Option<PathBuf> {
    if rel.is_empty() {
        return None;
    }
    let at = resolve(root, rel);
    fs::metadata(&at).ok().filter(|m| m.is_file()).map(|_| at)
}

fn sha_of_file(path: &Path) -> Option<String> {
    fs::read(path).ok().map(|bytes| sha256_hex(&bytes))
}

fn each_event(run: &Run, mut f: impl FnMut(Value)) {
    let Ok(file) = File::open(run.dir.join("log.jsonl")) else { return };
    for line in BufReader::new(file).lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(event) = serde_json::from_str::<Value>(&line) {
            f(event);
        }
    }
}

fn packet(root: &Path, run: &Run, args: &Value) -> Value {
    let refuse = |reason: &str| {
        run.event("review-refused", json!({"reason": reason, "stage": "packet"}));
        fail(&format!("refused: {reason}"))
    };
    let refs: Vec<String> = match args.get("refs") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => match items.iter().map(|v| v.as_str().map(str::to_string)).collect() {
            Some(list) => list,
            None => return refuse("refs must be a list of paths"),
        },
        Some(_) => return refuse("refs must be a list of paths"),
    };
    let spec = args.get("spec").and_then(Value::as_str).unwrap_or("");
    let Some(spec_at) = file_at(root, spec) else {
        return refuse(&format!("spec {} is not a file", js_text(args.get("spec"))));
    };
    let missing: Vec<&str> = refs
        .iter()
        .filter(|r| file_at(root, r).is_none())
        .map(String::as_str)
        .collect();
    if !missing.is_empty() {
        return refuse(&format!("referenced paths do not exist: {}", missing.join(", ")));
    }
    let spec_sha = match sha_of_file(&spec_at) {
        Some(sha) => sha,
        None => return fail("spec is not readable"),
    };
    let mut sorted = refs;
    sorted.sort();
    let canonical = json!({"spec": spec, "specSha": spec_sha, "refs": sorted}).to_string();
    let hash = sha256_hex(canonical.as_bytes());
    run.event(
        "review-packet",
        json!({"packet": hash, "spec": spec, "specSha": spec_sha, "refs": sorted}),
    );
    let result = json!({"ok": true, "packet": hash, "spec": spec, "specSha": spec_sha, "refs": sorted});
    json!({"ok": true, "output": result.to_string()})
}

fn record(root: &Path, run: &Run, args: &Value) -> Value {
    let packet_arg = args.get("packet");
    let refuse = |reason: &str| {
        let mut fields = serde_json::Map::new();
        if let Some(v) = packet_arg {
            fields.insert("packet".to_string(), v.clone());
        }
        fields.insert("reason".to_string(), json!(reason));
        fields.insert("stage".to_string(), json!("record"));
        run.event("review-refused", Value::Object(fields));
        fail(&format!("refused: {reason}"))
    };
    let Some(packet) = packet_arg.and_then(Value::as_str) else {
        return refuse(&format!("no review packet {} in this log", js_text(packet_arg)));
    };
    let mut made: Option<(String, String)> = None;
    let mut reviewed = false;
    each_event(run, |event| {
        let kind = event.get("kind").and_then(Value::as_str);
        let same = event.get("packet").and_then(Value::as_str) == Some(packet);
        if kind == Some("review-packet") && same && made.is_none() {
            let spec = event.get("spec").and_then(Value::as_str).unwrap_or("").to_string();
            let sha = event.get("specSha").and_then(Value::as_str).unwrap_or("").to_string();
            made = Some((spec, sha));
        }
        if kind == Some("review") && same {
            reviewed = true;
        }
    });
    let Some((spec, made_sha)) = made else {
        return refuse(&format!("no review packet {packet} in this log"));
    };
    if reviewed {
        return refuse(&format!("packet {packet} already has a record"));
    }
    let verdict = match args.get("verdict").and_then(Value::as_str) {
        Some(v) if VERDICTS.contains(&v) => v,
        _ => return refuse("verdict must be one of pass, fail, needs-changes"),
    };
    let reviewer = match args.get("reviewer").and_then(Value::as_str) {
        Some(r) if !r.trim().is_empty() => r,
        _ => return refuse("reviewer must name the fresh reviewer that the host spawned"),
    };
    let notes = match args.get("notes") {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(n)) => n.clone(),
        Some(_) => return refuse("notes must be a string"),
    };
    let now_sha = file_at(root, &spec).and_then(|p| sha_of_file(&p));
    if now_sha.as_deref() != Some(made_sha.as_str()) {
        let now = now_sha.unwrap_or_else(|| "missing".to_string());
        return refuse(&format!(
            "the spec changed since packet {packet}: packet sha {made_sha}, now {now}"
        ));
    }
    run.event(
        "review",
        json!({
            "packet": packet,
            "verdict": verdict,
            "reviewer": reviewer,
            "notes": notes,
            "isolation": "host-attested",
            "specSha": made_sha,
        }),
    );
    let result = json!({"ok": true, "packet": packet, "verdict": verdict});
    json!({"ok": true, "output": result.to_string()})
}

fn spec_sha_at(root: &Path, commit: &str, spec: &str) -> Option<String> {
    let shown = format!("{commit}:{spec}");
    git_output(root, &["show", shown.as_str()], SHOW_LIMIT, SHOW_TIMEOUT).map(|out| sha256_hex(&out))
}

struct Row {
    head: Value,
    spec: Option<String>,
    spec_sha: Option<String>,
    outcomes: Vec<Value>,
    unattributed: Vec<Value>,
}

fn place(row: &mut Row, covered: bool, mut item: Value, reason: &str) {
    if covered {
        row.outcomes.push(item);
    } else {
        item["reason"] = json!(reason);
        row.unattributed.push(item);
    }
}

fn join(root: &Path, run: &Run) -> Value {
    let mut packets: HashMap<String, (String, String)> = HashMap::new();
    each_event(run, |event| {
        if event.get("kind").and_then(Value::as_str) == Some("review-packet") {
            if let Some(packet) = event.get("packet").and_then(Value::as_str) {
                let spec = event.get("spec").and_then(Value::as_str).unwrap_or("").to_string();
                let sha = event.get("specSha").and_then(Value::as_str).unwrap_or("").to_string();
                packets.insert(packet.to_string(), (spec, sha));
            }
        }
    });
    let mut rows: Vec<Row> = Vec::new();
    let mut current: Option<usize> = None;
    let mut commit_covered = true;
    each_event(run, |event| {
        let kind = event.get("kind").and_then(Value::as_str).unwrap_or("");
        let seq = event.get("seq").cloned().unwrap_or(Value::Null);
        if kind == "review" {
            let packet = event.get("packet").and_then(Value::as_str);
            let made = packet.and_then(|p| packets.get(p));
            let spec = made.map(|m| m.0.clone());
            let spec_sha = made.map(|m| m.1.clone());
            let head = json!({
                "packet": event.get("packet").cloned().unwrap_or(Value::Null),
                "verdict": event.get("verdict").cloned().unwrap_or(Value::Null),
                "reviewer": event.get("reviewer").cloned().unwrap_or(Value::Null),
                "isolation": event.get("isolation").cloned().unwrap_or(Value::Null),
                "seq": seq,
                "spec": spec,
                "specSha": spec_sha,
            });
            rows.push(Row {
                head,
                spec,
                spec_sha,
                outcomes: Vec::new(),
                unattributed: Vec::new(),
            });
            current = Some(rows.len() - 1);
            return;
        }
        let Some(index) = current else { return };
        let row = &mut rows[index];
        match kind {
            "commit" => {
                let spec_at = match (event.get("sha").and_then(Value::as_str), row.spec.as_deref()) {
                    (Some(commit), Some(spec)) if !commit.is_empty() => spec_sha_at(root, commit, spec),
                    _ => None,
                };
                commit_covered = spec_at.is_some() && spec_at.as_deref() == row.spec_sha.as_deref();
                let gate_ok = match event.get("gate") {
                    Some(Value::Array(checks)) => json!(checks.iter().all(|c| truthy(c.get("ok")))),
                    _ => Value::Null,
                };
                let reason = if spec_at.is_none() {
                    "no spec at the commit"
                } else {
                    "after-edit"
                };
                let item = json!({
                    "seq": seq,
                    "kind": "commit",
                    "gateOk": gate_ok,
                    "sha": event.get("sha").cloned().unwrap_or(Value::Null),
                    "specSha": spec_at,
                });
                place(row, commit_covered, item, reason);
            }
            "result" if event.get("tool").and_then(Value::as_str) == Some("git_commit") => {
                let ok = event.get("ok") == Some(&Value::Bool(true));
                let covered = if ok { commit_covered } else { true };
                let item = json!({"seq": seq, "kind": "git_commit", "ok": ok});
                place(row, covered, item, "its commit is after-edit");
            }
            "end" => {
                let now = row
                    .spec
                    .as_deref()
                    .and_then(|spec| file_at(root, spec))
                    .and_then(|p| sha_of_file(&p));
                let covered = now.is_some() && now.as_deref() == row.spec_sha.as_deref();
                let reason = if now.is_none() { "no spec on disk" } else { "after-edit" };
                let item = json!({
                    "seq": seq,
                    "kind": "end",
                    "status": event.get("status").cloned().unwrap_or(Value::Null),
                    "witness": event.get("witness").cloned().unwrap_or(Value::Null),
                    "specSha": now,
                });
                place(row, covered, item, reason);
            }
            _ => {}
        }
    });
    let values: Vec<Value> = rows
        .into_iter()
        .map(|row| {
            let mut value = row.head;
            value["outcomes"] = Value::Array(row.outcomes);
            value["unattributed"] = Value::Array(row.unattributed);
            value
        })
        .collect();
    let mut text = String::new();
    pretty(&Value::Array(values), 0, &mut text);
    let output = if text.chars().count() > OUTPUT_PAGE {
        let head: String = text.chars().take(OUTPUT_PAGE).collect();
        format!("{head}\n(truncated at {OUTPUT_PAGE} characters)")
    } else {
        text
    };
    json!({"ok": true, "output": output})
}

fn push_indent(out: &mut String, depth: usize) {
    for _ in 0..depth {
        out.push(' ');
    }
}

fn pretty(value: &Value, depth: usize, out: &mut String) {
    match value {
        Value::Array(items) if !items.is_empty() => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                out.push_str(if i == 0 { "\n" } else { ",\n" });
                push_indent(out, depth + 1);
                pretty(item, depth + 1, out);
            }
            out.push('\n');
            push_indent(out, depth);
            out.push(']');
        }
        Value::Object(map) if !map.is_empty() => {
            out.push('{');
            for (i, (key, item)) in map.iter().enumerate() {
                out.push_str(if i == 0 { "\n" } else { ",\n" });
                push_indent(out, depth + 1);
                out.push_str(&serde_json::to_string(key).unwrap_or_default());
                out.push_str(": ");
                pretty(item, depth + 1, out);
            }
            out.push('\n');
            push_indent(out, depth);
            out.push('}');
        }
        other => out.push_str(&other.to_string()),
    }
}
