use std::collections::HashSet;
use std::fs;
use std::sync::OnceLock;

use regex::Regex;
use serde_json::{json, Map, Value};

use crate::run::{Run, DEFAULT_WITNESS};

pub const NEXT_CONTEXT_EVENTS_CAP: usize = 20000;
pub const H1: u64 = 14;
pub const H2: u64 = 12;
const LINE_CAP: usize = 400;
const ARG_CAP: usize = 200;
const ARG_HEAD: usize = 80;
const FAIL_LINES: usize = 60;
const ARRAY_OVERHEAD: usize = 2;
const OUTPUT_PAGE: usize = 20000;
const MAX_WAIT_MS: f64 = 300000.0;
const SKIP: &[&str] = &["t", "mono", "inFlight", "startMono", "durMs", "actionSeq", "step", "seq", "kind"];
const KEEP_WHOLE: &[&str] = &["witness", "error", "reason", "gate", "gaps", "paths"];
const KEEP_ZONE2: &[&str] = &["end", "commit", "refusal", "error", "restart", "job-killed", "review", "review-packet", "review-refused", "lint", "wiki", "recovery", "parse_error"];
const REPLY_TEXT: &str = "## Reply\n\nOne JSON object {\"tool\": \"...\", \"args\": {...}}, or a JSON array of them when the actions are independent. Nothing else.";

fn error_line() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?i)\b(error|errors|fail|failed|failure|fatal|warn|warning|exception|traceback|denied|not ok)\b").expect("regex"))
}

fn units(text: &str) -> usize {
    text.encode_utf16().count()
}

fn head(text: &str, cap: usize) -> &str {
    match text.char_indices().nth(cap) {
        Some((i, _)) => &text[..i],
        None => text,
    }
}

pub fn clip(text: &str, cap: usize) -> Value {
    let n = units(text);
    if n <= cap {
        return Value::from(text);
    }
    Value::from(format!("{} ...[{n} chars]", head(text, cap)))
}

pub fn args_of(args: Option<&Value>) -> Value {
    let mut out = Map::new();
    if let Some(Value::Object(map)) = args {
        for (k, v) in map {
            let value = match v {
                Value::String(s) if units(s) > ARG_CAP => json!({ "chars": units(s), "head": clip(s, ARG_HEAD) }),
                Value::Array(a) if a.len() > 10 => json!({ "items": a.len() }),
                other => other.clone(),
            };
            out.insert(k.clone(), value);
        }
    }
    Value::Object(out)
}

pub fn output_digest(text: &str, failed: bool) -> Value {
    if text.is_empty() {
        return json!({ "lines": 0, "bytes": 0 });
    }
    let lines: Vec<&str> = text.split('\n').collect();
    let n = lines.len();
    let mut digest = json!({
        "lines": n,
        "bytes": text.len(),
        "head": lines.iter().take(2).map(|l| clip(l, LINE_CAP)).collect::<Vec<_>>(),
        "tail": lines[n.saturating_sub(2)..].iter().map(|l| clip(l, LINE_CAP)).collect::<Vec<_>>(),
    });
    let errors: Vec<Value> = lines.iter().filter(|l| error_line().is_match(l)).map(|l| clip(l, LINE_CAP)).collect();
    if !errors.is_empty() {
        digest["errors"] = Value::Array(errors);
    }
    if failed {
        digest["all"] = Value::Array(lines.iter().take(FAIL_LINES).map(|l| clip(l, LINE_CAP)).collect());
        if n > FAIL_LINES {
            digest["more"] = json!(n - FAIL_LINES);
        }
    }
    digest
}

fn output_bytes(event: &Value) -> usize {
    event
        .get("outputBytes")
        .and_then(Value::as_u64)
        .map(|n| n as usize)
        .unwrap_or_else(|| event.get("output").and_then(Value::as_str).map(str::len).unwrap_or(0))
}

fn result_base(event: &Value, action: Option<&Value>) -> Map<String, Value> {
    let mut r = Map::new();
    r.insert("seq".into(), event.get("seq").cloned().unwrap_or(Value::Null));
    r.insert("tool".into(), event.get("tool").cloned().unwrap_or(Value::Null));
    r.insert("ok".into(), event.get("ok").cloned().unwrap_or(Value::Null));
    r.insert("args".into(), args_of(action.and_then(|a| a.get("args"))));
    if let Some(x) = event.get("exitCode") {
        r.insert("exit".into(), x.clone());
    }
    if let Some(x) = event.get("error") {
        r.insert("error".into(), x.clone());
    }
    r
}

pub fn result_zone1(event: &Value, action: Option<&Value>) -> Value {
    let mut r = result_base(event, action);
    if let Some(digest) = event.get("outputDigest") {
        r.insert("out".into(), digest.clone());
    } else if let Some(out) = event.get("output").and_then(Value::as_str) {
        let failed = event.get("ok") == Some(&Value::Bool(false)) || event.get("exitCode").and_then(Value::as_f64).is_some_and(|c| c != 0.0);
        r.insert("out".into(), output_digest(out, failed));
    }
    r.insert("full".into(), json!({ "seq": event.get("seq"), "bytes": output_bytes(event) }));
    Value::Object(r)
}

pub fn result_zone2(event: &Value, action: Option<&Value>) -> Value {
    let mut r = result_base(event, action);
    r.insert("full".into(), json!({ "seq": event.get("seq"), "bytes": output_bytes(event) }));
    Value::Object(r)
}

fn fields_of(event: &Value, whole: bool) -> Value {
    let mut r = Map::new();
    r.insert("seq".into(), event.get("seq").cloned().unwrap_or(Value::Null));
    r.insert("kind".into(), event.get("kind").cloned().unwrap_or(Value::Null));
    if let Some(map) = event.as_object() {
        for (k, v) in map.iter().filter(|(k, _)| !SKIP.contains(&k.as_str())) {
            let keep = whole && KEEP_WHOLE.contains(&k.as_str());
            let value = match v {
                Value::String(s) if !keep => clip(s, ARG_CAP),
                other => other.clone(),
            };
            r.insert(k.clone(), value);
        }
    }
    Value::Object(r)
}

pub fn short_kind(event: &Value) -> Value {
    fields_of(event, false)
}

pub fn kept_kind(event: &Value) -> Option<Value> {
    let kind = event.get("kind").and_then(Value::as_str).unwrap_or("");
    KEEP_ZONE2.contains(&kind).then(|| fields_of(event, true))
}

fn bytes_of(action: Option<&Value>, key: &str) -> Value {
    action.and_then(|a| a.get("args")).and_then(|a| a.get(key)).and_then(Value::as_str).map(|s| json!(s.len())).unwrap_or(Value::Null)
}

fn arg_of(action: Option<&Value>, key: &str) -> Value {
    action.and_then(|a| a.get("args")).and_then(|a| a.get(key)).cloned().unwrap_or(Value::Null)
}

fn with_args(mut record: Value, args: Value) -> Value {
    record["args"] = args;
    record
}

fn tool_record(tool: &str, zone: u8, event: &Value, action: Option<&Value>) -> Option<Value> {
    let default = || if zone == 1 { result_zone1(event, action) } else { result_zone2(event, action) };
    match tool {
        "list_dir" | "query_log" | "prd_list" | "unknown_list" | "callers" | "impact" | "git_log" | "git_diff" | "git_show" | "search" | "grep" | "wiki_search" | "review_join" if zone == 2 => None,
        "write_file" => Some(with_args(default(), json!({ "path": arg_of(action, "path"), "bytes": bytes_of(action, "content") }))),
        "edit_file" => Some(with_args(default(), json!({ "path": arg_of(action, "path"), "oldBytes": bytes_of(action, "old"), "newBytes": bytes_of(action, "new") }))),
        "finish" => Some(json!({
            "seq": event.get("seq"), "tool": "finish", "ok": event.get("ok"),
            "witness": arg_of(action, "witness"), "gaps": arg_of(action, "gaps"),
            "full": { "seq": event.get("seq"), "bytes": output_bytes(event) },
        })),
        "fetch" => {
            let mut r = default();
            r["source"] = event.get("source").cloned().unwrap_or(Value::Null);
            r["sourceNew"] = event.get("sourceNew").cloned().unwrap_or(Value::Null);
            Some(r)
        }
        "wiki_write" if zone == 2 => Some(json!({
            "seq": event.get("seq"), "tool": "wiki_write", "ok": event.get("ok"),
            "page": arg_of(action, "page"),
            "claims": action.and_then(|a| a.get("args")).and_then(|a| a.get("claims")).and_then(Value::as_array).map(|c| c.len()).unwrap_or(0),
            "full": { "seq": event.get("seq"), "bytes": output_bytes(event) },
        })),
        _ => Some(default()),
    }
}

struct Source<'a> {
    run: &'a Run,
    base: u64,
    events: Vec<Value>,
}

impl Source<'_> {
    fn at(&self, seq: u64) -> Option<Value> {
        if seq > self.base && seq <= self.base + self.events.len() as u64 {
            return Some(self.events[(seq - self.base - 1) as usize].clone());
        }
        self.run.at(seq)
    }

    fn get(&self, seq: u64) -> &Value {
        &self.events[(seq - self.base - 1) as usize]
    }
}

fn record_of(event: &Value, zone: u8, src: &Source, answered: &HashSet<u64>) -> Option<Value> {
    let kind = event.get("kind").and_then(Value::as_str).unwrap_or("");
    let seq = event.get("seq").and_then(Value::as_u64).unwrap_or(0);
    if kind == "action" {
        if answered.contains(&seq) || zone != 1 {
            return None;
        }
        return Some(json!({ "seq": seq, "kind": "action", "tool": event.get("tool"), "args": args_of(event.get("args")), "unanswered": true }));
    }
    if kind == "result" || kind == "code-call" {
        let fetched;
        let action = if kind == "code-call" {
            Some(event)
        } else {
            fetched = event.get("actionSeq").and_then(Value::as_u64).and_then(|a| src.at(a));
            fetched.as_ref()
        };
        let tool = event.get("tool").and_then(Value::as_str).unwrap_or("");
        return tool_record(tool, zone, event, action);
    }
    if kind == "code-load" {
        return if zone == 1 { Some(short_kind(event)) } else { None };
    }
    if zone == 1 {
        Some(short_kind(event))
    } else {
        kept_kind(event)
    }
}

fn fresh_record_of(event: &Value, src: &Source) -> Value {
    if event.get("kind").and_then(Value::as_str) == Some("action") {
        return json!({ "seq": event.get("seq"), "kind": "action", "tool": event.get("tool"), "args": args_of(event.get("args")) });
    }
    record_of(event, 1, src, &HashSet::new()).unwrap_or_else(|| short_kind(event))
}

fn cost_of(v: &Value) -> usize {
    units(&v.to_string()) + 1
}

struct Fresh {
    index: u64,
    cost: usize,
    record: Value,
    record_cost: usize,
}

pub fn feed_of(run: &Run, shown: u64) -> Map<String, Value> {
    let n = run.count();
    let new_start = shown.min(n);
    let zone1_start = new_start.saturating_sub(H1);
    let zone2_start = zone1_start.saturating_sub(H2);
    let src = Source { run, base: zone2_start, events: run.read_lines(zone2_start + 1, n) };
    let n = zone2_start + src.events.len() as u64;
    let new_start = new_start.min(n);
    let answered: HashSet<u64> = src.events.iter().filter_map(|e| e.get("actionSeq").and_then(Value::as_u64)).collect();
    let mut seen: Vec<(u64, Value, usize)> = Vec::new();
    for i in zone2_start..new_start {
        let zone = if i >= zone1_start { 1 } else { 2 };
        if let Some(record) = record_of(src.get(i + 1), zone, &src, &answered) {
            let cost = cost_of(&record);
            seen.push((i, record, cost));
        }
    }
    let mut fresh: Vec<Fresh> = Vec::new();
    for i in new_start..n {
        let event = src.get(i + 1);
        let record = fresh_record_of(event, &src);
        fresh.push(Fresh { index: i, cost: cost_of(event), record_cost: cost_of(&record), record });
    }
    let room = NEXT_CONTEXT_EVENTS_CAP - ARRAY_OVERHEAD;
    let (full, mut items, used) = plan_fresh(&fresh, &src, n, room);
    let mut spare = room.saturating_sub(used);
    let mut kept_from = seen.len();
    while kept_from > 0 && seen[kept_from - 1].2 <= spare {
        spare -= seen[kept_from - 1].2;
        kept_from -= 1;
    }
    let older = if kept_from < seen.len() { seen[kept_from].0 } else { new_start };
    let compact_from = if full < fresh.len() { json!(fresh[full].index + 1) } else { Value::Null };
    let mut events: Vec<Value> = seen.drain(kept_from..).map(|(_, r, _)| r).collect();
    events.append(&mut items);
    let mut out = Map::new();
    out.insert("older".into(), json!(older));
    out.insert("records".into(), json!(events.len()));
    out.insert("fullEvents".into(), json!(full));
    out.insert("compactFrom".into(), compact_from);
    out.insert("events".into(), Value::Array(events));
    out
}

fn plan_fresh(fresh: &[Fresh], src: &Source, last_seq: u64, room: usize) -> (usize, Vec<Value>, usize) {
    let tail_of = |k: usize| -> Option<Value> {
        (k < fresh.len()).then(|| json!({ "kind": "range", "seqs": [fresh[k].index + 1, last_seq], "count": last_seq - fresh[k].index }))
    };
    let tail_cost = |k: usize| tail_of(k).map(|v| cost_of(&v)).unwrap_or(0);
    let record_total: usize = fresh.iter().map(|f| f.record_cost).sum();
    if record_total <= room {
        let mut spare = room - record_total;
        let mut full = 0;
        while full < fresh.len() && fresh[full].cost.saturating_sub(fresh[full].record_cost) <= spare {
            spare -= fresh[full].cost.saturating_sub(fresh[full].record_cost);
            full += 1;
        }
        let items = fresh.iter().enumerate().map(|(i, f)| if i < full { src.get(f.index + 1).clone() } else { f.record.clone() }).collect();
        return (full, items, room - spare);
    }
    let mut used = 0;
    let mut full = 0;
    while full < fresh.len() && used + fresh[full].cost + tail_cost(full + 1) <= room {
        used += fresh[full].cost;
        full += 1;
    }
    let mut items: Vec<Value> = fresh[..full].iter().map(|f| src.get(f.index + 1).clone()).collect();
    if let Some(t) = tail_of(full) {
        used += cost_of(&t);
        items.push(t);
    }
    (full, items, used)
}

pub fn render_prefix(rules: &str, task: &str, witness: &str, tools: &str) -> String {
    [rules.trim().to_string(), format!("## Task\n\n{task}"), format!("## Witness requirement\n\n{witness}"), format!("## Tools\n\n{tools}"), REPLY_TEXT.to_string()].join("\n\n")
}

pub fn render_tools(tools: &[(&str, &str, &str)]) -> String {
    tools.iter().map(|(name, schema, description)| format!("{name} {schema}: {description}")).collect::<Vec<_>>().join("\n")
}

pub fn prefix_sha(run: &Run, rules: &str) -> String {
    crate::run::sha_hex(render_prefix(rules, &run.task(), DEFAULT_WITNESS, &run.tools_text()).as_bytes())
}

pub fn query_hint(shown: u64, total: u64) -> String {
    let reach = "query_log reads any event (after: seq - 1 returns it first)";
    if total > shown {
        format!("{reach}. This step's new events are seqs {} to {total}.", shown + 1)
    } else {
        format!("{reach}.")
    }
}

pub fn wait_ms_of(args: &Value) -> u64 {
    match args.get("waitMs").and_then(Value::as_f64) {
        Some(ms) if ms.is_finite() && ms > 0.0 => ms.min(MAX_WAIT_MS) as u64,
        _ => 0,
    }
}

pub fn digest(reply: &Value) -> Value {
    let mut out = Map::new();
    for key in ["ok", "error", "prefixSha", "total"] {
        if let Some(v) = reply.get(key) {
            out.insert(key.into(), v.clone());
        }
    }
    Value::Object(out)
}

fn recovery_note(recovery: Value) -> Value {
    let last_seq = recovery.get("lastSeq").cloned().unwrap_or(Value::Null);
    let open = recovery.get("open").cloned().unwrap_or_else(|| json!([]));
    let list: Vec<String> = open
        .as_array()
        .map(|a| a.iter().map(|x| format!("seq {} {} {}", x.get("seq").cloned().unwrap_or(Value::Null), x.get("tool").and_then(Value::as_str).unwrap_or(""), x.get("args").cloned().unwrap_or(Value::Null))).collect())
        .unwrap_or_default();
    json!({
        "lastSeq": last_seq,
        "open": open,
        "note": format!("This run resumed after a restart, at last seq {last_seq}. These actions started and have no end event: {}. Before you repeat any of them, check the outside effects: git log and git status, the remote, and the row files under work/rows. Do not repeat a push, a commit, a fetch that posted, or a row resolve unless those checks show that it did not happen.", list.join("; ")),
    })
}

fn pick(event: &Value, keys: &[&str]) -> Value {
    let mut out = Map::new();
    for k in keys {
        if let Some(v) = event.get(*k) {
            out.insert((*k).into(), v.clone());
        }
    }
    Value::Object(out)
}

fn ended_job(run: &Run, event: &Value) -> Value {
    let job = event.get("job").and_then(Value::as_str).unwrap_or("");
    let jobs = run.dir.join("jobs");
    let text = if event.get("body").is_some() {
        fs::read_to_string(jobs.join(format!("{job}.json")))
            .ok()
            .and_then(|t| serde_json::from_str::<Value>(&t).ok())
            .and_then(|s| s.get("value").or(s.get("error")).cloned())
            .map(|v| match v {
                Value::String(s) => s,
                other => other.to_string(),
            })
            .unwrap_or_default()
    } else {
        fs::read_to_string(jobs.join(format!("{job}.out"))).unwrap_or_default()
    };
    let total = units(&text);
    json!({
        "seq": event.get("seq"), "job": job, "state": event.get("state"), "exitCode": event.get("exitCode"), "signal": event.get("signal"),
        "output": head(&text, OUTPUT_PAGE),
        "next": if total > OUTPUT_PAGE { json!(OUTPUT_PAGE) } else { Value::Null },
    })
}

fn kind_is(e: &Value, kind: &str) -> bool {
    e.get("kind").and_then(Value::as_str) == Some(kind)
}

pub fn wait_command(run: &Run) -> String {
    let exe = std::env::current_exe().map(|p| p.to_string_lossy().into_owned()).unwrap_or_else(|_| "gmnew".to_string());
    format!("\"{exe}\" events --dir \"{}\" --wait", run.dir.display())
}

pub fn next_context(run: &Run, args: &Value) -> Value {
    if run.ended().is_some() {
        return json!({ "ok": false, "refused": true, "error": format!("refused: {}", run.ended_reason(false)) });
    }
    let step = match run.begin_step() {
        Ok(s) => s,
        Err(text) => {
            run.end_run("max_steps", None, None);
            return json!({ "ok": false, "refused": true, "error": format!("refused: {text}") });
        }
    };
    run.clear_delivered();
    let rules = fs::read_to_string(run.dir.join("rules.md")).unwrap_or_else(|_| crate::run::MCP_RULES.to_string());
    let recovery = run.take_recovery().map(recovery_note);
    let total = run.count();
    let notice = run.swap_notice_seq(total);
    let since = run.read_lines(notice + 1, total);
    let killed: Vec<&Value> = since.iter().filter(|e| kind_is(e, "job-killed")).collect();
    let killed_names: HashSet<&str> = killed.iter().filter_map(|e| e.get("job").and_then(Value::as_str)).collect();
    let state_of = |e: &Value| e.get("state").and_then(Value::as_str).unwrap_or("").to_string();
    let started: Vec<Value> = since.iter().filter(|e| kind_is(e, "job") && state_of(e) == "running" && e.get("body").is_some()).map(|e| pick(e, &["seq", "job"])).collect();
    let running: Vec<Value> = since.iter().filter(|e| kind_is(e, "job-running")).map(|e| pick(e, &["seq", "job", "runningMs", "prd"])).collect();
    let stuck: Vec<Value> = since.iter().filter(|e| kind_is(e, "job-stuck")).map(|e| pick(e, &["seq", "job", "body", "sinceProgressMs"])).collect();
    let unstuck: Vec<Value> = since.iter().filter(|e| kind_is(e, "job-unstuck")).map(|e| pick(e, &["seq", "job"])).collect();
    let ended: Vec<Value> = since
        .iter()
        .filter(|e| kind_is(e, "job") && state_of(e) != "running" && !(state_of(e) == "killed" && e.get("job").and_then(Value::as_str).is_some_and(|j| killed_names.contains(j))))
        .map(|e| ended_job(run, e))
        .collect();
    let killed: Vec<Value> = killed.iter().map(|e| pick(e, &["seq", "job", "reason", "lifetimeMs", "tail"])).collect();
    drop(since);
    let shown = run.shown();
    let feed = feed_of(run, shown);
    run.set_shown(total);
    let task = run.task();
    let mut reply = Map::new();
    reply.insert("ok".into(), json!(true));
    reply.insert("step".into(), json!(step));
    reply.insert("run".into(), json!(run.id));
    reply.insert("stepsLeft".into(), json!(run.max_steps().saturating_sub(step)));
    reply.insert("maxSteps".into(), json!(run.max_steps()));
    reply.insert("prefixSha".into(), json!(prefix_sha(run, &rules)));
    reply.insert("rules".into(), json!(rules));
    if task.is_empty() {
        reply.insert("task".into(), Value::Null);
        reply.insert("taskNotice".into(), json!("No task yet. The gm skill prompt, with its task argument, gives the task of this run."));
    } else {
        reply.insert("task".into(), json!(task));
    }
    reply.insert("witness".into(), json!(DEFAULT_WITNESS));
    reply.insert("total".into(), json!(total));
    reply.insert("pendingCommands".into(), json!({ "wait": wait_command(run) }));
    reply.extend(feed);
    reply.insert("query".into(), json!(query_hint(shown, total)));
    if let Some(r) = recovery {
        reply.insert("recovery".into(), r);
    }
    for (key, list) in [("jobsKilled", killed), ("jobsStarted", started), ("jobsRunning", running), ("jobsEnded", ended), ("jobsStuck", stuck), ("jobsUnstuck", unstuck)] {
        if !list.is_empty() {
            reply.insert(key.into(), Value::Array(list));
        }
    }
    let _ = args;
    match crate::log::take_pending(&run.dir) {
        Ok(taken) => {
            if !taken.records.is_empty() {
                let records: Vec<Value> = taken.records.iter().filter_map(|r| serde_json::from_str(r).ok()).collect();
                reply.insert("pending".into(), Value::Array(records));
            }
            run.set_delivered(taken.files);
        }
        Err(e) => {
            reply.insert("pendingError".into(), json!(e.to_string()));
        }
    }
    Value::Object(reply)
}
