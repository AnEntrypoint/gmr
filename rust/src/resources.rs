use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::log::{self, LOG_FILE};
use crate::skills::{self, SERVED_SKILLS};

pub const LOG_META: &str = "com.anentrypoint.gmnew/log";
const LOG_TAIL_BYTES: u64 = 65536;
const COMPLETION_CAP: usize = 100;
const POLL: Duration = Duration::from_millis(250);

pub fn templates() -> Value {
    json!([
        { "uriTemplate": "skill://{name}/SKILL.md", "name": "skill-md", "title": "Served skill", "description": "The SKILL.md of a served skill: its frontmatter and its prompt text.", "mimeType": "text/markdown" },
        { "uriTemplate": "skill://{name}/{path}", "name": "skill-file", "title": "Served skill file", "description": "Any other file of a served skill, by its path under the skill directory." },
        { "uriTemplate": "run://{run}/log", "name": "run-log", "title": "Run event log", "description": "A run's event log, one JSON event per line: its newest 64 KB, cut at a line boundary.", "mimeType": "application/x-ndjson" },
        { "uriTemplate": "run://{run}/witness", "name": "run-witness", "title": "Run witness", "description": "The witness a run finished with. It is listed once the run has ended.", "mimeType": "text/plain" }
    ])
}

fn is_run_id(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphanumeric()) && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn run_log(runs: &Path, run: &str) -> PathBuf {
    runs.join(run).join(LOG_FILE)
}

pub fn run_ids(runs: &Path) -> Vec<String> {
    let Ok(entries) = fs::read_dir(runs) else { return Vec::new() };
    let mut ids: Vec<String> = entries
        .flatten()
        .filter(|entry| entry.file_type().is_ok_and(|t| t.is_dir()))
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| is_run_id(name) && run_log(runs, name).exists())
        .collect();
    ids.sort();
    ids.reverse();
    ids
}

fn run_uri(uri: &str) -> Option<(&str, &str)> {
    let (run, part) = uri.strip_prefix("run://")?.split_once('/')?;
    (is_run_id(run) && (part == "log" || part == "witness")).then_some((run, part))
}

fn witness_of(file: &Path) -> Option<String> {
    let text = fs::read_to_string(file).ok()?;
    let end = text
        .lines()
        .rev()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|event| event.get("kind").and_then(Value::as_str) == Some("end"))?;
    end.get("witness").and_then(Value::as_str).map(str::to_string)
}

pub fn modified_at(file: &Path) -> Option<String> {
    let at = fs::metadata(file).ok()?.modified().ok()?.duration_since(UNIX_EPOCH).ok()?;
    Some(log::iso_of(at.as_secs() as i64, at.subsec_millis()))
}

pub fn run_resources(log_file: &Path, run: &str) -> Vec<Value> {
    let modified = modified_at(log_file);
    let mut out = vec![json!({
        "uri": format!("run://{run}/log"),
        "name": format!("{run} log"),
        "title": "Run event log",
        "mimeType": "application/x-ndjson",
        "annotations": { "audience": ["assistant"], "priority": 0.5, "lastModified": modified }
    })];
    if witness_of(log_file).is_some() {
        out.push(json!({
            "uri": format!("run://{run}/witness"),
            "name": format!("{run} witness"),
            "title": "Run witness",
            "mimeType": "text/plain",
            "annotations": { "audience": ["user", "assistant"], "priority": 0.8, "lastModified": modified }
        }));
    }
    out
}

fn tail_of(file: &Path) -> std::io::Result<(String, u64, u64)> {
    let size = fs::metadata(file)?.len();
    let at = size.saturating_sub(LOG_TAIL_BYTES);
    let mut buf = vec![0u8; (size - at) as usize];
    let mut handle = File::open(file)?;
    handle.seek(SeekFrom::Start(at))?;
    handle.read_exact(&mut buf)?;
    let start = if at == 0 {
        0
    } else {
        buf.iter().position(|b| *b == b'\n').map_or(buf.len(), |newline| newline + 1)
    };
    Ok((String::from_utf8_lossy(&buf[start..]).into_owned(), size, at + start as u64))
}

pub fn read_run(runs: &Path, uri: &str) -> Option<Value> {
    let (run, part) = run_uri(uri)?;
    let file = run_log(runs, run);
    if !file.exists() {
        return None;
    }
    if part == "witness" {
        let witness = witness_of(&file)?;
        return Some(json!({ "uri": uri, "mimeType": "text/plain", "text": witness }));
    }
    let (text, size, from) = tail_of(&file).ok()?;
    Some(json!({
        "uri": uri,
        "mimeType": "application/x-ndjson",
        "text": text,
        "_meta": { LOG_META: { "totalBytes": size, "fromByte": from, "truncated": from > 0 } }
    }))
}

fn candidates(uri: Option<&str>, argument: Option<&str>, context: Option<&Value>, runs: &Path) -> Vec<String> {
    match uri {
        Some("skill://{name}/SKILL.md") | Some("skill://{name}/{path}") => match argument {
            Some("name") => SERVED_SKILLS.iter().map(|s| s.to_string()).collect(),
            Some("path") => context
                .and_then(|c| c.get("name"))
                .and_then(Value::as_str)
                .filter(|name| SERVED_SKILLS.contains(name))
                .and_then(|name| skills::skill(name).ok())
                .map(|skill| skill.files.iter().map(|f| f.path.clone()).collect())
                .unwrap_or_default(),
            _ => Vec::new(),
        },
        Some("run://{run}/log") | Some("run://{run}/witness") if argument == Some("run") => run_ids(runs),
        _ => Vec::new(),
    }
}

pub fn completion(params: &Value, runs: &Path) -> Value {
    let uri = params.pointer("/ref/uri").and_then(Value::as_str);
    let argument = params.pointer("/argument/name").and_then(Value::as_str);
    let prefix = match params.pointer("/argument/value") {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    };
    let matching: Vec<String> = candidates(uri, argument, params.pointer("/context/arguments"), runs).into_iter().filter(|v| v.starts_with(&prefix)).collect();
    let values: Vec<String> = matching.iter().take(COMPLETION_CAP).cloned().collect();
    json!({ "completion": { "values": values, "total": matching.len(), "hasMore": matching.len() > values.len() } })
}

pub fn digest_of(uri: &str) -> Option<String> {
    let (name, _) = skills::split_uri(uri)?;
    let skill = skills::skill(name).ok()?;
    skill.files.iter().find(|f| f.uri == uri).map(|f| f.digest.clone())
}

type Seen = Mutex<BTreeMap<String, Option<String>>>;

pub struct Subscriptions {
    seen: Seen,
    poller: Mutex<Option<tokio::task::JoinHandle<()>>>,
    out: mpsc::UnboundedSender<Option<String>>,
}

impl Subscriptions {
    pub fn new(out: mpsc::UnboundedSender<Option<String>>) -> Arc<Self> {
        Arc::new(Subscriptions { seen: Mutex::new(BTreeMap::new()), poller: Mutex::new(None), out })
    }

    pub fn subscribe(self: &Arc<Self>, uri: &str, digest: String) {
        lock(&self.seen).insert(uri.to_string(), Some(digest));
        let mut poller = lock(&self.poller);
        if poller.is_none() {
            let me = self.clone();
            *poller = Some(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(POLL).await;
                    me.check();
                }
            }));
        }
    }

    pub fn unsubscribe(&self, uri: &str) {
        let empty = {
            let mut seen = lock(&self.seen);
            seen.remove(uri);
            seen.is_empty()
        };
        if empty {
            if let Some(poller) = lock(&self.poller).take() {
                poller.abort();
            }
        }
    }

    fn check(&self) {
        let snapshot: Vec<(String, Option<String>)> = lock(&self.seen).iter().map(|(uri, seen)| (uri.clone(), seen.clone())).collect();
        for (uri, seen) in snapshot {
            let now = digest_of(&uri);
            if now == seen {
                continue;
            }
            lock(&self.seen).insert(uri.clone(), now);
            let _ = self.out.send(Some(json!({ "jsonrpc": "2.0", "method": "notifications/resources/updated", "params": { "uri": uri } }).to_string()));
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
