use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::{bench, log, loop_run};

const USAGE: &str = "usage:
  gmnew run --task <file-or-text> --workspace <dir> [--model scripted] [--max-steps N]
            [--script <replies.json>] [--faults 429,429] [--serial] [--no-background]
            [--latency-ms N] [--overlap]
  gmnew replay (--run <id> | --dir <run dir>) [--workspace <dir>] [--max-steps N]
  gmnew bench [--task <name>] [--latency-ms N]
  gmnew events (--run <id> | --dir <run dir>) (--read-clear | --wait [--timeout-ms N])
  gmnew query (--run <id> | --dir <run dir>) [--kind K] [--after SEQ] [--last N]
  gmnew doctor [--workspace <dir>]";
const BOOLEAN_FLAGS: [&str; 5] = ["read-clear", "wait", "serial", "no-background", "overlap"];
const QUERY_CAP: usize = 20_000;
const DEFAULT_TIMEOUT_MS: u64 = 1_800_000;
const LIMITS: &str = "limits: run_shell is unconfined, so a shell command can hard-link a file from outside the workspace; the process-group kill is unverified on Windows";
const INSTALL_HINT: &str = "Run the remove and install commands that setup.sh prints.";

#[derive(Default)]
pub(crate) struct Flags {
    values: HashMap<String, String>,
    switches: HashSet<String>,
}

impl Flags {
    fn parse(argv: &[String]) -> Result<Flags, String> {
        let mut flags = Flags::default();
        let mut at = 0;
        while at < argv.len() {
            let name = &argv[at];
            let Some(key) = name.strip_prefix("--") else {
                return Err(format!("unexpected argument: {name}"));
            };
            if BOOLEAN_FLAGS.contains(&key) {
                flags.switches.insert(key.to_string());
                at += 1;
                continue;
            }
            match argv.get(at + 1) {
                Some(value) if !value.starts_with("--") => {
                    flags.values.insert(key.to_string(), value.clone());
                    at += 2;
                }
                _ => return Err(format!("{name} needs a value")),
            }
        }
        Ok(flags)
    }

    pub(crate) fn value(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    pub(crate) fn switch(&self, key: &str) -> bool {
        self.switches.contains(key)
    }
}

pub fn main(args: Vec<String>) -> i32 {
    let Some((command, rest)) = args.split_first() else {
        eprintln!("{USAGE}");
        return 2;
    };
    let result = match command.as_str() {
        "events" => Flags::parse(rest).and_then(|flags| events(&flags)),
        "query" => Flags::parse(rest).and_then(|flags| query(&flags)),
        "doctor" => Flags::parse(rest).and_then(|flags| doctor(&flags)),
        "run" => Flags::parse(rest).and_then(|flags| loop_run::run(&flags)),
        "replay" => Flags::parse(rest).and_then(|flags| loop_run::replay(&flags)),
        "bench" => return bench::main(rest),
        _ => {
            eprintln!("{USAGE}");
            return 2;
        }
    };
    match result {
        Ok(code) => code,
        Err(message) => {
            eprintln!("gmnew: {message}");
            1
        }
    }
}

fn events(flags: &Flags) -> Result<i32, String> {
    let wait = flags.switch("wait");
    if !wait && !flags.switch("read-clear") {
        return Err("events takes --read-clear, which prints the pending events once and clears them, or --wait".to_string());
    }
    let dir = run_dir_of(flags)?;
    let text = if wait {
        log::wait_pending(&dir, timeout_ms(flags)?)
    } else {
        log::read_clear(&dir)
    };
    write_stdout(&text);
    Ok(0)
}

fn query(flags: &Flags) -> Result<i32, String> {
    if flags.value("js").is_some() {
        return Err("query takes --kind, --after and --last; the --js filter is deferred in the Rust port".to_string());
    }
    let dir = run_dir_of(flags)?;
    let file = dir.join("log.jsonl");
    if !file.exists() {
        return Err(format!("no log at {}", file.display()));
    }
    let kind = flags.value("kind");
    let after = match flags.value("after") {
        Some(value) => value.parse::<u64>().map_err(|_| "--after takes a whole number of events".to_string())?,
        None => 0,
    };
    let mut matched: Vec<Value> = read_events(&file)?
        .into_iter()
        .filter(|event| event.get("seq").and_then(Value::as_u64).unwrap_or(0) > after)
        .filter(|event| kind.is_none_or(|kind| event.get("kind").and_then(Value::as_str) == Some(kind)))
        .collect();
    if let Some(last) = flags.value("last") {
        let last = last.parse::<usize>().map_err(|_| "--last takes a whole number of events".to_string())?;
        let skip = matched.len().saturating_sub(last);
        matched.drain(..skip);
    }
    let text = serde_json::to_string(&matched).map_err(|err| err.to_string())?;
    write_stdout(&format!("{}\n", capped(text)));
    Ok(0)
}

fn capped(text: String) -> String {
    match text.char_indices().nth(QUERY_CAP) {
        Some((at, _)) => format!("{} ...[cut at {QUERY_CAP} chars, narrow the filter]", &text[..at]),
        None => text,
    }
}

fn write_stdout(text: &str) {
    let mut out = io::stdout().lock();
    let _ = out.write_all(text.as_bytes());
    let _ = out.flush();
}

fn timeout_ms(flags: &Flags) -> Result<u64, String> {
    match flags.value("timeout-ms") {
        None => Ok(DEFAULT_TIMEOUT_MS),
        Some(value) => value
            .parse::<u64>()
            .ok()
            .filter(|ms| *ms >= 1)
            .ok_or_else(|| "--timeout-ms must be a positive integer".to_string()),
    }
}

pub(crate) fn run_dir_of(flags: &Flags) -> Result<PathBuf, String> {
    if let Some(dir) = flags.value("dir") {
        let dir = std::path::absolute(dir).map_err(|err| err.to_string())?;
        if !dir.is_dir() {
            return Err(format!("no run directory {}", dir.display()));
        }
        return Ok(dir);
    }
    let id = flags
        .value("run")
        .filter(|id| !id.is_empty() && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'))
        .ok_or_else(|| "--run <id> or --dir <run dir> is required".to_string())?;
    let dir = runs_dir().join(id);
    if !dir.is_dir() {
        return Err(format!("no run {id}"));
    }
    Ok(dir)
}

fn runs_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap_or(Path::new(".")).join("runs")
}

fn read_events(file: &Path) -> Result<Vec<Value>, String> {
    let text = fs::read_to_string(file).map_err(|err| format!("{}: {err}", file.display()))?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<Value>(line).map_err(|err| format!("{}: {err}", file.display())))
        .collect()
}

fn doctor(flags: &Flags) -> Result<i32, String> {
    let workspace = match flags.value("workspace") {
        Some(dir) => std::path::absolute(dir).map_err(|err| err.to_string())?,
        None => std::env::current_dir().map_err(|err| err.to_string())?,
    };
    println!("gmnew {} ({} {}): ok", env!("CARGO_PKG_VERSION"), std::env::consts::OS, std::env::consts::ARCH);
    let writable = probe_writable(&workspace);
    match &writable {
        Ok(()) => println!("workspace {}: writable", workspace.display()),
        Err(error) => println!("workspace {}: not writable ({error})", workspace.display()),
    }
    println!("{}", last_run_summary());
    let registration = registration_status();
    println!("{}", registration.1);
    println!("{LIMITS}");
    Ok(if writable.is_ok() && registration.0 { 0 } else { 1 })
}

fn probe_writable(dir: &Path) -> Result<(), String> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    let probe = dir.join(format!(".gmnew-doctor-{:08x}", (nanos as u32) ^ std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
        .map_err(|err| format!("{:?}", err.kind()))?;
    file.write_all(b"probe\n").map_err(|err| format!("{:?}", err.kind()))?;
    drop(file);
    fs::remove_file(&probe).map_err(|err| format!("{:?}", err.kind()))
}

fn last_run_summary() -> String {
    let runs = runs_dir();
    if !runs.is_dir() {
        return "last run: none (no runs/ directory)".to_string();
    }
    let mut found: Vec<(String, PathBuf)> = Vec::new();
    for dir in [runs.clone(), runs.join("self")] {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let file = entry.path().join("log.jsonl");
            if is_run_id(&name) && file.exists() {
                found.push((name, file));
            }
        }
    }
    found.sort_by(|a, b| a.0.cmp(&b.0));
    let Some((id, file)) = found.last() else {
        return "last run: none".to_string();
    };
    let events = read_events(file).unwrap_or_default();
    format!("last run {id}: {} events, outcome {}", events.len(), last_outcome(&events))
}

fn is_run_id(name: &str) -> bool {
    let parts: Vec<&str> = name.split('-').collect();
    parts.len() == 7
        && parts[..6].iter().enumerate().all(|(index, part)| {
            let width = if index == 0 { 4 } else { 2 };
            part.len() == width && part.bytes().all(|b| b.is_ascii_digit())
        })
        && parts[6].len() == 6
        && parts[6].bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn last_outcome(events: &[Value]) -> String {
    for event in events.iter().rev() {
        match event.get("kind").and_then(Value::as_str) {
            Some("end") => return text_of(event.get("status")),
            Some("result")
                if event.get("tool").and_then(Value::as_str) == Some("finish")
                    && event.get("finished") == Some(&Value::Bool(true)) =>
            {
                return "finished".to_string();
            }
            _ => {}
        }
    }
    "none".to_string()
}

fn text_of(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => "undefined".to_string(),
        Some(Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
    }
}

fn claude_config_file() -> PathBuf {
    let base = std::env::var_os("CLAUDE_CONFIG_DIR")
        .or_else(|| std::env::var_os("HOME"))
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_default();
    base.join(".claude.json")
}

fn registration_status() -> (bool, String) {
    let file = claude_config_file();
    if !file.exists() {
        return (true, format!("gm user entry: no {}, so none is registered", file.display()));
    }
    let config: Value = match fs::read_to_string(&file).map_err(|err| err.to_string()).and_then(|text| serde_json::from_str(&text).map_err(|err| err.to_string())) {
        Ok(config) => config,
        Err(err) => return (false, format!("gm user entry: {} is not readable JSON ({err})", file.display())),
    };
    let Some(entry) = config.get("mcpServers").and_then(|servers| servers.get("gm")) else {
        return (true, format!("gm user entry: none in {}", file.display()));
    };
    let relay = entry.get("args").and_then(|args| args.get(0)).and_then(Value::as_str);
    match relay {
        Some(relay) if Path::new(relay).exists() => (true, format!("gm user entry runs {relay}")),
        _ => (
            false,
            format!(
                "gm user entry in {} runs {}, which does not exist. {INSTALL_HINT}",
                file.display(),
                relay.unwrap_or("nothing")
            ),
        ),
    }
}
