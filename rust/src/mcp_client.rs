use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Map, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use crate::run::Run;
use crate::types::VerbOutcome;

const PROTOCOL_VERSIONS: [&str; 2] = ["2025-11-25", "2025-06-18"];
const CLIENT_NAME: &str = "gm-new";
const CLIENT_VERSION: &str = "0.1.0";
const STARTUP_MS: u64 = 30_000;
const CALL_MS: u64 = 60_000;
const STDERR_CHARS: usize = 2000;
const DESCRIPTION_CHARS: usize = 300;
const DEPTH_VARIABLE: &str = "GMNEW_MCP_DEPTH";
const MAX_DEPTH: u64 = 2;
const BATCH_LAUNCHERS: [&str; 2] = ["npx", "npm"];

#[derive(Clone, Debug)]
pub struct HubTool {
    pub server: String,
    pub tool: String,
    pub description: String,
    pub schema: Value,
}

pub struct Hub {
    sink: Arc<Sink>,
    runners: Mutex<Vec<Arc<Runner>>>,
    tools: Mutex<Vec<HubTool>>,
}

impl Hub {
    pub fn new() -> Self {
        Hub {
            sink: Arc::new(Sink::default()),
            runners: Mutex::new(Vec::new()),
            tools: Mutex::new(Vec::new()),
        }
    }

    pub fn attach(&self, run: Arc<Run>) {
        self.sink.attach(run);
    }

    pub async fn start(&self, workspace: PathBuf, layers: Vec<(PathBuf, bool)>) {
        let mut problems = Vec::new();
        let specs = read_specs(&layers, &workspace, &mut problems);
        for problem in problems {
            self.sink.event("mcp-config", json!({ "problem": problem }));
        }
        let depth = depth_from_env();
        let specs = if depth < MAX_DEPTH {
            specs
        } else {
            if !specs.is_empty() {
                self.sink.event(
                    "mcp-config",
                    json!({ "problem": format!("servers are not started at MCP depth {depth}, the limit is {MAX_DEPTH}") }),
                );
            }
            Vec::new()
        };
        let runners: Vec<Arc<Runner>> = specs
            .into_iter()
            .map(|spec| Arc::new(Runner::new(spec, self.sink.clone(), depth)))
            .collect();

        let mut starts = JoinSet::new();
        for runner in &runners {
            let runner = runner.clone();
            starts.spawn(async move {
                connect(&runner, false).await;
            });
        }
        while starts.join_next().await.is_some() {}

        let mut tools = Vec::new();
        let mut verbs: HashSet<String> = HashSet::new();
        for runner in &runners {
            for tool in runner.listed() {
                let name = text_field(&tool, "name");
                let verb = verb_name(&runner.spec.name, &name);
                if runner.spec.deny.contains(&name) {
                    self.sink.event(
                        "mcp-tool-skipped",
                        json!({ "server": runner.spec.name, "tool": name, "verb": verb, "reason": "the server entry denies it" }),
                    );
                    continue;
                }
                if !verbs.insert(verb.clone()) {
                    self.sink.event(
                        "mcp-tool-skipped",
                        json!({ "server": runner.spec.name, "tool": name, "verb": verb, "reason": "the name is another MCP tool" }),
                    );
                    continue;
                }
                tools.push(HubTool {
                    server: runner.spec.name.clone(),
                    tool: name.clone(),
                    description: describe(&runner.spec.name, &tool),
                    schema: tool.get("inputSchema").filter(|schema| !schema.is_null()).cloned().unwrap_or_else(|| json!({ "type": "object" })),
                });
            }
        }
        *self.runners.lock().unwrap() = runners;
        *self.tools.lock().unwrap() = tools;
    }

    pub fn tools(&self) -> Vec<HubTool> {
        self.tools.lock().unwrap().clone()
    }

    pub async fn call(&self, server: &str, tool: &str, args: Value) -> VerbOutcome {
        let runner = self.runners.lock().unwrap().iter().find(|runner| runner.spec.name == server).cloned();
        match runner {
            Some(runner) if !runner.spec.deny.iter().any(|denied| denied == tool) => Ok(call_tool(&runner, tool, args).await),
            Some(_) => Err(format!("MCP tool {tool} of {server} is denied")),
            None => Err(format!("no MCP server named {server}")),
        }
    }

    pub async fn stop(&self) {
        let runners = self.runners.lock().unwrap().clone();
        for runner in runners {
            runner.stop();
        }
    }
}

impl Default for Hub {
    fn default() -> Self {
        Self::new()
    }
}

pub fn verb_name(server: &str, tool: &str) -> String {
    let name = format!("{}_{}", ident_of(server), ident_of(tool));
    if name.starts_with(|c: char| c.is_ascii_digit()) {
        format!("mcp_{name}")
    } else {
        name
    }
}

fn ident_of(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            for _ in 0..ch.len_utf16() {
                out.push('_');
            }
        }
    }
    out
}

#[derive(Clone, Debug)]
struct Spec {
    name: String,
    command: String,
    args: Vec<String>,
    env: Vec<(String, String)>,
    cwd: PathBuf,
    timeout_ms: u64,
    startup_ms: u64,
    deny: Vec<String>,
    ignore: Vec<String>,
    workspace: PathBuf,
}

#[derive(Default)]
struct Sink {
    run: Mutex<Option<Arc<Run>>>,
    backlog: Mutex<Vec<(&'static str, Value)>>,
}

impl Sink {
    fn attach(&self, run: Arc<Run>) {
        let backlog = std::mem::take(&mut *self.backlog.lock().unwrap());
        for (kind, fields) in backlog {
            run.event(kind, fields);
        }
        *self.run.lock().unwrap() = Some(run);
    }

    fn event(&self, kind: &'static str, fields: Value) {
        let run = self.run.lock().unwrap().clone();
        match run {
            Some(run) => {
                run.event(kind, fields);
            }
            None => self.backlog.lock().unwrap().push((kind, fields)),
        }
    }
}

type Reply = Result<Value, String>;

#[derive(Default)]
struct State {
    gen: u64,
    up: bool,
    pid: Option<u32>,
    outgoing: Option<mpsc::UnboundedSender<String>>,
    pending: HashMap<u64, oneshot::Sender<Reply>>,
    next_id: u64,
    stderr: String,
}

struct Runner {
    spec: Spec,
    sink: Arc<Sink>,
    depth: u64,
    turn: tokio::sync::Mutex<()>,
    state: Mutex<State>,
    tools: Mutex<Vec<Value>>,
}

impl Runner {
    fn new(spec: Spec, sink: Arc<Sink>, depth: u64) -> Runner {
        Runner {
            spec,
            sink,
            depth,
            turn: tokio::sync::Mutex::new(()),
            state: Mutex::new(State { next_id: 1, ..State::default() }),
            tools: Mutex::new(Vec::new()),
        }
    }

    fn is_up(&self) -> bool {
        self.state.lock().unwrap().up
    }

    fn listed(&self) -> Vec<Value> {
        self.tools.lock().unwrap().clone()
    }

    fn stderr_text(&self) -> String {
        self.state.lock().unwrap().stderr.clone()
    }

    fn push_stderr(&self, text: &str) {
        let mut state = self.state.lock().unwrap();
        state.stderr.push_str(text);
        let length = state.stderr.chars().count();
        if length > STDERR_CHARS {
            let cut = state.stderr.char_indices().nth(length - STDERR_CHARS).map(|(at, _)| at).unwrap_or(0);
            state.stderr.drain(..cut);
        }
    }

    fn send(&self, message: &Value) -> Result<(), String> {
        let state = self.state.lock().unwrap();
        match &state.outgoing {
            Some(outgoing) => outgoing.send(encode(message)).map_err(|_| self.not_running()),
            None => Err(self.not_running()),
        }
    }

    fn not_running(&self) -> String {
        format!("MCP server {} is not running", self.spec.name)
    }

    fn notify(&self, method: &str, params: Option<Value>) -> Result<(), String> {
        let message = match params {
            Some(params) => json!({ "jsonrpc": "2.0", "method": method, "params": params }),
            None => json!({ "jsonrpc": "2.0", "method": method }),
        };
        self.send(&message)
    }

    fn lose(&self, gen: u64, reason: String) {
        let (waiting, stderr) = {
            let mut state = self.state.lock().unwrap();
            if state.gen != gen || !state.up {
                return;
            }
            state.up = false;
            state.pid = None;
            state.outgoing = None;
            let waiting: Vec<oneshot::Sender<Reply>> = state.pending.drain().map(|(_, sender)| sender).collect();
            (waiting, state.stderr.clone())
        };
        let error = format!("MCP server {} {reason}", self.spec.name);
        for sender in waiting {
            let _ = sender.send(Err(error.clone()));
        }
        self.sink.event(
            "mcp-server",
            json!({ "server": self.spec.name, "state": "down", "reason": reason, "stderr": stderr }),
        );
    }

    fn stop(&self) {
        let (gen, pid) = {
            let state = self.state.lock().unwrap();
            if !state.up {
                return;
            }
            (state.gen, state.pid)
        };
        self.lose(gen, "stopped by gm-new".to_string());
        if let Some(pid) = pid {
            kill_group(pid);
        }
    }

    fn receive(&self, line: &str) {
        let Ok(message) = serde_json::from_str::<Value>(line) else {
            return;
        };
        if let Some(method) = message.get("method").and_then(Value::as_str) {
            if let Some(id) = message.get("id") {
                let mut envelope = json!({ "jsonrpc": "2.0", "id": id });
                let body = if method == "ping" {
                    json!({ "result": {} })
                } else {
                    json!({ "error": { "code": -32601, "message": format!("gm-new does not serve {method}") } })
                };
                if let (Value::Object(envelope), Value::Object(body)) = (&mut envelope, body) {
                    envelope.extend(body);
                }
                let _ = self.send(&envelope);
            }
            return;
        }
        let Some(id) = message.get("id").and_then(Value::as_u64) else {
            return;
        };
        let Some(waiting) = self.state.lock().unwrap().pending.remove(&id) else {
            return;
        };
        let reply = match message.get("error") {
            Some(error) if !error.is_null() => Err(error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("the server returned an error")
                .to_string()),
            _ => Ok(match message.get("result") {
                Some(result) if !result.is_null() => result.clone(),
                _ => json!({}),
            }),
        };
        let _ = waiting.send(reply);
    }

    fn ignore_workspace(&self) {
        let spec = &self.spec;
        if spec.ignore.is_empty() {
            return;
        }
        if !spec.workspace.join(".git").exists() {
            self.sink.event(
                "gitignore",
                json!({ "server": spec.name, "state": "skipped", "reason": "the workspace is not a git repository", "ignore": spec.ignore }),
            );
            return;
        }
        let file = spec.workspace.join(".gitignore");
        match add_ignore_lines(&file, &spec.ignore) {
            Ok(added) if added.is_empty() => {}
            Ok(added) => self.sink.event(
                "gitignore",
                json!({ "server": spec.name, "state": "added", "file": file.display().to_string(), "added": added }),
            ),
            Err(err) => self.sink.event(
                "gitignore",
                json!({ "server": spec.name, "state": "failed", "file": file.display().to_string(), "error": err.to_string() }),
            ),
        }
    }
}

async fn connect(runner: &Arc<Runner>, again: bool) -> Option<String> {
    let _turn = runner.turn.lock().await;
    if runner.is_up() {
        return None;
    }
    handshake(runner, again).await
}

async fn handshake(runner: &Arc<Runner>, again: bool) -> Option<String> {
    let outcome = match start_process(runner) {
        Ok(()) => match tokio::time::timeout(Duration::from_millis(runner.spec.startup_ms), initialize(runner, again)).await {
            Ok(result) => result,
            Err(_) => Err(format!("startup timed out after {} ms", runner.spec.startup_ms)),
        },
        Err(error) => Err(error),
    };
    match outcome {
        Ok(()) => None,
        Err(error) => {
            runner.stop();
            runner.sink.event(
                "mcp-server",
                json!({ "server": runner.spec.name, "state": "failed", "error": error, "stderr": runner.stderr_text() }),
            );
            Some(error)
        }
    }
}

async fn initialize(runner: &Arc<Runner>, again: bool) -> Result<(), String> {
    let init = request(
        runner,
        "initialize",
        json!({
            "protocolVersion": PROTOCOL_VERSIONS[0],
            "capabilities": {},
            "clientInfo": { "name": CLIENT_NAME, "version": CLIENT_VERSION },
        }),
        runner.spec.startup_ms,
    )
    .await?;
    let version = init.get("protocolVersion").and_then(Value::as_str).map(str::to_string);
    let version_text = version.clone().unwrap_or_else(|| "undefined".to_string());
    if !version.as_deref().is_some_and(|v| PROTOCOL_VERSIONS.contains(&v)) {
        return Err(format!("protocol version {version_text} is not one gm-new speaks"));
    }
    runner.notify("notifications/initialized", None)?;
    let tools = list_tools(runner).await?;
    let count = tools.len();
    *runner.tools.lock().unwrap() = tools;
    runner.sink.event(
        "mcp-server",
        json!({
            "server": runner.spec.name,
            "state": if again { "restarted" } else { "connected" },
            "tools": count,
            "protocolVersion": version_text,
        }),
    );
    runner.ignore_workspace();
    Ok(())
}

async fn list_tools(runner: &Arc<Runner>) -> Result<Vec<Value>, String> {
    let mut tools = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut cursor: Option<String> = None;
    loop {
        let params = match &cursor {
            Some(cursor) => json!({ "cursor": cursor }),
            None => json!({}),
        };
        let page = request(runner, "tools/list", params, runner.spec.startup_ms).await?;
        if let Some(items) = page.get("tools").and_then(Value::as_array) {
            tools.extend(items.iter().cloned());
        }
        cursor = page
            .get("nextCursor")
            .and_then(Value::as_str)
            .filter(|next| !next.is_empty())
            .map(str::to_string);
        match &cursor {
            Some(next) if seen.insert(next.clone()) => {}
            Some(_) => break,
            None => break,
        }
    }
    Ok(tools)
}

fn start_process(runner: &Arc<Runner>) -> Result<(), String> {
    let spec = &runner.spec;
    let (program, args) = launch_of(spec);
    let mut command = Command::new(&program);
    command
        .args(&args)
        .current_dir(&spec.cwd)
        .envs(spec.env.iter().map(|(key, value)| (key, value)))
        .env(DEPTH_VARIABLE, (runner.depth + 1).to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    #[cfg(windows)]
    command.creation_flags(CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP);
    let mut child = command
        .spawn()
        .map_err(|err| format!("MCP server {} could not start: {err}", spec.name))?;
    let pid = child.id();
    let stdin = child.stdin.take();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let (outgoing, incoming) = mpsc::unbounded_channel::<String>();
    let gen = {
        let mut state = runner.state.lock().unwrap();
        state.gen += 1;
        state.up = true;
        state.pid = pid;
        state.outgoing = Some(outgoing);
        state.gen
    };
    if let Some(stdin) = stdin {
        tokio::spawn(write_requests(stdin, incoming));
    }
    if let Some(stdout) = stdout {
        tokio::spawn(read_replies(runner.clone(), stdout));
    }
    if let Some(stderr) = stderr {
        tokio::spawn(collect_stderr(runner.clone(), stderr));
    }
    tokio::spawn(await_exit(runner.clone(), gen, child));
    Ok(())
}

async fn write_requests(mut stdin: ChildStdin, mut incoming: mpsc::UnboundedReceiver<String>) {
    while let Some(line) = incoming.recv().await {
        if stdin.write_all(line.as_bytes()).await.is_err() {
            break;
        }
    }
}

async fn read_replies(runner: Arc<Runner>, stdout: ChildStdout) {
    let mut reader = BufReader::new(stdout);
    let mut line = Vec::new();
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        let text = String::from_utf8_lossy(&line);
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            runner.receive(trimmed);
        }
    }
}

async fn collect_stderr(runner: Arc<Runner>, mut stderr: ChildStderr) {
    let mut buffer = [0u8; 4096];
    loop {
        match stderr.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => runner.push_stderr(&String::from_utf8_lossy(&buffer[..read])),
        }
    }
}

async fn await_exit(runner: Arc<Runner>, gen: u64, mut child: Child) {
    let reason = match child.wait().await {
        Ok(status) => exit_reason(&status),
        Err(err) => format!("could not be waited for: {err}"),
    };
    runner.lose(gen, reason);
}

fn exit_reason(status: &ExitStatus) -> String {
    match status.code() {
        Some(code) => format!("exited with code {code}"),
        None => format!("exited with signal {}", signal_text(status)),
    }
}

#[cfg(unix)]
fn signal_text(status: &ExitStatus) -> String {
    use std::os::unix::process::ExitStatusExt;
    status.signal().map(|signal| signal.to_string()).unwrap_or_else(|| "unknown".to_string())
}

#[cfg(windows)]
fn signal_text(_status: &ExitStatus) -> String {
    "unknown".to_string()
}

#[cfg(unix)]
fn kill_group(pid: u32) {
    unsafe {
        libc::killpg(pid as libc::pid_t, libc::SIGKILL);
    }
}

#[cfg(windows)]
fn kill_group(pid: u32) {
    let _ = std::process::Command::new("taskkill")
        .args(["/T", "/F", "/PID", &pid.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;
#[cfg(windows)]
const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

fn launch_of(spec: &Spec) -> (String, Vec<String>) {
    if cfg!(windows) && BATCH_LAUNCHERS.contains(&spec.command.as_str()) {
        let shell = std::env::var("ComSpec").unwrap_or_else(|_| "cmd.exe".to_string());
        let mut args = vec!["/d".to_string(), "/s".to_string(), "/c".to_string(), spec.command.clone()];
        args.extend(spec.args.iter().cloned());
        return (shell, args);
    }
    (spec.command.clone(), spec.args.clone())
}

async fn request(runner: &Arc<Runner>, method: &str, params: Value, timeout_ms: u64) -> Reply {
    let (id, receiver) = {
        let mut state = runner.state.lock().unwrap();
        if !state.up {
            return Err(runner.not_running());
        }
        let id = state.next_id;
        state.next_id += 1;
        let (sender, receiver) = oneshot::channel();
        state.pending.insert(id, sender);
        let line = encode(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }));
        if let Some(outgoing) = &state.outgoing {
            let _ = outgoing.send(line);
        }
        (id, receiver)
    };
    match tokio::time::timeout(Duration::from_millis(timeout_ms), receiver).await {
        Ok(Ok(reply)) => reply,
        Ok(Err(_)) => Err(format!("{method} lost its reply")),
        Err(_) => {
            runner.state.lock().unwrap().pending.remove(&id);
            let _ = runner.notify(
                "notifications/cancelled",
                Some(json!({ "requestId": id, "reason": format!("timed out after {timeout_ms} ms") })),
            );
            Err(format!("{method} timed out after {timeout_ms} ms"))
        }
    }
}

async fn call_tool(runner: &Arc<Runner>, name: &str, args: Value) -> Value {
    if !runner.is_up() {
        if let Some(error) = connect(runner, true).await {
            return json!({ "ok": false, "error": format!("MCP server {} is not running: {error}", runner.spec.name) });
        }
    }
    let arguments = if args.is_null() { json!({}) } else { args };
    match request(runner, "tools/call", json!({ "name": name, "arguments": arguments }), runner.spec.timeout_ms).await {
        Ok(result) => outcome_of(&result),
        Err(error) => json!({ "ok": false, "error": error }),
    }
}

fn outcome_of(result: &Value) -> Value {
    let output = result
        .get("content")
        .and_then(Value::as_array)
        .map(|parts| parts.iter().map(part_text).collect::<Vec<_>>().join("\n"))
        .unwrap_or_default();
    let failed = result.get("isError") == Some(&Value::Bool(true));
    let mut outcome = Map::new();
    outcome.insert("ok".to_string(), Value::Bool(!failed));
    if failed {
        let error = if output.is_empty() { "the tool reported an error".to_string() } else { output.clone() };
        outcome.insert("error".to_string(), Value::String(error));
    }
    outcome.insert("output".to_string(), Value::String(output));
    if let Some(data) = result.get("structuredContent") {
        outcome.insert("data".to_string(), data.clone());
    }
    Value::Object(outcome)
}

fn part_text(part: &Value) -> String {
    let kind = part.get("type").and_then(Value::as_str).unwrap_or("undefined");
    if kind == "text" {
        return part.get("text").and_then(Value::as_str).unwrap_or("").to_string();
    }
    match part.get("mimeType").and_then(Value::as_str) {
        Some(mime) => format!("[{kind} {mime}]"),
        None => format!("[{kind}]"),
    }
}

fn describe(server: &str, tool: &Value) -> String {
    let text = ["description", "title", "name"]
        .iter()
        .filter_map(|key| tool.get(*key).filter(|value| !value.is_null()))
        .next()
        .map(value_text)
        .unwrap_or_else(|| "undefined".to_string());
    let short = if text.chars().count() > DESCRIPTION_CHARS {
        format!("{}...", text.chars().take(DESCRIPTION_CHARS).collect::<String>())
    } else {
        text
    };
    format!("{short} (MCP server {server}, tool {})", text_field(tool, "name"))
}

fn text_field(value: &Value, key: &str) -> String {
    value.get(key).map(value_text).unwrap_or_else(|| "undefined".to_string())
}

fn value_text(value: &Value) -> String {
    match value.as_str() {
        Some(text) => text.to_string(),
        None => value.to_string(),
    }
}

fn read_specs(layers: &[(PathBuf, bool)], workspace: &Path, problems: &mut Vec<String>) -> Vec<Spec> {
    let mut slots: Vec<(String, Option<Spec>)> = Vec::new();
    for (file, required) in layers {
        let text = match fs::read_to_string(file) {
            Ok(text) => text,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                if *required {
                    problems.push(format!("{} does not exist", file.display()));
                }
                continue;
            }
            Err(err) => {
                problems.push(format!("{} is not readable: {err}", file.display()));
                continue;
            }
        };
        let document: Value = match serde_json::from_str(&text) {
            Ok(document) => document,
            Err(err) => {
                problems.push(format!("{} is not valid JSON: {err}", file.display()));
                continue;
            }
        };
        let Some(servers) = document.get("mcpServers").and_then(Value::as_object) else {
            continue;
        };
        for (name, spec) in servers {
            let server = match server_of(name, spec, workspace) {
                Ok(server) => server,
                Err(problem) => {
                    problems.push(problem);
                    None
                }
            };
            match slots.iter_mut().find(|(key, _)| key == name) {
                Some(slot) => slot.1 = server,
                None => slots.push((name.clone(), server)),
            }
        }
    }
    slots.into_iter().filter_map(|(_, server)| server).collect()
}

fn server_of(name: &str, spec: &Value, workspace: &Path) -> Result<Option<Spec>, String> {
    if spec.get("disabled") == Some(&Value::Bool(true)) {
        return Ok(None);
    }
    let fail = |text: &str| format!("server {name}: {text}");
    if name.is_empty() {
        return Err(fail("the name must not be empty"));
    }
    if let Some(kind) = spec.get("type") {
        if kind.as_str() != Some("stdio") {
            return Err(fail(&format!("type {} is not supported, only stdio servers run", value_text(kind))));
        }
    }
    let command = match spec.get("command").and_then(Value::as_str) {
        Some(command) if !command.is_empty() => command,
        _ => return Err(fail("command must be a non-empty string")),
    };
    let cwd = match spec.get("cwd") {
        None => None,
        Some(cwd) => Some(cwd.as_str().ok_or_else(|| fail("cwd must be a string"))?),
    };
    let args = match defined(spec, "args") {
        None => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| item.as_str().map(str::to_string).ok_or_else(|| fail("args must be an array of strings")))
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => return Err(fail("args must be an array of strings")),
    };
    let env = match defined(spec, "env") {
        None => Vec::new(),
        Some(Value::Object(map)) => map
            .iter()
            .map(|(key, value)| value.as_str().map(|value| (key.clone(), value.to_string())).ok_or_else(|| fail("env must be an object of strings")))
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => return Err(fail("env must be an object of strings")),
    };
    let ignore = match defined(spec, "ignore") {
        None => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| match item.as_str().map(str::trim) {
                Some(line) if !line.is_empty() => Ok(line.to_string()),
                _ => Err(fail("ignore must be an array of non-empty strings")),
            })
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => return Err(fail("ignore must be an array of non-empty strings")),
    };
    if ignore.iter().any(|line| is_absolute(line)) {
        return Err(fail("ignore lines must be relative paths, not absolute"));
    }
    if ignore.iter().any(|line| line.split(['\\', '/']).any(|segment| segment == "..")) {
        return Err(fail("ignore lines must not contain a .. segment"));
    }
    let timeout_ms = match defined(spec, "timeoutMs") {
        None => Some(CALL_MS),
        Some(value) => positive(value),
    };
    let startup_ms = match defined(spec, "startupTimeoutMs") {
        None => Some(STARTUP_MS),
        Some(value) => positive(value),
    };
    let (Some(timeout_ms), Some(startup_ms)) = (timeout_ms, startup_ms) else {
        return Err(fail("timeoutMs and startupTimeoutMs must be positive integers"));
    };
    let deny = match defined(spec, "deny") {
        None => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| match item.as_str() {
                Some(tool) if !tool.is_empty() => Ok(tool.to_string()),
                _ => Err(fail("deny must be an array of non-empty tool names")),
            })
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => return Err(fail("deny must be an array of non-empty tool names")),
    };

    let mut missing = Vec::new();
    let command = expand(command, &mut missing);
    let args: Vec<String> = args.iter().map(|arg| expand(arg, &mut missing)).collect();
    let env: Vec<(String, String)> = env.iter().map(|(key, value)| (key.clone(), expand(value, &mut missing))).collect();
    let cwd = match cwd {
        None => workspace.to_path_buf(),
        Some(cwd) => workspace.join(expand(cwd, &mut missing)),
    };
    if !missing.is_empty() {
        return Err(fail(&format!("environment variable {} is not set", missing.join(", "))));
    }
    Ok(Some(Spec {
        name: name.to_string(),
        command,
        args,
        env,
        cwd,
        timeout_ms,
        startup_ms,
        deny,
        ignore: ignore.into_iter().map(|line| line.trim().to_string()).collect(),
        workspace: workspace.to_path_buf(),
    }))
}

fn defined<'a>(spec: &'a Value, key: &str) -> Option<&'a Value> {
    spec.get(key).filter(|value| !value.is_null())
}

fn positive(value: &Value) -> Option<u64> {
    let number = match value.as_u64() {
        Some(number) => number,
        None => {
            let float = value.as_f64()?;
            if float.fract() != 0.0 || float < 1.0 {
                return None;
            }
            float as u64
        }
    };
    (number >= 1).then_some(number)
}

fn is_absolute(line: &str) -> bool {
    let trimmed = line.trim();
    let bytes = trimmed.as_bytes();
    matches!(bytes.first(), Some(b'/') | Some(b'\\'))
        || (bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':')
}

fn expand(text: &str, missing: &mut Vec<String>) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find("${") {
        out.push_str(&rest[..at]);
        let after = &rest[at + 2..];
        match variable_length(after) {
            Some(length) => {
                let name = &after[..length];
                match std::env::var(name) {
                    Ok(value) => out.push_str(&value),
                    Err(_) => {
                        if !missing.iter().any(|known| known == name) {
                            missing.push(name.to_string());
                        }
                        out.push_str(&rest[at..at + 2 + length + 1]);
                    }
                }
                rest = &after[length + 1..];
            }
            None => {
                out.push_str("${");
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

fn variable_length(after: &str) -> Option<usize> {
    let bytes = after.as_bytes();
    let first = *bytes.first()?;
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return None;
    }
    let length = bytes.iter().take_while(|byte| byte.is_ascii_alphanumeric() || **byte == b'_').count();
    (bytes.get(length) == Some(&b'}')).then_some(length)
}

fn depth_from_env() -> u64 {
    std::env::var(DEPTH_VARIABLE)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

fn add_ignore_lines(file: &Path, wanted: &[String]) -> std::io::Result<Vec<String>> {
    let text = match fs::read_to_string(file) {
        Ok(text) => text,
        Err(err) if err.kind() == ErrorKind::NotFound => String::new(),
        Err(err) => return Err(err),
    };
    let present: HashSet<&str> = text.split('\n').map(str::trim).collect();
    let mut added: Vec<String> = Vec::new();
    for line in wanted {
        if !present.contains(line.as_str()) && !added.contains(line) {
            added.push(line.clone());
        }
    }
    if added.is_empty() {
        return Ok(added);
    }
    let eol = if text.contains("\r\n") { "\r\n" } else { "\n" };
    let mut out = String::new();
    if !text.is_empty() && !text.ends_with('\n') {
        out.push_str(eol);
    }
    for line in &added {
        out.push_str(line);
        out.push_str(eol);
    }
    OpenOptions::new().create(true).append(true).open(file)?.write_all(out.as_bytes())?;
    Ok(added)
}

fn encode(message: &Value) -> String {
    let mut line = message.to_string();
    line.push('\n');
    line
}
