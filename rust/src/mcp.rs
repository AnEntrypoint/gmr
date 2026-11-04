use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, watch, Semaphore};

use crate::exec::Jobs;
use crate::locks::Locks;
use crate::mcp_client::Hub;
use crate::notices::Notices;
use crate::resources::{self, Subscriptions};
use crate::run::Run;
use crate::runcode::{DEFAULT_TIMEOUT_MS, MAX_TIMEOUT_MS};
use crate::skills;
use crate::types::{Ctx, WorkerOpts};

pub const SUPPORTED_VERSIONS: [&str; 2] = ["2025-11-25", "2025-06-18"];
pub const RUN_META: &str = "com.anentrypoint.gmnew/run";
pub const RUN_SEQ: &str = "com.anentrypoint.gmnew/seq";
pub const SERVER_NAME: &str = "gm";
const SERVER_TITLE: &str = "gm-new run worker";
const SERVER_VERSION: &str = "0.1.0";
pub const LOGGED_REPLY_CHARS: usize = 1000;
pub const READ_ONLY_TOOLS: &[&str] = &[
    "next_context", "read_file", "list_dir", "stat_file", "search", "wiki_search", "grep", "callers", "impact", "query_log", "job_status",
    "git_status", "git_log", "git_diff", "git_show", "ci_status", "prd_list", "unknown_list", "review_join",
];
const LOGGED_OUTPUT_TOOLS: &[&str] = &["query_log", "job_status", "run_shell", "run_code"];
const OPEN_WORLD: &[&str] = &["fetch", "git_fetch", "git_pull", "git_push", "ci_status"];
const RECORDS_ONLY: &[&str] = &["next_context", "finish"];
const CODE_SURFACE: [&str; 3] = ["next_context", "run_code", "finish"];
const RUN_CODE_ARGS: [&str; 3] = ["body", "timeoutMs", "format"];
const TRACE_KEYS: [&str; 3] = ["traceparent", "tracestate", "baggage"];
const PROGRESS_MS: u64 = 5000;
const REPLY_MEMORY: usize = 256;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

const CODE_INSTRUCTIONS: &str = "One tool does the work: run_code. Its body is JavaScript in the project, and each verb is a function on gm, for example await gm.read_file({ path }). The verb list is in run_code's description, gm.verbs() returns each verb's details, and finish ends the run and is not a verb. Start each step with next_context.";
const RESOURCES_POINTER: &str = "Resources: skill://<name>/<file> reads a served skill file. run://<run>/log reads a run's event log, and run://<run>/witness its witness; resources/templates/list names the templates. Subscribe to a skill file with resources/subscribe to be told when it changes on disk. Log messages follow logging/setLevel, and completion/complete suggests skill names and run ids.";
const SKILL_POINTER: &str = "The gm skill holds the rules of a run, the routes for pending events, and the way to hand work to a child session. Read it when a run starts, and read it again after a compaction. Read skill://gm/SKILL.md with ReadMcpResourceTool (server gm), or run the gm prompt (mcp__gm__gm). The host's Skill tool does not list it, so do not call it with the Skill tool. The gm-continue skill plans the open rows of gm-new and hands them to the gm skill. Read skill://gm-continue/SKILL.md with ReadMcpResourceTool (server gm), or run the gm-continue prompt (mcp__gm__gm-continue). The host's Skill tool does not list it, so do not call it with the Skill tool.";

const NEXT_CONTEXT_DESCRIPTION: &str = "Begin a step. Returns rules, task, witness requirement, the events new since the last step in full, the older ones in compact form within two horizons, and prefixSha. pending lists the events that need you since your last step, each once, with an id. Delivery clears an event, so no later step or wait lists it again. Call it at the start of each step. query_log reaches every event. waitMs, when set, holds this call until an event is pending or that many milliseconds pass, at most 300000. child: true makes the call a read by a subagent on this connection: it takes no step, claims no pending event, and its reply has role child and the owning run. run, with child, names that owning run, and another run is refused.";
const FINISH_DESCRIPTION: &str = "End the run. witness is one string that quotes the evidence you observed. gaps optionally lists, one string each, what the run did not check; the end event logs them. finish is refused while a background run is running, and stopBackground true kills it first. finish is refused while a PRD or question row is open, and names the rows; stopRows true ends the run anyway, with each open row listed as a gap in the end event.";
const RUN_CODE_HEAD: &str = "Run a JavaScript body in the project. The body is an ES module when format is esm and CommonJS when format is cjs. Left out, format follows the project's package.json type (type module is esm, anything else is cjs). A CommonJS body loads with require() and cannot use a static import. Static imports (single or multi-line) load ES modules, and require() loads CommonJS; each project file loads in its own format (.mjs is ES module, .cjs is CommonJS, .js follows the package.json type), and each file is read from disk when the call runs. Relative paths resolve against the project root. Paths may point inside or outside the workspace, for the body's own require and import and for the file verbs alike; an outside path should be absolute. A non-literal import() or require() is refused. The body receives gm, with one method per verb: await gm.<name>(args), and gm.project is the project root. The verbs are: ";
const RUN_CODE_TAIL: &str = ". gm.verbs() returns each verb's description and input schema as data. A failed call returns {ok:false, error} and does not stop the body. The body's return value comes back as JSON. A body still running after timeoutMs (default 180000, at most 300000) becomes a background run: the call returns its job id at once, job_status with waitMs returns its value, and finish is refused while it runs unless stopBackground is true. One step is one body: read the inputs in one Promise.all, decide in code, act with the independent writes in one Promise.all, and verify in the same body. The ten most-used verbs: read_file({ path, offset?, limit? }), run_shell({ command, timeoutMs? }), write_file({ path, content }), query_log({ js?, kind?, fromMono?, toMono?, after?, last? }), search({ query, k? }), fetch({ url, timeoutMs? }), job_start({ job, command, timeoutMs? }), job_status({ job, waitMs?, offset? }), callers({ name, limit? }), grep({ pattern, root?, mode?, ignoreCase?, limit?, offset? }). finish is not a verb: end the run with the finish tool. The reply names its own action in actionSeq, the seq of the run_code action event in the log. A body's console output is not returned and is not logged. An argument this tool does not take is refused by name.";

const RULE_TEXT: [(u8, &str); 3] = [(2, "call next_context at the start of every step"), (3, "verify before you finish"), (4, "finish needs a witness")];

pub fn capabilities() -> Value {
    json!({
        "tools": { "listChanged": true },
        "prompts": { "listChanged": true },
        "resources": { "listChanged": true, "subscribe": true },
        "logging": {},
        "completions": {},
        "extensions": { "io.modelcontextprotocol/skills": {} }
    })
}

pub fn instructions() -> String {
    format!("{CODE_INSTRUCTIONS}\n\n{RESOURCES_POINTER}\n\n{SKILL_POINTER}")
}

pub fn is_read_only(tool: &str) -> bool {
    READ_ONLY_TOOLS.contains(&tool)
}

fn char_cut(text: &str, chars: usize) -> Option<(usize, usize)> {
    let cut = text.char_indices().nth(chars).map(|(at, _)| at)?;
    Some((cut, text.chars().count()))
}

pub fn logged_for(tool: &str, result: &Value) -> Value {
    if LOGGED_OUTPUT_TOOLS.contains(&tool) {
        if let Some(output) = result.get("output").and_then(Value::as_str) {
            if let Some((cut, total)) = char_cut(output, LOGGED_REPLY_CHARS) {
                let failed = result.get("ok") == Some(&Value::Bool(false))
                    || result.get("exitCode").and_then(Value::as_f64).is_some_and(|c| c != 0.0);
                let mut map = Map::new();
                if let Some(obj) = result.as_object() {
                    for (k, v) in obj {
                        if k != "output" {
                            map.insert(k.clone(), v.clone());
                        }
                    }
                }
                map.insert("output".into(), Value::String(output[..cut].to_string()));
                map.insert("outputChars".into(), json!(total));
                map.insert("outputBytes".into(), json!(output.len()));
                map.insert("outputDigest".into(), crate::feed::output_digest(output, failed));
                return Value::Object(map);
            }
        }
    }
    result.clone()
}

fn title_of(name: &str) -> String {
    let words = name.replace('_', " ");
    let mut chars = words.chars();
    match chars.next() {
        Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

fn annotated(name: &str, description: String, input_schema: Value, output_schema: Value) -> Value {
    let read_only = is_read_only(name) && name != "next_context";
    json!({
        "name": name,
        "description": description,
        "inputSchema": input_schema,
        "outputSchema": output_schema,
        "title": title_of(name),
        "annotations": {
            "readOnlyHint": read_only,
            "destructiveHint": !read_only && !RECORDS_ONLY.contains(&name),
            "openWorldHint": OPEN_WORLD.contains(&name)
        }
    })
}

pub fn verb_names(hub: &Hub) -> Vec<String> {
    crate::verbs::names(hub)
}

pub fn served_tools(hub: &Hub) -> Vec<Value> {
    let text = json!({ "type": "string" });
    let number = json!({ "type": "number" });
    let boolean = json!({ "type": "boolean" });
    let flag = json!({ "type": "boolean" });
    let text_out = json!({ "type": "string" });
    let number_out = json!({ "type": "number" });
    vec![
        annotated(
            "next_context",
            NEXT_CONTEXT_DESCRIPTION.to_string(),
            json!({ "type": "object", "properties": { "waitMs": number, "child": boolean, "run": text }, "required": [] }),
            json!({
                "type": "object",
                "properties": { "ok": flag, "refused": flag, "error": text_out, "step": number_out, "run": text_out, "stepsLeft": number_out, "maxSteps": number_out, "total": number_out, "prefixSha": text_out, "actionSeq": number_out },
                "required": ["ok"]
            }),
        ),
        annotated(
            "finish",
            FINISH_DESCRIPTION.to_string(),
            json!({ "type": "object", "properties": { "witness": text, "gaps": { "type": "array", "items": text }, "stopBackground": boolean, "stopRows": boolean }, "required": ["witness"] }),
            json!({ "type": "object", "properties": { "ok": flag, "refused": flag, "error": text_out, "finished": flag, "actionSeq": number_out }, "required": ["ok"] }),
        ),
        annotated(
            "run_code",
            format!("{RUN_CODE_HEAD}{}{RUN_CODE_TAIL}", verb_names(hub).join(", ")),
            json!({ "type": "object", "properties": { "body": text, "timeoutMs": number, "format": { "type": "string", "enum": ["esm", "cjs"] } }, "required": ["body"] }),
            json!({
                "type": "object",
                "properties": { "ok": flag, "refused": flag, "error": text_out, "truncated": flag, "background": text_out, "state": text_out, "actionSeq": number_out },
                "required": ["ok"]
            }),
        ),
    ]
}

#[derive(Clone)]
struct RpcErr {
    code: i64,
    message: String,
    data: Option<Value>,
}

impl RpcErr {
    fn new(code: i64, message: String) -> Self {
        RpcErr { code, message, data: None }
    }
}

type Answer = Option<Result<Value, RpcErr>>;

#[derive(Default)]
struct Replies {
    map: HashMap<String, watch::Receiver<Answer>>,
    order: VecDeque<String>,
}

#[derive(Default)]
struct Session {
    steps: u64,
    started: bool,
    step_calls: u64,
    ended: Option<&'static str>,
    step_limited: bool,
    rules: Vec<(u8, String)>,
    gm_loads: u64,
    task_set: bool,
    ended_task: Option<String>,
}

struct Server {
    workspace: PathBuf,
    max_steps: u64,
    task: Option<String>,
    ctx: RwLock<Arc<Ctx>>,
    session: Mutex<Session>,
    notices: Arc<Notices>,
    subscriptions: Arc<Subscriptions>,
    out: mpsc::UnboundedSender<Option<String>>,
    ready: watch::Receiver<bool>,
    slots: Arc<Semaphore>,
    max_inflight: usize,
    replies: Mutex<Replies>,
    inflight: Mutex<HashMap<String, String>>,
    actions: Mutex<HashMap<String, u64>>,
}

struct AbortOnDrop(Option<tokio::task::JoinHandle<()>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

pub fn js_text(value: Option<&Value>) -> String {
    match value {
        None => "undefined".to_string(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

fn is_ok(value: &Value) -> bool {
    value.get("ok") == Some(&Value::Bool(true))
}

fn pick(value: &Value, keys: &[&str]) -> Value {
    let mut map = Map::new();
    for key in keys {
        if let Some(v) = value.get(*key) {
            map.insert((*key).to_string(), v.clone());
        }
    }
    Value::Object(map)
}

fn error_line(id: Value, err: &RpcErr) -> String {
    let mut error = json!({ "code": err.code, "message": err.message });
    if let Some(data) = &err.data {
        error["data"] = data.clone();
    }
    json!({ "jsonrpc": "2.0", "id": id, "error": error }).to_string()
}

fn trace_of(meta: &Value) -> Map<String, Value> {
    let mut map = Map::new();
    for key in TRACE_KEYS {
        if let Some(v) = meta.get(key).filter(|v| v.is_string()) {
            map.insert(key.to_string(), v.clone());
        }
    }
    map
}

fn mcp_layers(workspace: &Path, extra: Option<&Path>) -> Vec<(PathBuf, bool)> {
    let mut layers = Vec::new();
    if let Some(home) = skills::home() {
        layers.push((home.join("defaults").join("mcp.json"), true));
    }
    layers.push((workspace.join(".gm-new").join("mcp.json"), false));
    if let Some(file) = extra {
        layers.push((std::path::absolute(file).unwrap_or_else(|_| file.to_path_buf()), true));
    }
    layers
}

fn log_tools(run: &Run, hub: &Hub) {
    let served = served_tools(hub);
    let rows: Vec<(String, String, String)> = served
        .iter()
        .map(|tool| (js_text(tool.get("name")), tool["inputSchema"].to_string(), js_text(tool.get("description"))))
        .collect();
    let refs: Vec<(&str, &str, &str)> = rows.iter().map(|(n, schema, d)| (n.as_str(), schema.as_str(), d.as_str())).collect();
    run.set_tools_text(crate::feed::render_tools(&refs));
    let sha = skills::sha256_hex(Value::Array(served).to_string().as_bytes());
    run.event("tools", json!({ "sha": sha, "count": CODE_SURFACE.len() }));
}

impl Server {
    fn ctx(&self) -> Arc<Ctx> {
        self.ctx.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn send(&self, line: String) {
        let _ = self.out.send(Some(line));
    }

    async fn handle_line(self: &Arc<Self>, line: String) -> Option<String> {
        let message: Value = match serde_json::from_str(&line) {
            Ok(m) => m,
            Err(e) => {
                self.ctx().run.event("parse_error", json!({ "error": e.to_string() }));
                return Some(error_line(Value::Null, &RpcErr::new(-32700, "Parse error".into())));
            }
        };
        drop(line);
        let Some(method) = message.as_object().and_then(|o| o.get("method")).and_then(Value::as_str).map(str::to_string) else {
            self.ctx().run.event("error", json!({ "code": -32600, "message": "Invalid Request" }));
            return Some(error_line(Value::Null, &RpcErr::new(-32600, "Invalid Request".into())));
        };
        let id = message.get("id").cloned();
        let params = message.get("params").cloned().unwrap_or(Value::Null);
        drop(message);
        let key = match (&id, method.as_str()) {
            (Some(id), "tools/call") => Some(id.to_string()),
            _ => None,
        };
        let outcome = match &key {
            Some(key) => self.call_once(key, &method, params, id.clone().unwrap_or(Value::Null)).await,
            None => self.route(&method, params, id.clone().unwrap_or(Value::Null)).await,
        };
        let id = id?;
        match outcome {
            Ok(result) => Some(json!({ "jsonrpc": "2.0", "id": id, "result": result.unwrap_or_else(|| json!({})) }).to_string()),
            Err(err) => {
                self.ctx().run.event("error", json!({ "method": method, "code": err.code, "message": err.message }));
                Some(error_line(id, &err))
            }
        }
    }

    async fn call_once(self: &Arc<Self>, key: &str, method: &str, params: Value, id: Value) -> Result<Option<Value>, RpcErr> {
        let earlier = lock(&self.replies).map.get(key).cloned();
        if let Some(mut rx) = earlier {
            self.ctx().run.event("duplicate", json!({ "rpcId": id, "tool": params.get("name") }));
            let answer = rx.wait_for(Option::is_some).await.map(|a| a.clone()).ok().flatten();
            return match answer {
                Some(result) => result.map(Some),
                None => Err(RpcErr::new(-32603, "the first call with this id did not answer".into())),
            };
        }
        let (tx, rx) = watch::channel::<Answer>(None);
        {
            let mut replies = lock(&self.replies);
            replies.map.insert(key.to_string(), rx);
            replies.order.push_back(key.to_string());
            while replies.order.len() > REPLY_MEMORY {
                if let Some(old) = replies.order.pop_front() {
                    replies.map.remove(&old);
                }
            }
        }
        let outcome = self.route(method, params, id).await;
        let _ = tx.send(Some(outcome.clone().map(|v| v.unwrap_or_else(|| json!({})))));
        outcome
    }

    async fn route(self: &Arc<Self>, method: &str, params: Value, id: Value) -> Result<Option<Value>, RpcErr> {
        match method {
            "initialize" => return Ok(Some(initialize(&params))),
            "ping" => return Ok(Some(json!({}))),
            _ => {}
        }
        let mut ready = self.ready.clone();
        let _ = ready.wait_for(|r| *r).await;
        let internal = |e: String| RpcErr::new(-32603, e);
        let value = match method {
            "tools/list" => json!({ "tools": served_tools(&self.ctx().hub) }),
            "tools/call" => self.call_tool(params, id).await?,
            "prompts/list" => {
                let mut prompts = Vec::new();
                for name in skills::SERVED_SKILLS {
                    prompts.push(prompt_of(name).map_err(internal)?);
                }
                json!({ "prompts": prompts })
            }
            "prompts/get" => self.get_prompt(&params)?,
            "resources/list" => json!({ "resources": self.resource_list().map_err(internal)? }),
            "resources/read" => self.read_resource(&params)?,
            "resources/subscribe" => self.subscribe(&params)?,
            "resources/unsubscribe" => {
                self.subscriptions.unsubscribe(params.get("uri").and_then(Value::as_str).unwrap_or_default());
                json!({})
            }
            "completion/complete" => resources::completion(&params, &self.workspace.join("runs")),
            "logging/setLevel" => self.notices.set_level(&params).map_err(|e| RpcErr::new(-32602, e))?,
            "skills/list" => json!({ "resultType": "complete", "skills": skills::entries().map_err(internal)? }),
            "skills/get" => {
                let uri = params.get("uri");
                let entry = uri.and_then(Value::as_str).and_then(skills::entry_of);
                match entry {
                    Some(skill) => json!({ "resultType": "complete", "skill": skill }),
                    None => return Err(RpcErr::new(-32602, format!("unknown skill: {}", js_text(uri)))),
                }
            }
            "resources/templates/list" => json!({ "resourceTemplates": resources::templates() }),
            "core/reload" => json!({ "ok": true, "generation": 0, "reloaded": [] }),
            "notifications/cancelled" => {
                self.cancel(params.get("requestId").cloned().unwrap_or(Value::Null));
                return Ok(None);
            }
            m if m.starts_with("notifications/") => return Ok(None),
            m => return Err(RpcErr::new(-32601, format!("method not found: {m}"))),
        };
        Ok(Some(value))
    }

    fn cancel(&self, request_id: Value) {
        let key = request_id.to_string();
        let tool = lock(&self.inflight).get(&key).cloned();
        let run = self.ctx().run.clone();
        match tool {
            None => run.event("cancel-ignored", json!({ "requestId": request_id, "reason": "no call in flight with that id" })),
            Some(tool) => {
                let reason = if is_read_only(&tool) { "read-only verb" } else { "side-effecting verb" };
                run.event("cancel-ignored", json!({ "requestId": request_id, "tool": tool, "reason": reason }))
            }
        };
    }

    fn get_prompt(&self, params: &Value) -> Result<Value, RpcErr> {
        let name = params.get("name").and_then(Value::as_str).filter(|n| skills::SERVED_SKILLS.contains(n));
        let Some(name) = name else {
            return Err(RpcErr::new(-32602, format!("unknown prompt: {}", js_text(params.get("name")))));
        };
        let skill = skills::skill(name).map_err(|e| RpcErr::new(-32603, e))?;
        let ctx = self.ctx();
        let description = skill.field("description").to_string();
        let result = |text: String| json!({ "description": description, "messages": [{ "role": "user", "content": { "type": "text", "text": text } }] });
        if name != "gm" {
            ctx.run.event("skill-loaded", json!({ "via": "prompt", "skill": name, "cleared": [] }));
            return Ok(result(skill.body.clone()));
        }
        let given = params.pointer("/arguments/task").and_then(Value::as_str).map(str::trim).unwrap_or("");
        {
            let mut s = lock(&self.session);
            let cleared: Vec<u8> = s.rules.iter().map(|(rule, _)| *rule).collect();
            ctx.run.event("skill-loaded", json!({ "via": "prompt", "skill": "gm", "cleared": cleared }));
            clear_rules(&mut s, &ctx.run);
            if !given.is_empty() && s.ended.is_some() {
                s.ended_task = Some(given.to_string());
            } else if !given.is_empty() && !s.task_set {
                s.task_set = true;
                ctx.run.set_task(given, "session prompt");
            }
        }
        Ok(result(if given.is_empty() { skill.body.clone() } else { format!("{}\n\nTask: {given}", skill.body) }))
    }

    fn resource_list(&self) -> Result<Vec<Value>, String> {
        let ctx = self.ctx();
        let mut list = skills::list()?;
        list.extend(resources::run_resources(&ctx.run.dir.join(crate::log::LOG_FILE), &ctx.run.id));
        Ok(list)
    }

    fn subscribe(&self, params: &Value) -> Result<Value, RpcErr> {
        let uri = params.get("uri").and_then(Value::as_str);
        match uri.and_then(resources::digest_of) {
            Some(digest) => {
                self.subscriptions.subscribe(uri.unwrap_or_default(), digest);
                Ok(json!({}))
            }
            None => Err(RpcErr { code: -32002, message: format!("resource not found: {}", js_text(params.get("uri"))), data: Some(json!({ "uri": params.get("uri") })) }),
        }
    }

    fn read_resource(&self, params: &Value) -> Result<Value, RpcErr> {
        if let Some(run) = resources::read_run(&self.workspace.join("runs"), params.get("uri").and_then(Value::as_str).unwrap_or_default()) {
            return Ok(json!({ "contents": [run] }));
        }
        let uri = params.get("uri");
        let Some((skill, text, mime)) = uri.and_then(Value::as_str).and_then(skills::file_of) else {
            let shown = js_text(uri);
            return Err(RpcErr { code: -32002, message: format!("resource not found: {shown}"), data: Some(json!({ "uri": uri })) });
        };
        let uri = uri.and_then(Value::as_str).unwrap_or_default();
        let is_gm = uri == skills::GM_SKILL_URI;
        let ctx = self.ctx();
        {
            let mut s = lock(&self.session);
            let cleared: Vec<u8> = if is_gm { s.rules.iter().map(|(rule, _)| *rule).collect() } else { Vec::new() };
            ctx.run.event("skill-loaded", json!({ "via": "resource", "skill": skill.name, "uri": uri, "cleared": cleared }));
            if is_gm {
                s.gm_loads = skills::gm_loads();
                clear_rules(&mut s, &ctx.run);
            }
        }
        Ok(json!({ "contents": [{ "uri": uri, "mimeType": mime, "text": text }] }))
    }

    async fn call_tool(self: &Arc<Self>, mut params: Value, id: Value) -> Result<Value, RpcErr> {
        let Some(name) = params.get("name").and_then(Value::as_str).map(str::to_string) else {
            return Err(RpcErr::new(-32602, "tools/call needs a string name".into()));
        };
        let args = match params.get_mut("arguments").map(Value::take) {
            None | Some(Value::Null) => json!({}),
            Some(v @ Value::Object(_)) => v,
            Some(_) => return Err(RpcErr::new(-32602, "tools/call arguments must be an object".into())),
        };
        if !CODE_SURFACE.contains(&name.as_str()) {
            return Err(RpcErr::new(
                -32602,
                format!("Unknown tool: {name}; the code surface serves next_context, run_code and finish; call a verb as gm.<name>(args) inside run_code"),
            ));
        }
        {
            let mut s = lock(&self.session);
            if !s.started && s.ended.is_none() && name != "next_context" && name != "finish" {
                note_rule(&mut s, &self.ctx().run, 2, format!("{name} was called before the first next_context of the run"));
            }
        }
        let meta = params.get("_meta").cloned().unwrap_or(Value::Null);
        let trace = trace_of(&meta);
        let key = id.to_string();
        lock(&self.inflight).insert(key.clone(), name.clone());
        let progress = self.progress_for(&name, meta.get("progressToken"));
        let (mut out, run_id) = {
            let _permit = self.slots.clone().acquire_owned().await.ok();
            self.invoke(&name, args, id, trace).await
        };
        drop(progress);
        lock(&self.inflight).remove(&key);
        let notice = {
            let mut s = lock(&self.session);
            let ctx = self.ctx();
            watch_call(&mut s, &ctx.run, &name, &out);
            rules_notice(&s)
        };
        if let (Some(notice), Some(obj)) = (notice, out.as_object_mut()) {
            obj.insert("rulesNotice".into(), Value::String(notice));
        }
        let is_error = !is_ok(&out);
        let mut meta = Map::new();
        meta.insert(RUN_META.into(), Value::String(run_id));
        if let Some(seq) = lock(&self.actions).remove(&key) {
            meta.insert(RUN_SEQ.into(), json!(seq));
        }
        Ok(json!({ "content": [{ "type": "text", "text": out.to_string() }], "structuredContent": out, "isError": is_error, "_meta": meta }))
    }

    fn progress_for(&self, name: &str, token: Option<&Value>) -> AbortOnDrop {
        let token = match token {
            Some(t) if name == "run_code" && (t.is_string() || t.is_i64() || t.is_u64()) => t.clone(),
            _ => return AbortOnDrop(None),
        };
        let out = self.out.clone();
        let handle = tokio::spawn(async move {
            let started = Instant::now();
            let mut last: u64 = 0;
            loop {
                tokio::time::sleep(Duration::from_millis(PROGRESS_MS)).await;
                last = (last + 1).max(started.elapsed().as_secs());
                let note = json!({ "jsonrpc": "2.0", "method": "notifications/progress", "params": { "progressToken": token, "progress": last, "message": format!("running {last} s") } });
                if out.send(Some(note.to_string())).is_err() {
                    return;
                }
            }
        });
        AbortOnDrop(Some(handle))
    }

    fn reopen(&self) -> Result<(), String> {
        let old = self.ctx();
        let task = {
            let s = lock(&self.session);
            s.ended_task.clone().or_else(|| self.task.clone()).or_else(|| Some(old.run.task()).filter(|t| !t.is_empty()))
        };
        let run = Arc::new(Run::open(&self.workspace, None, task.as_deref())?);
        self.notices.hosted(&run);
        old.jobs.set_dir(run.dir.clone());
        old.hub.attach(run.clone());
        log_tools(&run, &old.hub);
        let fresh = Arc::new(Ctx { workspace: self.workspace.clone(), run, jobs: old.jobs.clone(), locks: old.locks.clone(), hub: old.hub.clone() });
        *self.ctx.write().unwrap_or_else(|e| e.into_inner()) = fresh;
        *lock(&self.session) = Session { task_set: task.is_some(), gm_loads: skills::gm_loads(), ..Session::default() };
        Ok(())
    }

    fn ended_reason(&self, s: &Session, run_id: &str, child: bool) -> String {
        if s.step_limited {
            return format!("step limit {} reached; a restart of the server opens a new run", self.max_steps);
        }
        match s.ended {
            Some("closed") => "stdin closed before the call started".to_string(),
            Some("restarting") => "the server is restarting; repeat the call".to_string(),
            _ if child => format!("run {run_id} has ended; the session that opened it calls next_context to open the next run"),
            _ => format!("run {run_id} has ended; call next_context to open a new run"),
        }
    }

    async fn invoke(self: &Arc<Self>, name: &str, args: Value, rpc_id: Value, trace: Map<String, Value>) -> (Value, String) {
        let child = name == "next_context" && args.get("child") == Some(&Value::Bool(true));
        let reopen = {
            let s = lock(&self.session);
            name == "next_context" && !child && matches!(s.ended, Some("finished") | Some("max_steps")) && !s.step_limited
        };
        if reopen {
            if let Err(e) = self.reopen() {
                let ctx = self.ctx();
                let out = self.refuse(&ctx, name, args, &format!("a new run did not open: {e}"), &rpc_id, &trace);
                return (out, ctx.run.id.clone());
            }
        }
        let ctx = self.ctx();
        let run_id = ctx.run.id.clone();
        let refusal = {
            let s = lock(&self.session);
            s.ended.map(|_| self.ended_reason(&s, &run_id, child))
        };
        if let Some(reason) = refusal {
            return (self.refuse(&ctx, name, args, &reason, &rpc_id, &trace), run_id);
        }
        let out = match name {
            "run_code" => self.run_code(&ctx, args, &rpc_id, &trace).await,
            "next_context" if child => self.child_context(&ctx, args, &rpc_id, &trace).await,
            "next_context" => self.next_context(&ctx, args, &rpc_id, &trace).await,
            _ => self.finish(&ctx, args, &rpc_id, &trace).await,
        };
        (out, run_id)
    }

    async fn run_code(&self, ctx: &Arc<Ctx>, args: Value, rpc_id: &Value, trace: &Map<String, Value>) -> Value {
        let unknown: Vec<&str> = args.as_object().map(|o| o.keys().map(String::as_str).filter(|k| !RUN_CODE_ARGS.contains(k)).collect()).unwrap_or_default();
        if !unknown.is_empty() {
            let reason = format!("unknown argument {}; run_code takes {}", unknown.join(", "), RUN_CODE_ARGS.join(", "));
            return self.refuse(ctx, "run_code", args, &reason, rpc_id, trace);
        }
        let body = args.get("body").and_then(Value::as_str).map(str::to_string);
        let format = args.get("format").and_then(Value::as_str).map(str::to_string);
        let timeout_ms = args.get("timeoutMs").and_then(Value::as_f64).filter(|t| t.is_finite() && *t > 0.0).map_or(DEFAULT_TIMEOUT_MS, |t| (t as u64).min(MAX_TIMEOUT_MS));
        let body_ctx = ctx.clone();
        let work = async move {
            match body {
                None => json!({ "ok": false, "error": "run_code needs a string body" }),
                Some(body) => match crate::runcode::run_body(body_ctx, body, format, timeout_ms).await {
                    Ok(value) => value,
                    Err(error) => json!({ "ok": false, "error": error }),
                },
            }
        };
        self.perform(ctx, "run_code", args, rpc_id, trace, work, |r| logged_for("run_code", r)).await
    }

    async fn child_context(&self, ctx: &Arc<Ctx>, args: Value, rpc_id: &Value, trace: &Map<String, Value>) -> Value {
        let started = lock(&self.session).started;
        if !started {
            return self.refuse(ctx, "child_context", args, "no step has started on this connection; the session that owns the run calls next_context first", rpc_id, trace);
        }
        if let Some(asked) = args.get("run").filter(|r| r.as_str() != Some(ctx.run.id.as_str())) {
            let reason = format!("this connection's run is {}, not {}", ctx.run.id, js_text(Some(asked)));
            return self.refuse(ctx, "child_context", args, &reason, rpc_id, trace);
        }
        let work = feed_call(ctx.clone(), args.clone());
        self.perform(ctx, "child_context", args, rpc_id, trace, work, |r| pick(r, &["ok", "error", "role", "run"])).await
    }

    async fn next_context(&self, ctx: &Arc<Ctx>, args: Value, rpc_id: &Value, trace: &Map<String, Value>) -> Value {
        let limited = {
            let mut s = lock(&self.session);
            if s.steps >= self.max_steps {
                s.step_limited = true;
                s.ended = Some("max_steps");
                Some(s.steps)
            } else {
                s.steps += 1;
                s.started = true;
                s.step_calls = 0;
                None
            }
        };
        if let Some(steps) = limited {
            let reason = format!("step limit {} reached; a restart of the server opens a new run", self.max_steps);
            let out = self.refuse(ctx, "next_context", json!({}), &reason, rpc_id, trace);
            ctx.jobs.kill_all();
            ctx.run.event("end", json!({ "status": "max_steps", "steps": steps, "witness": Value::Null }));
            return out;
        }
        ctx.run.hold(crate::feed::wait_ms_of(&args)).await;
        let work = feed_call(ctx.clone(), args);
        let mut out = self.perform(ctx, "next_context", json!({}), rpc_id, trace, work, crate::feed::digest).await;
        let open = crate::runcode::jobs_open();
        if let (false, Some(obj)) = (open.is_empty(), out.as_object_mut()) {
            obj.insert("jobsOpen".into(), Value::Array(open));
            if let Ok(prd) = crate::rows::row_summary(&ctx.workspace, "prd", 5) {
                obj.insert("prd".into(), prd);
            }
        }
        let pending = rules_notice(&lock(&self.session));
        if let (Some(notice), Some(obj)) = (pending, out.as_object_mut()) {
            obj.insert("rulesPending".into(), Value::String(notice));
        }
        out
    }

    async fn finish(&self, ctx: &Arc<Ctx>, args: Value, rpc_id: &Value, trace: &Map<String, Value>) -> Value {
        if args.get("witness").and_then(Value::as_str).is_none_or(|w| w.trim().is_empty()) {
            return self.refuse(ctx, "finish", args, "finish needs a non-empty witness string", rpc_id, trace);
        }
        let gaps_ok = match args.get("gaps") {
            None => true,
            Some(Value::Array(items)) => items.iter().all(|g| g.as_str().is_some_and(|g| !g.trim().is_empty())),
            Some(_) => false,
        };
        if !gaps_ok {
            return self.refuse(ctx, "finish", args, "gaps must be an array of non-empty strings", rpc_id, trace);
        }
        let run = ctx.run.clone();
        let finish_args = args.clone();
        let running = crate::runcode::background_running() + ctx.jobs.running();
        let stop_ctx = ctx.clone();
        let work = async move {
            if let Some(reply) = kernel_finish(&stop_ctx, &finish_args, running).await {
                return reply;
            }
            let stop = move || {
                crate::runcode::stop_all(&stop_ctx, "stopBackground");
                stop_ctx.jobs.kill_all();
            };
            tokio::task::spawn_blocking(move || run.finish(&finish_args, running, stop))
                .await
                .unwrap_or_else(|e| json!({ "ok": false, "error": format!("finish failed: {e}") }))
        };
        let out = self.perform(ctx, "finish", args, rpc_id, trace, work, Value::clone).await;
        if is_ok(&out) {
            lock(&self.session).ended = Some("finished");
            ctx.jobs.kill_all();
        }
        out
    }

    async fn perform<F>(&self, ctx: &Arc<Ctx>, tool: &str, args: Value, rpc_id: &Value, trace: &Map<String, Value>, work: F, digest: fn(&Value) -> Value) -> Value
    where
        F: Future<Output = Value>,
    {
        let step = lock(&self.session).steps;
        let started = Instant::now();
        ctx.run.enter();
        let action = ctx.run.event("action", action_fields(step, tool, args, rpc_id, trace));
        lock(&self.actions).insert(rpc_id.to_string(), action);
        let mut result = work.await;
        ctx.run.leave();
        if tool == "run_code" {
            if let Some(obj) = result.as_object_mut() {
                obj.insert("actionSeq".into(), json!(action));
            }
        }
        let dur_ms = started.elapsed().as_millis() as u64;
        if result.get("refused") == Some(&Value::Bool(true)) {
            ctx.run.event("refusal", json!({ "step": step, "tool": tool, "error": result.get("error"), "durMs": dur_ms, "actionSeq": action }));
        } else if !(tool == "finish" && is_ok(&result)) {
            let mut fields = Map::new();
            fields.insert("step".into(), json!(step));
            fields.insert("tool".into(), json!(tool));
            fields.insert("durMs".into(), json!(dur_ms));
            fields.insert("actionSeq".into(), json!(action));
            match digest(&result) {
                Value::Object(shown) => fields.extend(shown),
                other => {
                    fields.insert("value".into(), other);
                }
            }
            ctx.run.event("result", Value::Object(fields));
        }
        result
    }

    fn refuse(&self, ctx: &Arc<Ctx>, tool: &str, args: Value, reason: &str, rpc_id: &Value, trace: &Map<String, Value>) -> Value {
        let step = lock(&self.session).steps;
        let action = ctx.run.event("action", action_fields(step, tool, args, rpc_id, trace));
        let error = format!("refused: {reason}");
        ctx.run.event("refusal", json!({ "step": step, "tool": tool, "error": error, "durMs": 0, "actionSeq": action }));
        json!({ "ok": false, "refused": true, "error": error, "actionSeq": action })
    }

    async fn shutdown(self: &Arc<Self>, reason: &str) {
        {
            let mut s = lock(&self.session);
            if s.ended.is_none() {
                s.ended = Some("restarting");
            }
        }
        let ctx = self.ctx();
        ctx.jobs.kill_all();
        let _ = tokio::time::timeout(SHUTDOWN_GRACE, self.slots.acquire_many(self.max_inflight as u32)).await;
        ctx.run.pause(reason);
        ctx.hub.stop().await;
    }
}

fn kernel_feed_args(run: &Run, args: &Value, background_running: usize) -> Result<Value, Value> {
    let refuse = |text: String| json!({ "ok": false, "refused": true, "error": format!("refused: {text}") });
    if run.ended().is_some() {
        return Err(refuse(run.ended_reason(false)));
    }
    let mut fields = args.as_object().cloned().unwrap_or_default();
    fields.insert("maxSteps".into(), json!(run.max_steps()));
    fields.insert("toolsText".into(), json!(run.tools_text()));
    fields.insert("waitCommand".into(), json!(crate::feed::wait_command(run)));
    fields.insert("backgroundRunning".into(), json!(background_running));
    if args.get("child") == Some(&Value::Bool(true)) {
        fields.insert("step".into(), json!(run.step()));
        return Ok(Value::Object(fields));
    }
    let step = match run.begin_step() {
        Ok(step) => step,
        Err(text) => {
            run.end_run("max_steps", None, None);
            return Err(refuse(text));
        }
    };
    fields.insert("step".into(), json!(step));
    run.clear_delivered();
    match crate::log::take_pending(&run.dir) {
        Ok(taken) => {
            let records: Vec<Value> = taken.records.iter().filter_map(|r| serde_json::from_str(r).ok()).collect();
            fields.insert("pending".into(), Value::Array(records));
            run.set_delivered(taken.files);
        }
        Err(e) => {
            fields.insert("pendingError".into(), json!(e.to_string()));
        }
    }
    Ok(Value::Object(fields))
}

async fn kernel_feed(ctx: &Arc<Ctx>, args: &Value) -> Option<Value> {
    if !crate::verbs::jit::serves_kernel("nextContext").await {
        return None;
    }
    let run = ctx.run.clone();
    let given = args.clone();
    let running = crate::runcode::background_running() + ctx.jobs.running();
    let prepared = tokio::task::spawn_blocking(move || kernel_feed_args(&run, &given, running)).await.ok()?;
    match prepared {
        Ok(kernel_args) => crate::verbs::jit::kernel(ctx.clone(), "nextContext", kernel_args).await,
        Err(refusal) => Some(refusal),
    }
}

async fn kernel_finish(ctx: &Arc<Ctx>, args: &Value, running: usize) -> Option<Value> {
    if !crate::verbs::jit::serves_kernel("finish").await {
        return None;
    }
    let mut fields = args.as_object().cloned().unwrap_or_default();
    fields.insert("backgroundRunning".into(), json!(running));
    fields.insert("step".into(), json!(ctx.run.step()));
    let mut reply = crate::verbs::jit::kernel(ctx.clone(), "finish", Value::Object(fields)).await?;
    let end = reply.as_object_mut().and_then(|o| o.remove("end"));
    if let (true, Some(end)) = (is_ok(&reply), end) {
        if args.get("stopBackground") == Some(&Value::Bool(true)) {
            crate::runcode::stop_all(ctx, "stopBackground");
            ctx.jobs.kill_all();
        }
        let lint = crate::search::lint_wiki(&ctx.workspace);
        if !lint.skipped {
            ctx.run.event("lint", json!({ "problems": lint.problems, "pages": lint.pages }));
        }
        let gaps = end.get("gaps").filter(|g| !g.is_null()).cloned();
        ctx.run.end_run("finished", end.get("witness").and_then(Value::as_str), gaps);
    }
    Some(reply)
}

async fn feed_call(ctx: Arc<Ctx>, args: Value) -> Value {
    if let Some(reply) = kernel_feed(&ctx, &args).await {
        return reply;
    }
    let run = ctx.run.clone();
    tokio::task::spawn_blocking(move || crate::feed::next_context(&run, &args))
        .await
        .unwrap_or_else(|e| json!({ "ok": false, "error": format!("next_context failed: {e}") }))
}

fn action_fields(step: u64, tool: &str, args: Value, rpc_id: &Value, trace: &Map<String, Value>) -> Value {
    let mut fields = Map::new();
    fields.insert("step".into(), json!(step));
    fields.insert("tool".into(), json!(tool));
    fields.insert("args".into(), args);
    if !rpc_id.is_null() {
        fields.insert("rpcId".into(), rpc_id.clone());
        fields.extend(trace.iter().map(|(k, v)| (k.clone(), v.clone())));
    }
    Value::Object(fields)
}

fn initialize(params: &Value) -> Value {
    let asked = params.get("protocolVersion").and_then(Value::as_str);
    let version = asked.filter(|v| SUPPORTED_VERSIONS.contains(v)).unwrap_or(SUPPORTED_VERSIONS[0]);
    json!({
        "protocolVersion": version,
        "capabilities": capabilities(),
        "instructions": instructions(),
        "serverInfo": { "name": SERVER_NAME, "title": SERVER_TITLE, "version": SERVER_VERSION }
    })
}

fn prompt_of(name: &str) -> Result<Value, String> {
    let skill = skills::skill(name)?;
    let title = if name == "gm" { "gm run" } else { "gm continue" };
    let mut prompt = json!({ "name": name, "title": title, "description": skill.field("description") });
    if name == "gm" {
        prompt["arguments"] = json!([{ "name": "task", "description": "The task to run", "required": false }]);
    }
    Ok(prompt)
}

fn note_rule(s: &mut Session, run: &Run, rule: u8, evidence: String) {
    if s.rules.iter().any(|(known, _)| *known == rule) {
        return;
    }
    run.event("rules-notice", json!({ "step": s.steps, "rule": rule, "evidence": evidence }));
    s.rules.push((rule, evidence));
}

fn clear_rules(s: &mut Session, run: &Run) {
    if s.rules.is_empty() {
        return;
    }
    let rules: Vec<u8> = s.rules.iter().map(|(rule, _)| *rule).collect();
    s.rules.clear();
    run.event("rules-cleared", json!({ "step": s.steps, "rules": rules }));
}

fn rules_notice(s: &Session) -> Option<String> {
    if s.rules.is_empty() {
        return None;
    }
    let lines: Vec<String> = s
        .rules
        .iter()
        .map(|(rule, evidence)| {
            let text = RULE_TEXT.iter().find(|(r, _)| r == rule).map_or("", |(_, t)| t);
            format!("rule {rule} ({text}): {evidence}")
        })
        .collect();
    Some(format!(
        "The rules check found: {}. Read the gm skill again with ReadMcpResourceTool (server gm, uri skill://gm/SKILL.md) and follow the rules it gives. That one read clears this notice. Do not repeat finished work.",
        lines.join("; ")
    ))
}

fn watch_call(s: &mut Session, run: &Run, name: &str, out: &Value) {
    let loads = skills::gm_loads();
    if loads != s.gm_loads {
        s.gm_loads = loads;
        clear_rules(s, run);
    }
    match name {
        "next_context" => {}
        "finish" => {
            let error = out.get("error").and_then(Value::as_str).unwrap_or("");
            if out.get("refused") == Some(&Value::Bool(true)) && error.contains("needs a non-empty witness") {
                note_rule(s, run, 4, format!("finish was refused: {error}"));
            } else if is_ok(out) && s.step_calls == 0 {
                note_rule(s, run, 3, "finish was called with no call in its step".to_string());
            }
        }
        _ => s.step_calls += 1,
    }
}

#[cfg(unix)]
fn sigterm() -> mpsc::UnboundedReceiver<()> {
    use std::sync::atomic::{AtomicI32, Ordering};
    static WRITE_FD: AtomicI32 = AtomicI32::new(-1);
    extern "C" fn on_term(_: libc::c_int) {
        let fd = WRITE_FD.load(Ordering::Relaxed);
        if fd >= 0 {
            let byte = 1u8;
            unsafe {
                libc::write(fd, &byte as *const u8 as *const libc::c_void, 1);
            }
        }
    }
    let (tx, rx) = mpsc::unbounded_channel();
    let mut fds = [0 as libc::c_int; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        std::mem::forget(tx);
        return rx;
    }
    WRITE_FD.store(fds[1], Ordering::Relaxed);
    let handler = on_term as extern "C" fn(libc::c_int);
    unsafe {
        libc::signal(libc::SIGTERM, handler as libc::sighandler_t);
    }
    let read_fd = fds[0];
    std::thread::spawn(move || {
        let mut byte = 0u8;
        loop {
            let n = unsafe { libc::read(read_fd, &mut byte as *mut u8 as *mut libc::c_void, 1) };
            if n == 1 {
                let _ = tx.send(());
                return;
            }
            if n == 0 {
                std::mem::forget(tx);
                return;
            }
        }
    });
    rx
}

#[cfg(not(unix))]
fn sigterm() -> mpsc::UnboundedReceiver<()> {
    let (tx, rx) = mpsc::unbounded_channel();
    std::mem::forget(tx);
    rx
}

pub async fn serve_stdio(opts: WorkerOpts) -> Result<(), String> {
    let workspace = std::path::absolute(&opts.workspace).map_err(|e| format!("workspace: {e}"))?;
    if !workspace.is_dir() {
        return Err(format!("workspace is not a directory: {}", workspace.display()));
    }
    if opts.max_steps < 1 {
        return Err("--max-steps must be a positive integer".into());
    }
    if opts.max_inflight < 1 {
        return Err("--max-inflight must be a positive integer".into());
    }
    let run = Arc::new(Run::open(&workspace, opts.run_dir.as_deref(), opts.task.as_deref())?);
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Option<String>>();
    let notices = Notices::new(out_tx.clone());
    notices.hosted(&run);
    let subscriptions = Subscriptions::new(out_tx.clone());
    let ctx = Arc::new(Ctx {
        workspace: workspace.clone(),
        run: run.clone(),
        jobs: Arc::new(Jobs::new()),
        locks: Arc::new(Locks::new()),
        hub: Arc::new(Hub::new()),
    });
    ctx.jobs.set_dir(run.dir.clone());
    ctx.hub.attach(run.clone());

    let (ready_tx, ready_rx) = watch::channel(false);
    {
        let hub = ctx.hub.clone();
        let layers = mcp_layers(&workspace, opts.mcp_config.as_deref());
        let ws = workspace.clone();
        tokio::spawn(async move {
            hub.start(ws, layers).await;
            log_tools(&run, &hub);
            let _ = ready_tx.send(true);
        });
    }

    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(Some(line)) = out_rx.recv().await {
            if stdout.write_all(line.as_bytes()).await.is_err() || stdout.write_all(b"\n").await.is_err() || stdout.flush().await.is_err() {
                eprintln!("gm: stdout closed");
            }
        }
        let _ = stdout.flush().await;
    });

    let server = Arc::new(Server {
        workspace,
        max_steps: opts.max_steps,
        task: opts.task.clone(),
        ctx: RwLock::new(ctx),
        session: Mutex::new(Session { task_set: opts.task.is_some(), gm_loads: skills::gm_loads(), ..Session::default() }),
        notices,
        subscriptions,
        out: out_tx,
        ready: ready_rx,
        slots: Arc::new(Semaphore::new(opts.max_inflight)),
        max_inflight: opts.max_inflight,
        replies: Mutex::new(Replies::default()),
        inflight: Mutex::new(HashMap::new()),
        actions: Mutex::new(HashMap::new()),
    });

    let mut term = sigterm();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut handlers = tokio::task::JoinSet::new();
    let reason = loop {
        tokio::select! {
            Some(_) = handlers.join_next(), if !handlers.is_empty() => {}
            line = lines.next_line() => match line {
                Ok(Some(line)) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let server = server.clone();
                    handlers.spawn(async move {
                        if let Some(reply) = server.handle_line(line).await {
                            server.send(reply);
                        }
                    });
                }
                Ok(None) => break "the supervisor ended stdin",
                Err(e) => {
                    eprintln!("gm: stdin: {e}");
                    break "stdin failed";
                }
            },
            _ = term.recv() => break "SIGTERM from the supervisor",
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(10), async { while handlers.join_next().await.is_some() {} }).await;
    server.shutdown(reason).await;
    let _ = server.out.send(None);
    let _ = tokio::time::timeout(Duration::from_secs(2), writer).await;
    std::process::exit(0);
}
