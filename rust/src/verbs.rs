use std::sync::Arc;

use serde_json::{json, Value};

use crate::mcp_client::Hub;
use crate::types::{Ctx, VerbDef};

#[cfg(feature = "jit")]
#[path = "jit.rs"]
pub mod jit;

#[cfg(feature = "jit")]
#[path = "native.rs"]
pub mod native;

#[cfg(not(feature = "jit"))]
pub mod jit {
    use std::sync::Arc;

    use serde_json::Value;

    use crate::types::Ctx;

    pub fn listing() -> Vec<Value> {
        Vec::new()
    }

    pub async fn call_verb(_gm: Arc<Ctx>, _name: &str, _args: Value) -> Option<Value> {
        None
    }

    pub async fn serves_kernel(_function: &str) -> bool {
        false
    }

    pub async fn kernel(_gm: Arc<Ctx>, _function: &'static str, _args: Value) -> Option<Value> {
        None
    }
}

pub const READ_ONLY: &[&str] = &[
    "next_context",
    "read_file",
    "list_dir",
    "stat_file",
    "search",
    "wiki_search",
    "grep",
    "callers",
    "impact",
    "query_log",
    "job_status",
    "git_status",
    "git_log",
    "git_diff",
    "git_show",
    "ci_status",
    "prd_list",
    "unknown_list",
    "review_join",
];

pub fn native() -> Vec<&'static VerbDef> {
    let groups: [&'static [VerbDef]; 11] = [
        crate::fsverbs::VERBS,
        crate::exec::VERBS,
        crate::log::VERBS,
        crate::search::VERBS,
        crate::code::VERBS,
        crate::git::VERBS,
        crate::ci::VERBS,
        crate::rows::VERBS,
        crate::fetch::VERBS,
        crate::review::VERBS,
        crate::run::VERBS,
    ];
    groups.iter().flat_map(|g| g.iter()).collect()
}

pub fn lookup(name: &str) -> Option<&'static VerbDef> {
    native().into_iter().find(|verb| verb.name == name)
}

pub fn is_read_only(name: &str) -> bool {
    READ_ONLY.contains(&name)
}

pub fn schema_of(verb: &VerbDef) -> Value {
    serde_json::from_str(verb.schema).unwrap_or_else(|_| json!({ "type": "object" }))
}

pub fn hub_name(server: &str, tool: &str) -> String {
    format!("{server}_{tool}")
}

pub fn catalog(hub: &Hub) -> Vec<Value> {
    let mut list: Vec<Value> = native()
        .into_iter()
        .map(|verb| json!({ "name": verb.name, "description": verb.description, "inputSchema": schema_of(verb) }))
        .collect();
    for entry in jit::listing() {
        let Some(name) = entry.get("name").and_then(Value::as_str) else { continue };
        let shown = json!({ "name": name, "description": entry["description"], "inputSchema": entry["inputSchema"] });
        match list.iter_mut().find(|verb| verb["name"] == name) {
            Some(slot) => *slot = shown,
            None => list.push(shown),
        }
    }
    for tool in hub.tools() {
        let name = hub_name(&tool.server, &tool.tool);
        if list.iter().all(|verb| verb["name"] != name.as_str()) {
            list.push(json!({ "name": name, "description": tool.description, "inputSchema": tool.schema }));
        }
    }
    list
}

pub fn names(hub: &Hub) -> Vec<String> {
    catalog(hub)
        .into_iter()
        .filter_map(|verb| verb.get("name").and_then(Value::as_str).map(str::to_string))
        .collect()
}

pub async fn call(ctx: Arc<Ctx>, name: &str, args: Value) -> Value {
    let args = if args.is_null() { json!({}) } else { args };
    if let Some(reply) = jit::call_verb(ctx.clone(), name, args.clone()).await {
        return reply;
    }
    let outcome = match lookup(name) {
        Some(verb) => (verb.call)(ctx, args).await,
        None => {
            let target = ctx.hub.tools().into_iter().find(|tool| hub_name(&tool.server, &tool.tool) == name);
            match target {
                Some(tool) => ctx.hub.call(&tool.server, &tool.tool, args).await,
                None => Err(format!("unknown verb: {name}")),
            }
        }
    };
    match outcome {
        Ok(value) => value,
        Err(error) => json!({ "ok": false, "error": error }),
    }
}
