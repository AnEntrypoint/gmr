use std::sync::{Arc, Mutex};

use serde_json::{json, Map, Value};
use tokio::sync::mpsc;

use crate::mcp::SERVER_NAME;
use crate::run::Run;

pub const LOG_LEVELS: [&str; 8] = ["debug", "info", "notice", "warning", "error", "critical", "alert", "emergency"];
const QUEUE_MAX: usize = 64;
const DETAIL_FIELDS: [&str; 12] = ["job", "tool", "state", "code", "method", "message", "error", "reason", "status", "rules", "sinceProgressMs", "lastSeq"];

fn level_of(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "restart" | "reload" | "tools-changed" | "end" => "notice",
        "paused" | "cancel" | "job-unstuck" => "info",
        "recovery" | "job-stuck" | "job-killed" | "rules-notice" => "warning",
        "error" => "error",
        _ => return None,
    })
}

fn rank(level: &str) -> Option<usize> {
    LOG_LEVELS.iter().position(|known| *known == level)
}

pub struct Notices {
    level: Mutex<Option<usize>>,
    queued: Mutex<Vec<Value>>,
    out: mpsc::UnboundedSender<Option<String>>,
}

impl Notices {
    pub fn new(out: mpsc::UnboundedSender<Option<String>>) -> Arc<Self> {
        Arc::new(Notices { level: Mutex::new(None), queued: Mutex::new(Vec::new()), out })
    }

    pub fn hosted(self: &Arc<Self>, run: &Run) {
        let notices = self.clone();
        run.set_sink(Arc::new(move |kind: &str, seq: u64, fields: &Value| notices.event(kind, seq, fields)));
    }

    fn event(&self, kind: &str, seq: u64, fields: &Value) {
        if kind == "end" {
            self.send(json!({ "jsonrpc": "2.0", "method": "notifications/resources/list_changed" }));
        }
        let Some(level) = level_of(kind) else { return };
        let mut data = Map::new();
        data.insert("kind".into(), json!(kind));
        data.insert("seq".into(), json!(seq));
        for key in DETAIL_FIELDS {
            if let Some(value) = fields.get(key) {
                data.insert(key.into(), value.clone());
            }
        }
        let notice = json!({ "level": level, "logger": SERVER_NAME, "data": Value::Object(data) });
        match *lock(&self.level) {
            Some(current) => self.deliver(&notice, current),
            None => {
                let mut queued = lock(&self.queued);
                if queued.len() < QUEUE_MAX {
                    queued.push(notice);
                }
            }
        }
    }

    fn deliver(&self, notice: &Value, current: usize) {
        let at = notice.get("level").and_then(Value::as_str).and_then(rank).unwrap_or(0);
        if at >= current {
            self.send(json!({ "jsonrpc": "2.0", "method": "notifications/message", "params": notice }));
        }
    }

    fn send(&self, message: Value) {
        let _ = self.out.send(Some(message.to_string()));
    }

    pub fn set_level(&self, params: &Value) -> Result<Value, String> {
        let level = params.get("level").and_then(Value::as_str).and_then(rank).ok_or_else(|| format!("unknown log level: {}", crate::mcp::js_text(params.get("level"))))?;
        *lock(&self.level) = Some(level);
        for notice in std::mem::take(&mut *lock(&self.queued)) {
            self.deliver(&notice, level);
        }
        Ok(json!({}))
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
