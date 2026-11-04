use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::sync::OnceLock;
use std::time::Duration;

use regex::Regex;
use serde_json::Value;

use crate::run::Run;

const RETRIES: u32 = 3;
const BASE_MS: u64 = 1000;
const CAP_MS: u64 = 16000;
const JITTER: f64 = 0.2;

const TRANSIENT_CODES: [&str; 9] = [
    "ECONNRESET",
    "ECONNREFUSED",
    "ECONNABORTED",
    "ETIMEDOUT",
    "EAI_AGAIN",
    "UND_ERR_SOCKET",
    "UND_ERR_CONNECT_TIMEOUT",
    "UND_ERR_HEADERS_TIMEOUT",
    "UND_ERR_BODY_TIMEOUT",
];

const TEXT_PATTERNS: [(&str, &str); 6] = [
    ("(?i)connection refused|econnrefused", "ECONNREFUSED"),
    ("(?i)failed to connect|error connecting to", "connect"),
    ("(?i)connection reset|econnreset|reset by peer|und_err_socket", "ECONNRESET"),
    ("(?i)timed out|timeout|etimedout|aborterror|aborted", "ETIMEDOUT"),
    ("(?i)hung up unexpectedly|remote end hung up|socket hang up", "hung-up"),
    ("(?i)temporary failure in name resolution", "EAI_AGAIN"),
];

const STATUS_PATTERN: &str = r"(?i)(?:HTTP|returned error:?|status code)\s*(408|429|5\d\d)\b";
const URL_PATTERN: &str = r"(?i)[a-z][a-z0-9+.-]*://\S*";
const HOST_PATTERN: &str = r"(?i)\b[a-z0-9-]+(?:\.[a-z0-9-]+)+\b";

struct Patterns {
    text: Vec<(Regex, &'static str)>,
    status: Regex,
    url: Regex,
    host: Regex,
}

fn patterns() -> &'static Patterns {
    static PATTERNS: OnceLock<Patterns> = OnceLock::new();
    PATTERNS.get_or_init(|| Patterns {
        text: TEXT_PATTERNS
            .iter()
            .map(|(pattern, kind)| (Regex::new(pattern).expect("text pattern"), *kind))
            .collect(),
        status: Regex::new(STATUS_PATTERN).expect("status pattern"),
        url: Regex::new(URL_PATTERN).expect("url pattern"),
        host: Regex::new(HOST_PATTERN).expect("host pattern"),
    })
}

pub fn error_type(message: &str) -> Option<String> {
    let patterns = patterns();
    let without_urls = patterns.url.replace_all(message, " ");
    let stripped = patterns.host.replace_all(&without_urls, " ");
    if let Some(code) = TRANSIENT_CODES.iter().find(|code| stripped.contains(**code)) {
        return Some((*code).to_string());
    }
    if let Some((_, kind)) = patterns.text.iter().find(|(pattern, _)| pattern.is_match(&stripped)) {
        return Some((*kind).to_string());
    }
    patterns
        .status
        .captures(&stripped)
        .map(|captures| format!("HTTP-{}", &captures[1]))
}

pub fn is_transient(message: &str) -> bool {
    error_type(message).is_some()
}

pub fn backoff_ms(attempt: u32) -> u64 {
    let base = (BASE_MS << attempt.saturating_sub(1).min(16)).min(CAP_MS);
    let unit = jitter_unit();
    (base as f64 * (1.0 + JITTER * (2.0 * unit - 1.0))).round() as u64
}

fn jitter_unit() -> f64 {
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0));
    (hasher.finish() % 1_000_000) as f64 / 1_000_000.0
}

pub async fn retrying<F, Fut>(tool: &str, run: &Run, mut attempt: F) -> Value
where
    F: FnMut() -> Fut,
    Fut: Future<Output = (Value, Option<String>)>,
{
    let mut n: u32 = 1;
    loop {
        let (mut result, error_kind) = attempt().await;
        let Some(error_kind) = error_kind else {
            set_field(&mut result, "attempts", n.into());
            return result;
        };
        if n > RETRIES {
            set_field(&mut result, "attempts", n.into());
            set_field(&mut result, "exhausted", Value::Bool(true));
            return result;
        }
        let delay = backoff_ms(n);
        run.event(
            "retry",
            serde_json::json!({ "tool": tool, "attempt": n, "errorType": error_kind, "delayMs": delay }),
        );
        tokio::time::sleep(Duration::from_millis(delay)).await;
        n += 1;
    }
}

fn set_field(value: &mut Value, key: &str, field: Value) {
    if let Value::Object(map) = value {
        map.insert(key.to_string(), field);
    }
}
