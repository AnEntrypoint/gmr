use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Map, Value};

use crate::types::{Ctx, Fut, VerbDef, VerbOutcome};

const K1: f64 = 1.2;
const B: f64 = 0.75;
const MAX_FILE_BYTES: u64 = 1_000_000;
const MAX_LIST_BYTES: usize = 64 * 1024 * 1024;
const LIST_TIMEOUT: Duration = Duration::from_secs(30);
const SNIPPET_UNITS: usize = 300;
const OUTPUT_PAGE: usize = 20_000;
const DEFAULT_K: i64 = 10;
const MAX_K: i64 = 50;
const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 1000;
const MAX_PATTERN_UNITS: usize = 200;
const BINARY_PROBE: usize = 8000;
const SETTLE: Duration = Duration::from_millis(3000);
const CLOSURE_ANCHORS: usize = 10;
const CLOSURE_CAP: usize = 20;
const STATE_DIRS: [&str; 7] = [".git", ".gm", ".agentplug", ".agentplug-kv", ".gm-new", ".playwriter", "runs"];
const ROOT_ARGS: [&str; 3] = ["root", "projectPath", "cwd"];
const SEARCH_TAKES: [&str; 3] = ["query", "k", "closure"];
const WIKI_SEARCH_TAKES: [&str; 2] = ["query", "k"];
const GREP_TAKES: [&str; 8] = ["pattern", "path", "mode", "scope", "ignoreCase", "limit", "offset", "vendor"];
const KINDS: [&str; 3] = ["ingest", "answer", "lint"];
const REGEX_CHARS: &str = ".|()[]{}*+?^$\\";

pub const VERBS: &[VerbDef] = &[
    VerbDef {
        name: "search",
        description: "Rank the lines of git-tracked files by BM25 and return the best k as path:line score text. An identifier also matches its parts, so planWaves matches plan and waves. k is 1 to 50, default 10. closure true (default false) adds one line per JavaScript file that defines a top-level function called from the function enclosing a top-10 hit and is not itself a top-10 file, as path:line closure text in place of a score, where line is the first line of that function, at most 20 lines. root (or its aliases projectPath and cwd) names another existing directory to search instead of the workspace; with a root other than the workspace, hits show absolute paths. An unknown argument is refused by name.",
        schema: r#"{"type":"object","properties":{"query":{"type":"string"},"k":{"type":"number"},"root":{"type":"string"},"closure":{"type":"boolean"}},"required":["query"]}"#,
        call: search_call,
    },
    VerbDef {
        name: "wiki_search",
        description: "Rank the lines of the wiki pages (wiki/, tracked or not) by BM25 and return the best k as path:line score text, the same way as search. k is 1 to 50, default 10. root (or its aliases projectPath and cwd) names another existing directory to search instead of the workspace; with a root other than the workspace, hits show absolute paths. An unknown argument is refused by name.",
        schema: r#"{"type":"object","properties":{"query":{"type":"string"},"k":{"type":"number"},"root":{"type":"string"}},"required":["query"]}"#,
        call: wiki_search_call,
    },
    VerbDef {
        name: "grep",
        description: "Find text in the files under path: the workspace by default, or a file or directory of it. A path inside the workspace searches its git-tracked files (or, outside a git repository, every file under it); a path outside the workspace is walked and its hits show absolute paths. files is git or walk, the source used. mode is literal (default; also named content), regex, or word (a whole identifier). If a literal search finds nothing and the pattern has a regex character, the pattern runs once more as a regex; the hint says which pass matched, and a pattern that is not a valid regex is an error. scope is tracked (default) or all: all also searches untracked and gitignored files, node_modules included, and skips .git and the state directories .gm, .agentplug, .agentplug-kv, .gm-new, .playwriter and runs. A tracked search that skips files says how many in a hint and names scope all. Returns path:line text. ignoreCase is optional; limit is 1 to 1000, default 100. Paths under node_modules/ or vendor/ are skipped by tracked unless vendor is true; a walk also skips dot-directories and runs/. all lists files with git ls-files, which has a 30 s limit, so a large tree can take longer. root (or its aliases projectPath and cwd) names another existing directory to search instead of the workspace; with a root other than the workspace, hits show absolute paths. An unknown argument is refused by name.",
        schema: r#"{"type":"object","properties":{"pattern":{"type":"string"},"path":{"type":"string"},"mode":{"type":"string"},"scope":{"type":"string"},"ignoreCase":{"type":"boolean"},"limit":{"type":"number"},"offset":{"type":"number"},"vendor":{"type":"boolean"},"root":{"type":"string"}},"required":["pattern"]}"#,
        call: grep_call,
    },
    VerbDef {
        name: "wiki_write",
        description: "Write a wiki page, wiki/<page>.md, with its source ids and its claims. A claim quote must appear in its named source (sources/<id>.txt), or the whole page is refused and nothing is written. Updates wiki/index.md and wiki/log.md. kind is ingest (default), answer or lint. page is lowercase letters, digits and hyphens. contradicts and resolves name pages.",
        schema: r#"{"type":"object","properties":{"page":{"type":"string"},"title":{"type":"string"},"body":{"type":"string"},"summary":{"type":"string"},"sources":{"type":"array","items":{"type":"string"}},"claims":{"type":"array","items":{"type":"object"}},"kind":{"type":"string"},"contradicts":{"type":"array","items":{"type":"string"}},"resolves":{"type":"array","items":{"type":"string"}}},"required":["page","title","body","summary","sources"]}"#,
        call: wiki_write_call,
    },
];

fn search_call(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    blocking(ctx, args, search_work)
}

fn wiki_search_call(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    blocking(ctx, args, wiki_search_work)
}

fn grep_call(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    blocking(ctx, args, grep_work)
}

fn wiki_write_call(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    blocking(ctx, args, wiki_write_work)
}

pub(crate) fn blocking(ctx: Arc<Ctx>, args: Value, work: fn(&Ctx, &Value) -> Value) -> Fut<VerbOutcome> {
    Box::pin(async move {
        tokio::task::spawn_blocking(move || work(&ctx, &args))
            .await
            .map_err(|e| e.to_string())
    })
}

fn search_work(ctx: &Ctx, args: &Value) -> Value {
    guarded(ctx, "search", &SEARCH_TAKES, args, search_op)
}

fn wiki_search_work(ctx: &Ctx, args: &Value) -> Value {
    guarded(ctx, "wiki_search", &WIKI_SEARCH_TAKES, args, wiki_search_op)
}

fn grep_work(ctx: &Ctx, args: &Value) -> Value {
    guarded(ctx, "grep", &GREP_TAKES, args, grep_op)
}

fn wiki_write_work(ctx: &Ctx, args: &Value) -> Value {
    let root = absolute(&ctx.workspace);
    wiki_write(&root, ctx.run.as_ref(), args)
}

fn search_op(s: &Scope, args: &Value) -> Value {
    ranked_op(s, TRACKED, args)
}

fn wiki_search_op(s: &Scope, args: &Value) -> Value {
    ranked_op(s, WIKI, args)
}

pub(crate) fn fail(error: &str) -> Value {
    json!({"ok": false, "error": error})
}

fn refused(reason: &str) -> Value {
    json!({"ok": false, "refused": true, "error": format!("refused: {reason}")})
}

pub(crate) fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

pub(crate) fn resolve(base: &Path, rel: &str) -> PathBuf {
    normalize(&base.join(rel))
}

pub(crate) fn absolute(path: &Path) -> PathBuf {
    if path.is_absolute() {
        normalize(path)
    } else {
        normalize(&std::env::current_dir().unwrap_or_default().join(path))
    }
}

fn rel_slash(root: &Path, at: &Path) -> String {
    match at.strip_prefix(root) {
        Ok(rel) => rel
            .components()
            .filter_map(|c| match c {
                Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("/"),
        Err(_) => at.to_string_lossy().into_owned(),
    }
}

fn int_arg(args: &Value, key: &str, default: i64) -> Option<i64> {
    match args.get(key) {
        None => Some(default),
        Some(v) => v
            .as_f64()
            .filter(|f| f.fract() == 0.0 && f.is_finite())
            .map(|f| f as i64),
    }
}

fn plural(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn nanos(t: SystemTime) -> u128 {
    t.duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0)
}

fn settled(t: SystemTime) -> bool {
    SystemTime::now()
        .duration_since(t)
        .map(|age| age > SETTLE)
        .unwrap_or(false)
}

struct Scope {
    root: PathBuf,
    confined: bool,
    real: Option<PathBuf>,
}

fn scope_for(workspace: &Path, dir: PathBuf) -> Scope {
    let confined = dir == workspace;
    let real = if confined { fs::canonicalize(workspace).ok() } else { None };
    Scope { root: dir, confined, real }
}

fn guarded(ctx: &Ctx, name: &str, takes: &[&str], args: &Value, op: fn(&Scope, &Value) -> Value) -> Value {
    let empty = Map::new();
    let obj = args.as_object().unwrap_or(&empty);
    let unknown: Vec<&str> = obj
        .keys()
        .map(String::as_str)
        .filter(|k| !takes.contains(k) && !ROOT_ARGS.contains(k))
        .collect();
    if !unknown.is_empty() {
        let mut shown: Vec<&str> = takes.to_vec();
        shown.push("root");
        return refused(&format!(
            "unknown argument {}; {name} takes {}",
            unknown.join(", "),
            shown.join(", ")
        ));
    }
    let workspace = absolute(&ctx.workspace);
    match home_of(&workspace, obj) {
        Ok(dir) => op(&scope_for(&workspace, dir), args),
        Err(reason) => refused(&reason),
    }
}

fn home_of(root: &Path, args: &Map<String, Value>) -> Result<PathBuf, String> {
    let given: Vec<&str> = ROOT_ARGS
        .iter()
        .copied()
        .filter(|key| args.contains_key(*key))
        .collect();
    if given.is_empty() {
        return Ok(root.to_path_buf());
    }
    let mut dirs: Vec<PathBuf> = Vec::new();
    for key in &given {
        let text = args
            .get(*key)
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty())
            .ok_or_else(|| format!("{key} must be a non-empty path"))?;
        dirs.push(resolve(root, text));
    }
    let distinct: HashSet<&PathBuf> = dirs.iter().collect();
    if distinct.len() > 1 {
        return Err(format!("{} name different roots; pass one", given.join(" and ")));
    }
    let raw = args
        .get(given[0])
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let dir = dirs.swap_remove(0);
    match fs::metadata(&dir) {
        Err(_) => Err(format!("no such root: {raw}")),
        Ok(meta) if meta.is_dir() => Ok(dir),
        Ok(_) => Err(format!("root is not a directory: {raw}")),
    }
}

#[derive(Clone, Copy)]
struct Listing {
    name: &'static str,
    args: &'static [&'static str],
    cached: bool,
}

const TRACKED: Listing = Listing {
    name: "tracked",
    args: &["ls-files", "-z"],
    cached: true,
};

const WIKI: Listing = Listing {
    name: "wiki",
    args: &["ls-files", "-z", "-co", "--exclude-standard", "--", "wiki"],
    cached: false,
};

const ALL_FILES: Listing = Listing {
    name: "all",
    args: &["ls-files", "-z", "-c", "-o"],
    cached: false,
};

const UNTRACKED: Listing = Listing {
    name: "untracked",
    args: &["ls-files", "-z", "-o", "--exclude-standard"],
    cached: false,
};

struct ListCache {
    key: String,
    root: PathBuf,
    paths: Arc<Vec<String>>,
}

fn listing_cache() -> &'static Mutex<HashMap<&'static str, ListCache>> {
    static CACHE: OnceLock<Mutex<HashMap<&'static str, ListCache>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn index_stamp(root: &Path) -> Option<(String, bool)> {
    let meta = fs::metadata(root.join(".git").join("index")).ok()?;
    let modified = meta.modified().ok()?;
    Some((format!("{}:{}", meta.len(), nanos(modified)), settled(modified)))
}

pub(crate) fn git_output(root: &Path, args: &[&str], limit: usize, timeout: Duration) -> Option<Vec<u8>> {
    let mut child = Command::new("git")
        .args(args)
        .current_dir(root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let mut pipe = child.stdout.take()?;
    let reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let result = pipe.by_ref().take(limit as u64 + 1).read_to_end(&mut buf);
        result.map(|_| buf)
    });
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let buf = reader.join().ok()?.ok()?;
                return (status.success() && buf.len() <= limit).then_some(buf);
            }
            Ok(None) => {
                if started.elapsed() > timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(_) => return None,
        }
    }
}

fn tracked(root: &Path, listing: Listing) -> Result<Arc<Vec<String>>, String> {
    let stamp = index_stamp(root);
    if listing.cached {
        if let Some((key, true)) = &stamp {
            if let Some(known) = lock(listing_cache()).get(listing.name) {
                if known.key == *key && known.root == root {
                    return Ok(known.paths.clone());
                }
            }
        }
    }
    let out = git_output(root, listing.args, MAX_LIST_BYTES, LIST_TIMEOUT)
        .ok_or_else(|| "search and grep need a git repository: git ls-files failed".to_string())?;
    let paths: Arc<Vec<String>> = Arc::new(
        out.split(|b| *b == 0)
            .filter(|p| !p.is_empty())
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .collect(),
    );
    if listing.cached {
        if let Some((key, true)) = stamp {
            lock(listing_cache()).insert(
                listing.name,
                ListCache {
                    key,
                    root: root.to_path_buf(),
                    paths: paths.clone(),
                },
            );
        }
    }
    Ok(paths)
}

fn is_vendor(rel: &str) -> bool {
    rel.rsplit_once('/').map_or(false, |(dirs, _)| {
        dirs.split('/').any(|p| p == "node_modules" || p == "vendor")
    })
}

fn in_state(rel: &str) -> bool {
    rel.rsplit_once('/')
        .map_or(false, |(dirs, _)| dirs.split('/').any(|p| STATE_DIRS.contains(&p)))
}

fn under_dir(paths: &[String], dir: &str, vendor: bool) -> Vec<String> {
    paths
        .iter()
        .filter(|rel| {
            let rel = rel.as_str();
            let inside = dir.is_empty()
                || rel == dir
                || (rel.starts_with(dir) && rel.as_bytes().get(dir.len()) == Some(&b'/'));
            inside && (vendor || !is_vendor(rel))
        })
        .cloned()
        .collect()
}

fn confined_target(s: &Scope, rel: &str) -> Option<PathBuf> {
    if rel.is_empty() || rel.split(['/', '\\']).any(|part| part == "..") {
        return None;
    }
    let target = s.root.join(rel);
    let real = fs::canonicalize(&target).ok()?;
    real.starts_with(s.real.as_ref()?).then_some(target)
}

fn file_target(s: &Scope, rel: &str) -> Option<PathBuf> {
    if s.confined {
        confined_target(s, rel)
    } else {
        Some(s.root.join(rel))
    }
}

fn shown_at(s: &Scope, rel: &str) -> String {
    if s.confined {
        rel.to_string()
    } else {
        s.root.join(rel).to_string_lossy().into_owned()
    }
}

fn read_text(path: &Path) -> Option<Vec<u8>> {
    let meta = fs::symlink_metadata(path).ok()?;
    if !meta.is_file() || meta.len() > MAX_FILE_BYTES {
        return None;
    }
    let bytes = fs::read(path).ok()?;
    let probe = &bytes[..bytes.len().min(BINARY_PROBE)];
    if probe.contains(&0) {
        None
    } else {
        Some(bytes)
    }
}

fn each_line(bytes: &[u8], mut f: impl FnMut(usize, &str)) {
    for (i, raw) in bytes.split(|b| *b == b'\n').enumerate() {
        let raw = if raw.last() == Some(&b'\r') { &raw[..raw.len() - 1] } else { raw };
        let text = String::from_utf8_lossy(raw);
        f(i + 1, &text);
    }
}

fn utf16_len(text: &str) -> usize {
    text.chars().map(char::len_utf16).sum()
}

fn snippet(text: &str) -> String {
    if utf16_len(text) <= SNIPPET_UNITS {
        return text.to_string();
    }
    let mut units = 0;
    let mut end = 0;
    for (i, c) in text.char_indices() {
        if units + c.len_utf16() > SNIPPET_UNITS {
            break;
        }
        units += c.len_utf16();
        end = i + c.len_utf8();
    }
    format!("{} ...", &text[..end])
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn has_word(hay: &str, needle: &str) -> bool {
    let bytes = hay.as_bytes();
    let n = needle.len();
    if n == 0 {
        return false;
    }
    for (i, _) in hay.char_indices() {
        if i + n > bytes.len() || !hay[i..].starts_with(needle) {
            continue;
        }
        let before_ok = i == 0 || !is_word_byte(bytes[i - 1]);
        let after_ok = i + n == bytes.len() || !is_word_byte(bytes[i + n]);
        if before_ok && after_ok {
            return true;
        }
    }
    false
}

fn lower_into(buf: &mut String, word: &str) {
    buf.clear();
    buf.extend(word.chars().map(|c| c.to_ascii_lowercase()));
}

fn part_ranges(word: &str) -> Vec<(usize, usize)> {
    let b = word.as_bytes();
    let mut out = Vec::new();
    let mut start = 0;
    for i in 0..b.len() {
        if b[i] == b'_' {
            if i > start {
                out.push((start, i));
            }
            start = i + 1;
        } else if i > 0
            && b[i].is_ascii_uppercase()
            && (b[i - 1].is_ascii_lowercase() || b[i - 1].is_ascii_digit())
        {
            if i > start {
                out.push((start, i));
            }
            start = i;
        }
    }
    if b.len() > start {
        out.push((start, b.len()));
    }
    out
}

fn for_each_token(text: &str, mut f: impl FnMut(&str)) {
    let bytes = text.as_bytes();
    let mut buf = String::new();
    let mut i = 0;
    while i < bytes.len() {
        if !is_word_byte(bytes[i]) {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len() && is_word_byte(bytes[i]) {
            i += 1;
        }
        let word = &text[start..i];
        lower_into(&mut buf, word);
        f(&buf);
        if word.bytes().any(|b| b == b'_' || b.is_ascii_uppercase()) {
            let parts = part_ranges(word);
            if parts.len() > 1 {
                for (a, b) in parts {
                    lower_into(&mut buf, &word[a..b]);
                    f(&buf);
                }
            }
        }
    }
}

fn scan_line(text: &str, terms: &[String], counts: &mut [u32]) -> u64 {
    counts.iter_mut().for_each(|c| *c = 0);
    let mut len = 0u64;
    for_each_token(text, |tok| {
        len += 1;
        if let Some(i) = terms.iter().position(|t| t.as_str() == tok) {
            counts[i] += 1;
        }
    });
    len
}

fn for_each_file(s: &Scope, rels: &[String], mut f: impl FnMut(&str, &[u8])) {
    for rel in rels {
        let Some(target) = file_target(s, rel) else { continue };
        let Some(bytes) = read_text(&target) else { continue };
        f(rel, &bytes);
    }
}

struct Hit {
    score: f64,
    id: u64,
    rel: String,
    line: usize,
    text: String,
}

fn better(score: f64, id: u64, other: f64, other_id: u64) -> bool {
    score > other || (score == other && id < other_id)
}

fn offer(top: &mut Vec<Hit>, k: usize, score: f64, id: u64, rel: &str, line: usize, text: &str) {
    if top.len() == k {
        let worst = &top[k - 1];
        if !better(score, id, worst.score, worst.id) {
            return;
        }
        top.pop();
    }
    let at = top
        .iter()
        .position(|hit| better(score, id, hit.score, hit.id))
        .unwrap_or(top.len());
    top.insert(
        at,
        Hit {
            score,
            id,
            rel: rel.to_string(),
            line,
            text: snippet(text),
        },
    );
}

fn bm25(s: &Scope, rels: &[String], terms: &[String], k: usize) -> Vec<Hit> {
    let mut counts = vec![0u32; terms.len()];
    let mut docs = 0u64;
    let mut total = 0u64;
    let mut df = vec![0u64; terms.len()];
    for_each_file(s, rels, |_, bytes| {
        each_line(bytes, |_, text| {
            let len = scan_line(text, terms, &mut counts);
            if len > 0 {
                docs += 1;
                total += len;
                for (d, c) in df.iter_mut().zip(counts.iter()) {
                    if *c > 0 {
                        *d += 1;
                    }
                }
            }
        })
    });
    let n = docs as f64;
    let avg = total as f64 / docs.max(1) as f64;
    let idf: Vec<f64> = df
        .iter()
        .map(|&d| {
            let d = d as f64;
            (1.0 + (n - d + 0.5) / (d + 0.5)).ln()
        })
        .collect();
    let mut top: Vec<Hit> = Vec::with_capacity(k + 1);
    let mut id = 0u64;
    for_each_file(s, rels, |rel, bytes| {
        each_line(bytes, |line, text| {
            let len = scan_line(text, terms, &mut counts);
            if len == 0 {
                return;
            }
            let this = id;
            id += 1;
            let mut score = 0.0;
            let mut matched = false;
            for (i, &tf) in counts.iter().enumerate() {
                if tf == 0 {
                    continue;
                }
                let tf = tf as f64;
                let weight = (tf * (K1 + 1.0)) / (tf + K1 * (1.0 - B + (B * len as f64) / avg));
                score += idf[i] * weight;
                matched = true;
            }
            if matched {
                offer(&mut top, k, score, this, rel, line, text);
            }
        })
    });
    top
}

fn ranked_op(s: &Scope, listing: Listing, args: &Value) -> Value {
    let Some(query) = args
        .get("query")
        .and_then(Value::as_str)
        .filter(|q| !q.trim().is_empty())
    else {
        return fail("query must be a non-empty string");
    };
    let k = match int_arg(args, "k", DEFAULT_K) {
        Some(k) if (1..=MAX_K).contains(&k) => k as usize,
        _ => return fail(&format!("k must be an integer from 1 to {MAX_K}")),
    };
    let closure = match args.get("closure") {
        None => false,
        Some(Value::Bool(flag)) => *flag,
        Some(_) => return fail("closure must be true or false"),
    };
    let mut seen: HashSet<String> = HashSet::new();
    let mut terms: Vec<String> = Vec::new();
    for_each_token(query, |tok| {
        if !seen.contains(tok) {
            seen.insert(tok.to_string());
            terms.push(tok.to_string());
        }
    });
    if terms.is_empty() {
        return fail("query has no word characters");
    }
    let rels = match tracked(&s.root, listing) {
        Ok(rels) => rels,
        Err(error) => return fail(&error),
    };
    let hits = bm25(s, &rels, &terms, k);
    let mut lines: Vec<String> = hits
        .iter()
        .map(|hit| format!("{}:{} {:.3} {}", shown_at(s, &hit.rel), hit.line, hit.score, hit.text))
        .collect();
    if closure {
        lines.extend(closure_lines(s, &rels, &hits));
    }
    let output = if lines.is_empty() {
        "(no match)".to_string()
    } else {
        lines.join("\n")
    };
    json!({"ok": true, "output": output})
}

fn is_js(rel: &str) -> bool {
    rel.ends_with(".js") || rel.ends_with(".mjs") || rel.ends_with(".cjs")
}

fn closure_lines(s: &Scope, rels: &[String], hits: &[Hit]) -> Vec<String> {
    let files: Vec<(String, String)> = rels
        .iter()
        .filter(|rel| is_js(rel))
        .filter_map(|rel| {
            let bytes = read_text(&file_target(s, rel)?)?;
            Some((rel.clone(), String::from_utf8(bytes).ok()?))
        })
        .collect();
    let anchors: Vec<(String, usize)> = hits.iter().map(|hit| (hit.rel.clone(), hit.line)).collect();
    crate::code::closure_lines(&files, &anchors, CLOSURE_ANCHORS, CLOSURE_CAP)
        .into_iter()
        .map(|row| format!("{}:{} closure {}", shown_at(s, &row.file), row.start, snippet(row.text.trim_end_matches('\r'))))
        .collect()
}

enum Matcher {
    Plain { needle: String, fold: bool },
    Word { needle: String, fold: bool },
    Regex(regex::Regex),
}

impl Matcher {
    fn new(pattern: &str, mode: &str, ignore: bool) -> Result<Matcher, String> {
        let needle = if ignore { pattern.to_lowercase() } else { pattern.to_string() };
        match mode {
            "regex" => regex::RegexBuilder::new(pattern)
                .case_insensitive(ignore)
                .build()
                .map(Matcher::Regex)
                .map_err(|e| e.to_string()),
            "word" => Ok(Matcher::Word { needle, fold: ignore }),
            _ => Ok(Matcher::Plain { needle, fold: ignore }),
        }
    }

    fn matches(&self, text: &str) -> bool {
        match self {
            Matcher::Plain { needle, fold } => {
                if *fold {
                    text.to_lowercase().contains(needle.as_str())
                } else {
                    text.contains(needle.as_str())
                }
            }
            Matcher::Word { needle, fold } => {
                if *fold {
                    has_word(&text.to_lowercase(), needle)
                } else {
                    has_word(text, needle)
                }
            }
            Matcher::Regex(re) => re.is_match(text),
        }
    }
}

struct Page {
    found: Vec<String>,
    total: u64,
    next: Option<u64>,
}

fn scan_hits(
    s: &Scope,
    base: &Path,
    names: &[String],
    inside: bool,
    matcher: &Matcher,
    limit: usize,
    offset: u64,
) -> Page {
    let mut page = Page {
        found: Vec::new(),
        total: 0,
        next: None,
    };
    let mut size = 0usize;
    for name in names {
        let at = if name.is_empty() { base.to_path_buf() } else { base.join(name) };
        let (target, shown) = if inside && s.confined {
            let rel = rel_slash(&s.root, &at);
            match confined_target(s, &rel) {
                Some(target) => (target, rel),
                None => continue,
            }
        } else {
            (at.clone(), at.to_string_lossy().into_owned())
        };
        let Some(bytes) = read_text(&target) else { continue };
        each_line(&bytes, |number, text| {
            if !matcher.matches(text) {
                return;
            }
            page.total += 1;
            if page.total <= offset || page.next.is_some() {
                return;
            }
            let row = format!("{shown}:{number} {}", snippet(text));
            let units = utf16_len(&row);
            if page.found.len() >= limit || size + units > OUTPUT_PAGE {
                page.next = Some(page.total - 1);
                return;
            }
            size += units + 1;
            page.found.push(row);
        });
    }
    page
}

fn skipped_directory(name: &str, vendor: bool) -> bool {
    if name.starts_with('.') || name == "runs" {
        return true;
    }
    !vendor && (name == "node_modules" || name == "vendor")
}

fn walk_files(base: &Path, all: bool, vendor: bool) -> (Vec<String>, u64) {
    if fs::symlink_metadata(base).map(|m| m.is_file()).unwrap_or(false) {
        return (vec![String::new()], 0);
    }
    let mut paths = Vec::new();
    let mut skipped = 0u64;
    let mut pending = vec![String::new()];
    while let Some(dir) = pending.pop() {
        let at = if dir.is_empty() { base.to_path_buf() } else { base.join(&dir) };
        let Ok(entries) = fs::read_dir(&at) else { continue };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let rel = if dir.is_empty() { name.clone() } else { format!("{dir}/{name}") };
            let Ok(kind) = entry.file_type() else { continue };
            if kind.is_dir() {
                if STATE_DIRS.contains(&name.as_str()) {
                    continue;
                }
                if !all && skipped_directory(&name, vendor) {
                    skipped += 1;
                } else {
                    pending.push(rel);
                }
            } else if kind.is_file() {
                paths.push(rel);
            }
        }
    }
    paths.sort();
    (paths, skipped)
}

fn walk_skip_hint(dirs: u64) -> Option<String> {
    if dirs == 0 {
        return None;
    }
    Some(format!(
        "{} skipped (dot-directories, node_modules, vendor or runs); use scope: 'all' to include them",
        plural(dirs as usize, "directory", "directories")
    ))
}

fn skipped_hint(root: &Path, list: &[String], under: &str, vendor: bool) -> Option<String> {
    let every = match tracked(root, ALL_FILES) {
        Ok(v) => v,
        Err(_) => return Some("skipped files not counted: git ls-files failed or ran past its limit".to_string()),
    };
    let untracked = match tracked(root, UNTRACKED) {
        Ok(v) => v,
        Err(_) => return Some("skipped files not counted: git ls-files failed or ran past its limit".to_string()),
    };
    let tracked_set: HashSet<&str> = list.iter().map(String::as_str).collect();
    let loose: HashSet<&str> = untracked.iter().map(String::as_str).collect();
    let skipped: Vec<String> = under_dir(&every, under, true)
        .into_iter()
        .filter(|rel| {
            !rel.ends_with('/')
                && if tracked_set.contains(rel.as_str()) {
                    !vendor && is_vendor(rel)
                } else {
                    !in_state(rel)
                }
        })
        .collect();
    if skipped.is_empty() {
        return None;
    }
    let n = skipped.len();
    let untracked_count = skipped
        .iter()
        .filter(|rel| !tracked_set.contains(rel.as_str()) && loose.contains(rel.as_str()) && !is_vendor(rel))
        .count();
    Some(format!(
        "{} skipped ({} untracked, {} gitignored or in node_modules); use scope: 'all' to include them",
        plural(n, "file", "files"),
        untracked_count,
        n - untracked_count
    ))
}

fn grep_op(s: &Scope, args: &Value) -> Value {
    let pattern = match args.get("pattern").and_then(Value::as_str) {
        Some(p) if !p.is_empty() => p,
        _ => return fail("pattern must be a non-empty string"),
    };
    if pattern.encode_utf16().count() > MAX_PATTERN_UNITS {
        return fail("pattern must be at most 200 characters");
    }
    let mode = match args.get("mode") {
        None => "literal",
        Some(Value::String(m)) => match m.as_str() {
            "content" | "literal" => "literal",
            "regex" => "regex",
            "word" => "word",
            _ => return fail("mode must be one of literal, regex, word"),
        },
        Some(_) => return fail("mode must be one of literal, regex, word"),
    };
    let scope_mode = match args.get("scope") {
        None => "tracked",
        Some(Value::String(v)) => match v.as_str() {
            "tracked" => "tracked",
            "all" => "all",
            _ => return fail("scope must be one of tracked, all"),
        },
        Some(_) => return fail("scope must be one of tracked, all"),
    };
    let ignore = args.get("ignoreCase") == Some(&Value::Bool(true));
    let limit = match int_arg(args, "limit", DEFAULT_LIMIT) {
        Some(l) if (1..=MAX_LIMIT).contains(&l) => l as usize,
        _ => return fail(&format!("limit must be an integer from 1 to {MAX_LIMIT}")),
    };
    let vendor = match args.get("vendor") {
        None => false,
        Some(Value::Bool(v)) => *v,
        Some(_) => return fail("vendor must be true or false"),
    };
    let path = match args.get("path") {
        None => None,
        Some(Value::String(p)) if !p.is_empty() => Some(p.as_str()),
        Some(_) => return fail("path must be a non-empty string"),
    };
    let offset_arg = int_arg(args, "offset", 0);
    let matcher = match Matcher::new(pattern, mode, ignore) {
        Ok(m) => m,
        Err(error) => return fail(&format!("bad pattern: {error}")),
    };
    let all = scope_mode == "all";
    let target = resolve(&s.root, path.unwrap_or("."));
    if fs::symlink_metadata(&target).is_err() {
        return fail(&format!("no such path: {}", path.unwrap_or("")));
    }
    let inside = target.strip_prefix(&s.root).is_ok();
    let under = if inside { rel_slash(&s.root, &target) } else { String::new() };
    let listed = if inside { tracked(&s.root, TRACKED).ok() } else { None };
    let (source, base, names, walk_skipped) = match &listed {
        Some(list) => {
            let names = if all {
                let every = match tracked(&s.root, ALL_FILES) {
                    Ok(every) => every,
                    Err(_) => return fail("scope all: git ls-files failed or ran past its 30 s limit"),
                };
                let tracked_set: HashSet<&str> = list.iter().map(String::as_str).collect();
                under_dir(&every, &under, true)
                    .into_iter()
                    .filter(|rel| !rel.ends_with('/') && (tracked_set.contains(rel.as_str()) || !in_state(rel)))
                    .collect()
            } else {
                under_dir(list, &under, vendor)
            };
            ("git", s.root.clone(), names, None)
        }
        None => {
            let (names, skipped) = walk_files(&target, all, vendor);
            ("walk", target.clone(), names, Some(skipped))
        }
    };
    let offset = match offset_arg {
        Some(o) if o >= 0 => o as u64,
        _ => return fail("offset must be a whole number, zero or more"),
    };
    let mut page = scan_hits(s, &base, &names, inside, &matcher, limit, offset);
    let mut hints: Vec<String> = Vec::new();
    if page.total == 0 && mode == "literal" && pattern.chars().any(|c| REGEX_CHARS.contains(c)) {
        let regex = match Matcher::new(pattern, "regex", ignore) {
            Ok(m) => m,
            Err(error) => {
                return fail(&format!(
                    "no literal match, and the pattern is not a valid regex: {error}"
                ))
            }
        };
        page = scan_hits(s, &base, &names, inside, &regex, limit, offset);
        hints.push(if page.total > 0 {
            "no literal match; matched as regex".to_string()
        } else {
            "no match as literal or regex".to_string()
        });
    }
    let skipped = if all || page.total > 0 {
        None
    } else if let Some(dirs) = walk_skipped {
        walk_skip_hint(dirs)
    } else {
        listed
            .as_ref()
            .and_then(|list| skipped_hint(&s.root, list, &under, vendor))
    };
    if let Some(hint) = skipped {
        hints.push(hint);
    }
    let footer = format!(
        "({} matches; showing {} to {}; {})",
        page.total,
        if page.found.is_empty() { 0 } else { offset },
        offset + page.found.len() as u64,
        match page.next {
            None => "no more".to_string(),
            Some(n) => format!("next offset {n}"),
        }
    );
    let mut lines = page.found;
    lines.push(footer);
    lines.extend(hints.iter().map(|hint| format!("hint: {hint}")));
    let mut result = json!({"ok": true, "files": source, "output": lines.join("\n")});
    if !hints.is_empty() {
        result["hint"] = json!(hints.join(" | "));
    }
    result
}

fn valid_page(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

fn valid_source(id: &str) -> bool {
    id.len() == 12 && id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn one_line(text: &str) -> bool {
    !text.trim().is_empty() && !text.contains(['\r', '\n']) && text.is_ascii()
}

pub fn ascii_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if c.is_ascii() {
            out.push(c);
        } else {
            let mut units = [0u16; 2];
            for unit in c.encode_utf16(&mut units).iter() {
                out.push_str(&format!("\\u{unit:04x}"));
            }
        }
    }
    out
}

pub fn source_text(root: &Path, id: &str) -> Option<String> {
    if !valid_source(id) {
        return None;
    }
    fs::read(root.join("sources").join(format!("{id}.txt")))
        .ok()
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

fn iso_from_ms(ms: i64) -> String {
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000
    )
}

fn parse_iso_ms(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let num = |a: usize, b: usize| text.get(a..b)?.parse::<i64>().ok();
    let year = num(0, 4)?;
    let month = num(5, 7)?;
    let day = num(8, 10)?;
    let hour = num(11, 13)?;
    let minute = num(14, 16)?;
    let second = num(17, 19)?;
    let tail = text.get(19..)?;
    let millis = if tail == "Z" {
        0
    } else {
        let frac = tail.strip_prefix('.')?.strip_suffix('Z')?;
        if frac.is_empty() || frac.len() > 9 || !frac.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        format!("{:0<3}", &frac[..frac.len().min(3)]).parse::<i64>().ok()?
    };
    let days = days_from_civil(year, month, day);
    Some(((days * 24 + hour) * 60 + minute) * 60_000 + second * 1000 + millis)
}

fn newer(a: &str, b: &str) -> bool {
    matches!((parse_iso_ms(a), parse_iso_ms(b)), (Some(x), Some(y)) if x > y)
}

fn now_iso() -> String {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    iso_from_ms(ms)
}

fn wiki_write(root: &Path, run: &crate::run::Run, args: &Value) -> Value {
    let refuse = |reason: &str| fail(&format!("refused: {reason}"));
    let page = match args.get("page").and_then(Value::as_str) {
        Some(p) if valid_page(p) => p.to_string(),
        _ => return refuse("page must be lowercase letters, digits and hyphens, at most 64 characters"),
    };
    let title = match args.get("title") {
        None | Some(Value::Null) => page.clone(),
        Some(Value::String(t)) if one_line(t) => t.clone(),
        Some(_) => return refuse("title must be one line of ASCII"),
    };
    let body = match args.get("body").and_then(Value::as_str) {
        Some(b) if b.is_ascii() => b,
        _ => return refuse("body must be ASCII text; write non-ASCII as \\uXXXX"),
    };
    let summary = match args.get("summary").and_then(Value::as_str) {
        Some(s) if one_line(s) => s,
        _ => return refuse("summary must be one line of ASCII"),
    };
    let kind = match args.get("kind") {
        None | Some(Value::Null) => "ingest",
        Some(Value::String(k)) if KINDS.contains(&k.as_str()) => k.as_str(),
        Some(_) => return refuse("kind must be one of ingest, answer, lint"),
    };
    let sources: Vec<&str> = match args.get("sources") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => match items.iter().map(Value::as_str).collect::<Option<Vec<&str>>>() {
            Some(ids) => ids,
            None => return refuse("sources must be a non-empty list of source ids"),
        },
        Some(_) => return refuse("sources must be a non-empty list of source ids"),
    };
    if sources.is_empty() || !sources.iter().all(|id| valid_source(id)) {
        return refuse("sources must be a non-empty list of source ids");
    }
    for id in &sources {
        if source_text(root, id).is_none() {
            return refuse(&format!("source {id} is not in sources/"));
        }
    }
    let claims: Vec<&Value> = match args.get("claims") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => items.iter().collect(),
        Some(_) => return refuse("claims must be a list of {quote, source}"),
    };
    let mut claim_out: Vec<Value> = Vec::new();
    for (i, claim) in claims.iter().enumerate() {
        let quote = claim.get("quote").and_then(Value::as_str).filter(|q| !q.is_empty());
        let source = claim.get("source").and_then(Value::as_str);
        let (Some(quote), Some(source)) = (quote, source) else {
            return refuse(&format!("claim {} needs a quote and a source that is listed in sources", i + 1));
        };
        if !sources.contains(&source) {
            return refuse(&format!("claim {} needs a quote and a source that is listed in sources", i + 1));
        }
        let Some(text) = source_text(root, source) else {
            return refuse(&format!("claim {}: the quote is not in source {source}", i + 1));
        };
        let quote_ascii = ascii_text(quote);
        if !text.contains(&quote_ascii) {
            return refuse(&format!("claim {}: the quote is not in source {source}", i + 1));
        }
        claim_out.push(json!({"quote": quote_ascii, "source": source}));
    }
    let contradicts = match page_list(args, "contradicts") {
        Some(list) => list,
        None => return refuse("contradicts and resolves must be lists of page names"),
    };
    let resolves = match page_list(args, "resolves") {
        Some(list) => list,
        None => return refuse("contradicts and resolves must be lists of page names"),
    };
    let wiki_dir = root.join("wiki");
    if let Err(e) = fs::create_dir_all(&wiki_dir) {
        return fail(&e.to_string());
    }
    let now = now_iso();
    let front = [
        "---".to_string(),
        format!("title: {}", json!(title)),
        format!("updated: {}", json!(now)),
        format!("sources: {}", json!(sources)),
        format!("claims: {}", Value::Array(claim_out.clone())),
        format!("contradicts: {}", json!(contradicts)),
        format!("resolves: {}", json!(resolves)),
        "---".to_string(),
    ]
    .join("\n");
    let body_nl = if body.ends_with('\n') { body.to_string() } else { format!("{body}\n") };
    if let Err(e) = fs::write(wiki_dir.join(format!("{page}.md")), format!("{front}\n{body_nl}")) {
        return fail(&e.to_string());
    }
    let index_path = wiki_dir.join("index.md");
    let existing = fs::read_to_string(&index_path).unwrap_or_else(|_| "# Index\n\n".to_string());
    let mut rows: Vec<String> = existing.split('\n').map(str::to_string).collect();
    if rows.last().map(String::as_str) == Some("") {
        rows.pop();
    }
    let entry = format!("- [[{page}]] - {summary}");
    let prefix = format!("- [[{page}]] - ");
    match rows.iter().position(|row| row.starts_with(&prefix)) {
        Some(at) => rows[at] = entry,
        None => rows.push(entry),
    }
    if let Err(e) = fs::write(&index_path, format!("{}\n", rows.join("\n"))) {
        return fail(&e.to_string());
    }
    let log_path = wiki_dir.join("log.md");
    if !log_path.exists() {
        if let Err(e) = fs::write(&log_path, "# Log\n\n") {
            return fail(&e.to_string());
        }
    }
    let today = &now[..10];
    let appended = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .and_then(|mut f| f.write_all(format!("## [{today}] {kind} | {title}\n").as_bytes()));
    if let Err(e) = appended {
        return fail(&e.to_string());
    }
    run.event(
        "wiki",
        json!({"page": page, "writeKind": kind, "sources": sources.len(), "claims": claim_out.len()}),
    );
    let result = json!({
        "ok": true,
        "output": format!("wrote wiki/{page}.md, its index entry and its log line"),
        "page": format!("wiki/{page}.md"),
        "claims": claim_out.len(),
    });
    json!({"ok": true, "output": result.to_string()})
}

fn page_list(args: &Value, key: &str) -> Option<Vec<String>> {
    match args.get(key) {
        None | Some(Value::Null) => Some(Vec::new()),
        Some(Value::Array(items)) => items
            .iter()
            .map(|v| v.as_str().filter(|name| valid_page(name)).map(str::to_string))
            .collect(),
        Some(_) => None,
    }
}

pub struct Lint {
    pub problems: Vec<String>,
    pub pages: usize,
    pub claims: usize,
    pub skipped: bool,
}

struct WikiPage {
    title: String,
    updated: String,
    sources: Vec<String>,
    claims: Vec<(String, String)>,
    contradicts: Vec<String>,
    resolves: Vec<String>,
    body: String,
}

struct Registry {
    id: String,
    url: String,
    at: String,
}

fn text_of(v: Option<&Value>) -> String {
    v.and_then(Value::as_str).unwrap_or("").to_string()
}

fn strings_of(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|items| items.iter().filter_map(|x| x.as_str().map(str::to_string)).collect())
        .unwrap_or_default()
}

fn parse_page(text: &str) -> Option<WikiPage> {
    let rest = text.strip_prefix("---\n")?;
    let at = rest.find("\n---\n")?;
    let mut page = WikiPage {
        title: String::new(),
        updated: String::new(),
        sources: Vec::new(),
        claims: Vec::new(),
        contradicts: Vec::new(),
        resolves: Vec::new(),
        body: rest[at + 5..].to_string(),
    };
    for line in rest[..at].split('\n') {
        let Some(sep) = line.find(": ").filter(|&i| i > 0) else { continue };
        let value: Value = serde_json::from_str(&line[sep + 2..]).ok()?;
        match &line[..sep] {
            "title" => page.title = text_of(Some(&value)),
            "updated" => page.updated = text_of(Some(&value)),
            "sources" => page.sources = strings_of(&value),
            "claims" => {
                page.claims = value
                    .as_array()
                    .map(|items| {
                        items
                            .iter()
                            .map(|c| (text_of(c.get("quote")), text_of(c.get("source"))))
                            .collect()
                    })
                    .unwrap_or_default()
            }
            "contradicts" => page.contradicts = strings_of(&value),
            "resolves" => page.resolves = strings_of(&value),
            _ => {}
        }
    }
    Some(page)
}

fn entry_name(row: &str) -> Option<&str> {
    let rest = row.strip_prefix("- [[")?;
    let close = rest.find("]]")?;
    let name = &rest[..close];
    let tail = rest[close + 2..].strip_prefix(" - ")?;
    (valid_page(name) && !tail.is_empty()).then_some(name)
}

fn links_in(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(found) = text[from..].find("[[") {
        let start = from + found + 2;
        match text[start..].find(']') {
            Some(close) if close > 0 && text[start + close + 1..].starts_with(']') => {
                out.push(&text[start..start + close]);
                from = start + close + 2;
            }
            _ => from = from + found + 1,
        }
    }
    out
}

fn log_title(row: &str) -> Option<&str> {
    let rest = row.strip_prefix("## [")?;
    let date = rest.get(..10)?.as_bytes();
    let digits_ok = date.iter().enumerate().all(|(i, b)| match i {
        4 | 7 => *b == b'-',
        _ => b.is_ascii_digit(),
    });
    if !digits_ok {
        return None;
    }
    let rest = rest.get(10..)?.strip_prefix("] ")?;
    KINDS.iter().find_map(|kind| {
        let tail = rest.strip_prefix(kind)?.strip_prefix(" | ")?;
        (!tail.is_empty()).then_some(tail)
    })
}

fn registry_of(root: &Path, problems: &mut Vec<String>) -> Vec<Registry> {
    let file = root.join("sources").join("index.jsonl");
    if !file.exists() {
        return Vec::new();
    }
    let text = fs::read_to_string(&file).unwrap_or_default();
    let mut out = Vec::new();
    for line in text.split('\n').filter(|l| !l.is_empty()) {
        match serde_json::from_str::<Value>(line) {
            Ok(v) => out.push(Registry {
                id: text_of(v.get("id")),
                url: text_of(v.get("url")),
                at: text_of(v.get("at")),
            }),
            Err(_) => {
                problems.push("sources/index.jsonl is not JSON lines".to_string());
                return Vec::new();
            }
        }
    }
    out
}

pub fn lint_wiki(root: &Path) -> Lint {
    let dir = root.join("wiki");
    if !dir.exists() {
        return Lint {
            problems: Vec::new(),
            pages: 0,
            claims: 0,
            skipped: true,
        };
    }
    let mut problems: Vec<String> = Vec::new();
    let index_path = dir.join("index.md");
    let log_path = dir.join("log.md");
    if !index_path.exists() {
        problems.push("wiki/index.md is missing".to_string());
    }
    if !log_path.exists() {
        problems.push("wiki/log.md is missing".to_string());
    }
    let index_text = fs::read_to_string(&index_path).unwrap_or_default();
    let log_text = fs::read_to_string(&log_path).unwrap_or_default();
    let mut indexed: Vec<String> = Vec::new();
    let mut indexed_set: HashSet<String> = HashSet::new();
    for row in index_text.split('\n') {
        if let Some(name) = entry_name(row) {
            if indexed_set.insert(name.to_string()) {
                indexed.push(name.to_string());
            }
        }
    }
    let mut files: Vec<String> = fs::read_dir(&dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.ends_with(".md") && n != "index.md" && n != "log.md")
                .collect()
        })
        .unwrap_or_default();
    files.sort();
    let mut pages: Vec<(String, WikiPage)> = Vec::new();
    for file in files {
        let text = fs::read_to_string(dir.join(&file)).unwrap_or_default();
        match parse_page(&text) {
            Some(page) => pages.push((file[..file.len() - 3].to_string(), page)),
            None => problems.push(format!("wiki/{file} has no frontmatter")),
        }
    }
    let names: HashSet<&str> = pages.iter().map(|(name, _)| name.as_str()).collect();
    let index_names: HashSet<&str> = indexed.iter().map(String::as_str).collect();

    for (name, _) in &pages {
        if !index_names.contains(name.as_str()) {
            problems.push(format!("page {name} has no entry in wiki/index.md"));
        }
    }
    for name in &indexed {
        if !names.contains(name.as_str()) {
            problems.push(format!("wiki/index.md lists {name}, which has no page"));
        }
    }
    for target in links_in(&index_text) {
        if !names.contains(target) {
            problems.push(format!("wiki/index.md links to {target}, which has no page"));
        }
    }
    let mut linked: HashSet<&str> = HashSet::new();
    for (name, page) in &pages {
        for target in links_in(&page.body) {
            if !names.contains(target) {
                problems.push(format!("page {name} links to {target}, which has no page"));
            } else if target != name.as_str() {
                linked.insert(target);
            }
        }
    }
    for (name, _) in &pages {
        if !linked.contains(name.as_str()) {
            problems.push(format!("page {name} is an orphan: no other page links to it"));
        }
    }
    let log_titles: HashSet<&str> = log_text.split('\n').filter_map(log_title).collect();
    for (name, page) in &pages {
        if !log_titles.contains(page.title.as_str()) {
            problems.push(format!("page {name} has no line in wiki/log.md"));
        }
        for id in &page.sources {
            if source_text(root, id).is_none() {
                problems.push(format!("page {name} cites source {id}, which is not in sources/"));
            }
        }
        for (quote, source) in &page.claims {
            if !page.sources.contains(source) {
                problems.push(format!("page {name} has a claim from {source}, which it does not list"));
            }
            if let Some(text) = source_text(root, source) {
                if !text.contains(quote.as_str()) {
                    problems.push(format!("page {name}: a quote is no longer in source {source}"));
                }
            }
        }
    }
    let registry = registry_of(root, &mut problems);
    for (name, page) in &pages {
        for id in &page.sources {
            let Some(url) = registry.iter().find(|e| e.id == *id).map(|e| e.url.clone()) else {
                continue;
            };
            let Some(latest) = registry.iter().filter(|e| e.url == url).last() else {
                continue;
            };
            if latest.id != *id && newer(&latest.at, &page.updated) {
                problems.push(format!(
                    "page {name} cites source {id}, but {url} was archived again as {} at {}",
                    latest.id, latest.at
                ));
            }
        }
    }
    for (name, page) in &pages {
        for other in &page.contradicts {
            if !names.contains(other.as_str()) {
                problems.push(format!("page {name} contradicts {other}, which has no page"));
                continue;
            }
            let resolved = pages.iter().any(|(later, body)| {
                later != name
                    && (body.resolves.contains(name) || body.resolves.contains(other))
                    && newer(&body.updated, &page.updated)
            });
            if !resolved {
                problems.push(format!("open contradiction: {name} contradicts {other}"));
            }
        }
        for other in &page.resolves {
            if !names.contains(other.as_str()) {
                problems.push(format!("page {name} resolves {other}, which has no page"));
            }
        }
    }
    let claims = pages.iter().map(|(_, p)| p.claims.len()).sum();
    Lint {
        problems,
        pages: pages.len(),
        claims,
        skipped: false,
    }
}

pub fn wiki_check(root: &Path) -> Value {
    let lint = lint_wiki(root);
    if lint.skipped {
        return json!({"ok": true, "detail": "no wiki"});
    }
    if !lint.problems.is_empty() {
        let head: Vec<&str> = lint.problems.iter().take(3).map(String::as_str).collect();
        return json!({
            "ok": false,
            "detail": format!("{} problems: {}", lint.problems.len(), head.join(" | ")),
        });
    }
    json!({
        "ok": true,
        "detail": format!("{} pages, {} claims, lint clean", lint.pages, lint.claims),
    })
}
