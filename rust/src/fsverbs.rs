use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

use crate::types::{Ctx, Fut, VerbDef, VerbOutcome};

pub const OUTPUT_PAGE: u64 = 20_000;
const SCAN_CHUNK: usize = 64 * 1024;
const UNBOUNDED: u64 = u64::MAX;

const DESC_READ_FILE: &str = r#"Read a text file, inside or outside the workspace, one page of at most 20000 characters. Lines are the default unit: offset is a 0-based line index, limit a line count (default: as many whole lines as fit), and total (the file in lines) and next (the line index of the next page, null at the end) count lines. truncated is true when the page stops before the end, totalBytes is the file size in bytes, and hint names the read that gets the rest. A line longer than the page is cut to its first 20000 characters, with cutLine true, and next is the line after it. unit "chars" counts offset, limit (1 to 20000, default 20000), total and next in characters instead. A path that starts with skill:// reads a served skill file, the text resources/read returns, and a skill:// uri that no served skill has is an error that names it."#;
const SCHEMA_READ_FILE: &str = r#"{"type":"object","properties":{"path":{"type":"string"},"offset":{"type":"number"},"limit":{"type":"number"},"unit":{"type":"string"}},"required":["path"]}"#;
const DESC_WRITE_FILE: &str =
    "Write a text file, inside or outside the workspace, creating parent directories.";
const SCHEMA_WRITE_FILE: &str = r#"{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path","content"]}"#;
const DESC_EDIT_FILE: &str = "Replace old with new in a text file, inside or outside the workspace. old must occur once, unless replaceAll is true.";
const SCHEMA_EDIT_FILE: &str = r#"{"type":"object","properties":{"path":{"type":"string"},"old":{"type":"string"},"new":{"type":"string"},"replaceAll":{"type":"boolean"}},"required":["path","old","new"]}"#;
const DESC_STAT_FILE: &str =
    "Type, size and mtime of a path, inside or outside the workspace, as JSON.";
const SCHEMA_STAT_FILE: &str =
    r#"{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}"#;
const DESC_LIST_DIR: &str =
    r#"List a directory, inside or outside the workspace. Directories end with "/"."#;
const SCHEMA_LIST_DIR: &str =
    r#"{"type":"object","properties":{"path":{"type":"string"}},"required":[]}"#;

pub const VERBS: &[VerbDef] = &[
    VerbDef {
        name: "read_file",
        description: DESC_READ_FILE,
        schema: SCHEMA_READ_FILE,
        call: read_file,
    },
    VerbDef {
        name: "write_file",
        description: DESC_WRITE_FILE,
        schema: SCHEMA_WRITE_FILE,
        call: write_file,
    },
    VerbDef {
        name: "edit_file",
        description: DESC_EDIT_FILE,
        schema: SCHEMA_EDIT_FILE,
        call: edit_file,
    },
    VerbDef {
        name: "stat_file",
        description: DESC_STAT_FILE,
        schema: SCHEMA_STAT_FILE,
        call: stat_file,
    },
    VerbDef {
        name: "list_dir",
        description: DESC_LIST_DIR,
        schema: SCHEMA_LIST_DIR,
        call: list_dir,
    },
];

pub struct Scan {
    pub size: u64,
    pub lines: u64,
    pub chars: u64,
    pub line_start: Option<(u64, u64)>,
    pub char_start: Option<u64>,
}

pub fn scan(path: &Path, want_line: u64, want_char: u64) -> io::Result<Scan> {
    let mut file = File::open(path)?;
    let mut buf = vec![0u8; SCAN_CHUNK];
    let mut size = 0u64;
    let mut newlines = 0u64;
    let mut chars = 0u64;
    let mut last = b'\n';
    let mut line_start = (want_line == 0).then_some((0, 0));
    let mut char_start = None;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        for (k, &b) in buf[..n].iter().enumerate() {
            let at = size + k as u64;
            if b & 0xC0 != 0x80 {
                if chars == want_char && char_start.is_none() {
                    char_start = Some(at);
                }
                chars += 1;
            }
            if b == b'\n' {
                newlines += 1;
                if newlines == want_line {
                    line_start = Some((at + 1, chars));
                }
            }
        }
        size += n as u64;
        last = buf[n - 1];
    }
    let lines = newlines + u64::from(size > 0 && last != b'\n');
    if char_start.is_none() && chars == want_char {
        char_start = Some(size);
    }
    Ok(Scan {
        size,
        lines,
        chars,
        line_start,
        char_start,
    })
}

pub fn char_total(path: &Path) -> io::Result<u64> {
    Ok(scan(path, UNBOUNDED, UNBOUNDED)?.chars)
}

pub fn chars_window(path: &Path, from: u64, count: u64) -> io::Result<String> {
    match scan(path, UNBOUNDED, from)?.char_start {
        Some(byte) => chars_from(path, byte, count),
        None => Ok(String::new()),
    }
}

fn chars_from(path: &Path, byte: u64, count: u64) -> io::Result<String> {
    let mut reader = reader_at(path, byte)?;
    Ok(lossy(take(&mut reader, count, false)?.bytes))
}

fn reader_at(path: &Path, byte: u64) -> io::Result<BufReader<File>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(byte))?;
    Ok(BufReader::with_capacity(SCAN_CHUNK, file))
}

struct Taken {
    bytes: Vec<u8>,
    chars: u64,
    over: bool,
}

fn take(reader: &mut impl BufRead, budget: u64, until_newline: bool) -> io::Result<Taken> {
    let mut bytes = Vec::new();
    let mut chars = 0u64;
    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Ok(Taken {
                bytes,
                chars,
                over: false,
            });
        }
        let mut used = 0;
        let mut stop = None;
        for &b in buf {
            if b & 0xC0 != 0x80 {
                if chars == budget {
                    stop = Some(true);
                    break;
                }
                chars += 1;
            }
            bytes.push(b);
            used += 1;
            if until_newline && b == b'\n' {
                stop = Some(false);
                break;
            }
        }
        reader.consume(used);
        if let Some(over) = stop {
            return Ok(Taken { bytes, chars, over });
        }
    }
}

pub(crate) fn lossy(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

pub(crate) fn whole(v: &Value) -> Option<u64> {
    match v {
        Value::Number(n) => n.as_u64().or_else(|| {
            n.as_f64()
                .filter(|f| f.fract() == 0.0 && *f >= 0.0)
                .map(|f| f as u64)
        }),
        _ => None,
    }
}

pub(crate) fn arg<'a>(args: &'a Value, key: &str) -> Option<&'a Value> {
    args.get(key).filter(|v| !v.is_null())
}

pub(crate) fn text_arg<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    arg(args, key).and_then(Value::as_str)
}

fn raw_path(args: &Value) -> &str {
    text_arg(args, "path").unwrap_or("")
}

pub(crate) fn error(message: impl Into<String>) -> Value {
    json!({ "ok": false, "error": message.into() })
}

fn refusal(reason: &str) -> Value {
    json!({ "ok": false, "refused": true, "error": format!("refused: {reason}") })
}

fn locate(root: &Path, rel: &str) -> Result<PathBuf, Value> {
    if rel.is_empty() {
        return Err(refusal("path must be a non-empty string"));
    }
    let root = std::path::absolute(root).unwrap_or_else(|_| root.to_path_buf());
    Ok(normalize(&root.join(rel)))
}

fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for part in path.components() {
        match part {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

fn relative_from(root: &Path, path: &Path) -> String {
    let base: Vec<Component> = root.components().collect();
    let target: Vec<Component> = path.components().collect();
    let common = base.iter().zip(&target).take_while(|(a, b)| a == b).count();
    let mut out = PathBuf::new();
    for _ in common..base.len() {
        out.push("..");
    }
    for part in &target[common..] {
        out.push(part.as_os_str());
    }
    out.display().to_string()
}

fn add_cut(reply: &mut Value, size: u64, hint: Option<String>) {
    if let Value::Object(map) = reply {
        map.insert("truncated".into(), json!(hint.is_some()));
        map.insert("totalBytes".into(), json!(size));
        if let Some(hint) = hint {
            map.insert("hint".into(), json!(hint));
        }
    }
}

fn read_file(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    Box::pin(async move {
        let raw = raw_path(&args);
        if raw.starts_with("skill://") {
            return Ok(served_skill(raw));
        }
        let path = match locate(&ctx.workspace, raw) {
            Ok(path) => path,
            Err(reply) => return Ok(reply),
        };
        let _shared = ctx.locks.shared().await;
        Ok(tokio::task::spawn_blocking(move || read_page(&path, &args))
            .await
            .unwrap_or_else(|e| error(e.to_string())))
    })
}

fn served_skill(uri: &str) -> Value {
    match crate::skills::read(uri) {
        Some(text) => json!({ "ok": true, "output": text }),
        None => error(format!("no served skill file has the uri {uri}")),
    }
}

fn read_page(path: &Path, args: &Value) -> Value {
    let unit = arg(args, "unit").map_or("lines", |v| v.as_str().unwrap_or(""));
    let offset = arg(args, "offset").map_or(Some(0), whole);
    let limit = arg(args, "limit").map(whole);
    let outcome = match unit {
        "chars" => {
            let Some(offset) = offset else {
                return error("offset must be a whole number of characters, zero or more");
            };
            let limit = match limit {
                None => OUTPUT_PAGE,
                Some(Some(limit)) if (1..=OUTPUT_PAGE).contains(&limit) => limit,
                _ => {
                    return error(format!(
                        "limit must be a whole number from 1 to {OUTPUT_PAGE}"
                    ))
                }
            };
            read_chars(path, offset, limit)
        }
        "lines" => {
            let Some(offset) = offset else {
                return error("offset must be a whole number of lines, zero or more");
            };
            let limit = match limit {
                None => UNBOUNDED,
                Some(Some(limit)) if limit >= 1 => limit,
                _ => return error("limit must be a whole number of lines, one or more"),
            };
            read_lines(path, offset, limit)
        }
        _ => return error("unit must be lines or chars"),
    };
    outcome.unwrap_or_else(|e| error(e.to_string()))
}

fn read_chars(path: &Path, offset: u64, limit: u64) -> io::Result<Value> {
    let found = scan(path, UNBOUNDED, offset)?;
    let total = found.chars;
    let end = total.min(offset.saturating_add(limit));
    let output = if offset < end {
        chars_from(path, found.char_start.unwrap_or(found.size), end - offset)?
    } else {
        String::new()
    };
    let next = (end < total).then_some(end);
    let mut reply =
        json!({ "ok": true, "output": output, "total": total, "offset": offset, "next": next });
    add_cut(
        &mut reply,
        found.size,
        next.map(|n| {
            format!(r#"pass unit: "chars", offset: {n} and limit: {OUTPUT_PAGE} to read the rest"#)
        }),
    );
    Ok(reply)
}

fn read_lines(path: &Path, offset: u64, limit: u64) -> io::Result<Value> {
    let found = scan(path, offset, UNBOUNDED)?;
    let total = found.lines;
    if offset >= total {
        let mut reply = json!({ "ok": true, "unit": "lines", "output": "", "total": total, "offset": offset, "next": Value::Null });
        add_cut(&mut reply, found.size, None);
        return Ok(reply);
    }
    let (begin, begin_char) = found.line_start.unwrap_or((found.size, found.chars));
    let mut reader = reader_at(path, begin)?;
    let first = take(&mut reader, OUTPUT_PAGE, true)?;
    if first.over {
        let next = (offset + 1 < total).then_some(offset + 1);
        let mut reply = json!({ "ok": true, "unit": "lines", "output": lossy(first.bytes), "cutLine": true, "total": total, "offset": offset, "next": next });
        let hint = format!(
            r#"line {offset} is longer than a page: pass unit: "chars", offset: {} and limit: {OUTPUT_PAGE} to read the rest"#,
            begin_char + OUTPUT_PAGE
        );
        add_cut(&mut reply, found.size, Some(hint));
        return Ok(reply);
    }
    let mut bytes = first.bytes;
    let mut used = first.chars;
    let mut count = 1u64;
    while count < limit && offset + count < total {
        let line = take(&mut reader, OUTPUT_PAGE - used, true)?;
        if line.over {
            break;
        }
        used += line.chars;
        bytes.extend_from_slice(&line.bytes);
        count += 1;
    }
    let end_line = offset + count;
    let next = (end_line < total).then_some(end_line);
    let mut reply = json!({ "ok": true, "unit": "lines", "output": lossy(bytes), "total": total, "offset": offset, "next": next });
    add_cut(
        &mut reply,
        found.size,
        next.map(|n| format!("pass offset: {n} and limit: {} to read the rest", total - n)),
    );
    Ok(reply)
}

fn write_file(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    Box::pin(async move {
        let path = match locate(&ctx.workspace, raw_path(&args)) {
            Ok(path) => path,
            Err(reply) => return Ok(reply),
        };
        let Some(content) = text_arg(&args, "content") else {
            return Ok(error("content must be a string"));
        };
        let _shared = ctx.locks.shared().await;
        let _ordered = ctx.locks.path(&path).await;
        if let Some(parent) = path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                return Ok(error(e.to_string()));
            }
        }
        Ok(match tokio::fs::write(&path, content).await {
            Ok(()) => {
                let root =
                    std::path::absolute(&ctx.workspace).unwrap_or_else(|_| ctx.workspace.clone());
                json!({ "ok": true, "output": format!("wrote {} ({} chars)", relative_from(&root, &path), content.encode_utf16().count()) })
            }
            Err(e) => error(e.to_string()),
        })
    })
}

fn edit_file(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    Box::pin(async move {
        let raw = raw_path(&args);
        let path = match locate(&ctx.workspace, raw) {
            Ok(path) => path,
            Err(reply) => return Ok(reply),
        };
        let old = match text_arg(&args, "old") {
            Some(old) if !old.is_empty() => old,
            _ => return Ok(error("old must be a non-empty string")),
        };
        let Some(new) = text_arg(&args, "new") else {
            return Ok(error("new must be a string"));
        };
        let replace_all = arg(&args, "replaceAll") == Some(&Value::Bool(true));
        let _shared = ctx.locks.shared().await;
        let _ordered = ctx.locks.path(&path).await;
        let text = match tokio::fs::read_to_string(&path).await {
            Ok(text) => text,
            Err(e) => return Ok(error(e.to_string())),
        };
        let count = text.matches(old).count();
        if count == 0 {
            return Ok(error(format!("old text not found in {raw}")));
        }
        if count > 1 && !replace_all {
            return Ok(error(format!("old text occurs {count} times in {raw}; add context to make it unique, or set replaceAll")));
        }
        if let Err(e) = tokio::fs::write(&path, text.replace(old, new)).await {
            return Ok(error(e.to_string()));
        }
        let root = std::path::absolute(&ctx.workspace).unwrap_or_else(|_| ctx.workspace.clone());
        let plural = if count == 1 { "" } else { "s" };
        Ok(
            json!({ "ok": true, "output": format!("edited {} ({count} replacement{plural})", relative_from(&root, &path)) }),
        )
    })
}

fn stat_file(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    Box::pin(async move {
        let path = match locate(&ctx.workspace, raw_path(&args)) {
            Ok(path) => path,
            Err(reply) => return Ok(reply),
        };
        let _shared = ctx.locks.shared().await;
        Ok(match tokio::fs::metadata(&path).await {
            Ok(info) => {
                let kind = if info.is_dir() {
                    "dir"
                } else if info.is_file() {
                    "file"
                } else {
                    "other"
                };
                let mtime = info.modified().map(iso_time).unwrap_or_default();
                json!({ "ok": true, "output": format!(r#"{{"type":"{kind}","size":{},"mtime":"{mtime}"}}"#, info.len()) })
            }
            Err(e) => error(e.to_string()),
        })
    })
}

fn iso_time(time: SystemTime) -> String {
    let since = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = since.as_secs() as i64;
    let millis = since.subsec_millis();
    let days = secs.div_euclid(86_400) + 719_468;
    let day_secs = secs.rem_euclid(86_400);
    let era = days.div_euclid(146_097);
    let doe = days - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        day_secs / 3600,
        day_secs % 3600 / 60,
        day_secs % 60
    )
}

fn list_dir(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    Box::pin(async move {
        let raw = arg(&args, "path").map_or(".", |v| v.as_str().unwrap_or(""));
        let path = match locate(&ctx.workspace, raw) {
            Ok(path) => path,
            Err(reply) => return Ok(reply),
        };
        let _shared = ctx.locks.shared().await;
        Ok(match list(&path).await {
            Ok(output) => json!({ "ok": true, "output": output }),
            Err(e) => error(e.to_string()),
        })
    })
}

async fn list(path: &Path) -> io::Result<String> {
    let mut entries = tokio::fs::read_dir(path).await?;
    let mut names = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name().to_string_lossy().into_owned();
        let is_dir = entry.file_type().await?.is_dir();
        names.push(if is_dir { format!("{name}/") } else { name });
    }
    names.sort();
    Ok(names.join("\n"))
}
