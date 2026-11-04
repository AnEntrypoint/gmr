use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

pub const SERVED_SKILLS: [&str; 2] = ["gm", "gm-continue"];
pub const GM_SKILL_URI: &str = "skill://gm/SKILL.md";

static GM_LOADS: AtomicU64 = AtomicU64::new(0);
static LAST_GOOD: OnceLock<Mutex<HashMap<String, Arc<Skill>>>> = OnceLock::new();

pub struct SkillFile {
    pub path: String,
    pub uri: String,
    pub digest: String,
    pub size: u64,
    pub mime_type: &'static str,
}

pub struct Skill {
    pub name: String,
    pub uri: String,
    pub frontmatter: Vec<(String, String)>,
    pub body: String,
    pub files: Vec<SkillFile>,
}

impl Skill {
    pub fn field(&self, key: &str) -> &str {
        self.frontmatter.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str()).unwrap_or("")
    }

    pub fn frontmatter_json(&self) -> Value {
        let mut map = Map::new();
        for (k, v) in &self.frontmatter {
            map.insert(k.clone(), Value::String(v.clone()));
        }
        Value::Object(map)
    }

    pub fn entry(&self) -> Value {
        let resources: Vec<Value> = self.files.iter().map(|f| json!({ "uri": f.uri, "digest": f.digest, "size": f.size })).collect();
        json!({ "uri": self.uri, "frontmatter": self.frontmatter_json(), "resources": resources })
    }

    pub fn resources(&self) -> Vec<Value> {
        self.files
            .iter()
            .map(|f| {
                if f.path == "SKILL.md" {
                    json!({ "uri": f.uri, "name": self.field("name"), "description": self.field("description"), "mimeType": f.mime_type })
                } else {
                    json!({ "uri": f.uri, "name": f.path, "mimeType": f.mime_type })
                }
            })
            .collect()
    }
}

pub fn home() -> Option<&'static Path> {
    static HOME: OnceLock<Option<PathBuf>> = OnceLock::new();
    HOME.get_or_init(|| {
        if let Some(dir) = std::env::var_os("GMNEW_HOME").filter(|v| !v.is_empty()) {
            return Some(PathBuf::from(dir));
        }
        let exe = std::env::current_exe().ok()?;
        let exe = exe.canonicalize().unwrap_or(exe);
        exe.ancestors().skip(1).find(|dir| dir.join("skills").is_dir()).map(Path::to_path_buf)
    })
    .as_deref()
}

pub fn gm_loads() -> u64 {
    GM_LOADS.load(Ordering::Relaxed)
}

fn skills_dir() -> Result<PathBuf, String> {
    home().map(|h| h.join("skills")).ok_or_else(|| "skills/ not found: set GMNEW_HOME to the gm-new checkout".to_string())
}

fn is_served(name: &str) -> bool {
    SERVED_SKILLS.contains(&name)
}

pub fn frontmatter_end(text: &str) -> Option<(usize, usize)> {
    let rest = text.strip_prefix("---\n")?;
    let at = rest.find("\n---")?;
    let mut end = 4 + at + 4;
    if text.as_bytes().get(end) == Some(&b'\n') {
        end += 1;
    }
    Some((4 + at, end))
}

fn plain_problem(value: &str) -> Option<&'static str> {
    if value.starts_with(|c: char| "-?:,[]{}#&*!|>'\"%@`".contains(c)) {
        return Some("starts with a character YAML reads as syntax");
    }
    if value.contains(": ") || value.ends_with(':') {
        return Some("has ': ' inside it, which YAML reads as a mapping");
    }
    if value.contains(" #") {
        return Some("has ' #' inside it, which YAML reads as a comment");
    }
    None
}

fn field_of(line: &str) -> Option<(&str, &str)> {
    let colon = line.find(':')?;
    let key = &line[..colon];
    let mut chars = key.chars();
    let first_ok = chars.next().is_some_and(|c| c.is_ascii_alphabetic());
    if !first_ok || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
        return None;
    }
    let after = &line[colon + 1..];
    let value = after.trim_start_matches(|c: char| c.is_whitespace() && c != '\r');
    if value.len() == after.len() || value.contains('\r') {
        return None;
    }
    Some((key, value))
}

pub fn parse_skill(text: &str, dir: &str) -> Result<(Vec<(String, String)>, String), String> {
    let (inner_end, end) = frontmatter_end(text).ok_or_else(|| format!("skills/{dir}/SKILL.md has no frontmatter"))?;
    let mut frontmatter: Vec<(String, String)> = Vec::new();
    for line in text[4..inner_end].split('\n') {
        let (key, value) = field_of(line).ok_or_else(|| {
            let head: String = line.chars().take(40).collect();
            format!("skills/{dir}/SKILL.md: frontmatter line is not key: value: {head}")
        })?;
        if let Some(problem) = plain_problem(value) {
            return Err(format!("skills/{dir}/SKILL.md: {key} {problem}; reword it"));
        }
        match frontmatter.iter_mut().find(|(k, _)| k == key) {
            Some(slot) => slot.1 = value.to_string(),
            None => frontmatter.push((key.to_string(), value.to_string())),
        }
    }
    for key in ["name", "description"] {
        if frontmatter.iter().find(|(k, _)| k == key).is_none_or(|(_, v)| v.trim().is_empty()) {
            return Err(format!("skills/{dir}/SKILL.md has no {key}"));
        }
    }
    let name = &frontmatter.iter().find(|(k, _)| k == "name").expect("checked").1;
    if name != dir {
        return Err(format!("skills/{dir}/SKILL.md: name is {name}, and its URI ends in {dir}; they must match"));
    }
    Ok((frontmatter, text[end..].trim().to_string()))
}

fn files_under(dir: &Path, rel: &str, out: &mut Vec<String>) -> Result<(), String> {
    let at = if rel.is_empty() { dir.to_path_buf() } else { dir.join(rel) };
    let entries = std::fs::read_dir(&at).map_err(|e| format!("{}: {e}", at.display()))?;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = if rel.is_empty() { name } else { format!("{rel}/{name}") };
        match entry.file_type() {
            Ok(t) if t.is_dir() => files_under(dir, &path, out)?,
            Ok(t) if t.is_file() => out.push(path),
            _ => {}
        }
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[(b >> 4) as usize] as char);
        s.push(DIGITS[(b & 15) as usize] as char);
    }
    s
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn mime_of(path: &str) -> &'static str {
    if path.ends_with(".md") {
        "text/markdown"
    } else {
        "text/plain"
    }
}

fn read_skill(name: &str) -> Result<Skill, String> {
    let dir = skills_dir()?.join(name);
    let mut paths = Vec::new();
    files_under(&dir, "", &mut paths)?;
    paths.sort();
    let mut files = Vec::with_capacity(paths.len());
    let mut skill_md: Option<String> = None;
    for path in paths {
        let bytes = std::fs::read(dir.join(&path)).map_err(|e| format!("skills/{name}/{path}: {e}"))?;
        if path == "SKILL.md" {
            skill_md = Some(String::from_utf8_lossy(&bytes).into_owned());
        }
        files.push(SkillFile {
            uri: format!("skill://{name}/{path}"),
            digest: format!("sha256:{}", sha256_hex(&bytes)),
            size: bytes.len() as u64,
            mime_type: mime_of(&path),
            path,
        });
    }
    let text = skill_md.ok_or_else(|| format!("skills/{name}/SKILL.md is missing"))?;
    let (frontmatter, body) = parse_skill(&text, name)?;
    Ok(Skill { name: name.to_string(), uri: format!("skill://{name}/SKILL.md"), frontmatter, body, files })
}

pub fn skill(name: &str) -> Result<Arc<Skill>, String> {
    let cache = LAST_GOOD.get_or_init(|| Mutex::new(HashMap::new()));
    match read_skill(name) {
        Ok(fresh) => {
            let fresh = Arc::new(fresh);
            cache.lock().unwrap_or_else(|e| e.into_inner()).insert(name.to_string(), fresh.clone());
            Ok(fresh)
        }
        Err(err) => cache.lock().unwrap_or_else(|e| e.into_inner()).get(name).cloned().ok_or(err),
    }
}

pub fn split_uri(uri: &str) -> Option<(&str, &str)> {
    let rest = uri.strip_prefix("skill://")?;
    let slash = rest.find('/')?;
    let (name, path) = (&rest[..slash], &rest[slash + 1..]);
    if name.is_empty() || !is_served(name) || path.is_empty() || path.split('/').any(|seg| seg.is_empty() || seg == "." || seg == "..") {
        return None;
    }
    Some((name, path))
}

pub fn file_of(uri: &str) -> Option<(Arc<Skill>, String, &'static str)> {
    let (name, path) = split_uri(uri)?;
    let skill = skill(name).ok()?;
    let mime = skill.files.iter().find(|f| f.uri == uri)?.mime_type;
    let bytes = std::fs::read(skills_dir().ok()?.join(name).join(path)).ok()?;
    if uri == GM_SKILL_URI {
        GM_LOADS.fetch_add(1, Ordering::Relaxed);
    }
    Some((skill, String::from_utf8_lossy(&bytes).into_owned(), mime))
}

pub fn read(uri: &str) -> Option<String> {
    file_of(uri).map(|(_, text, _)| text)
}

pub fn list() -> Result<Vec<Value>, String> {
    let mut out = Vec::new();
    for name in SERVED_SKILLS {
        out.extend(skill(name)?.resources());
    }
    Ok(out)
}

pub fn entries() -> Result<Vec<Value>, String> {
    SERVED_SKILLS.iter().map(|name| skill(name).map(|s| s.entry())).collect()
}

pub fn entry_of(uri: &str) -> Option<Value> {
    let name = uri.strip_prefix("skill://")?.strip_suffix("/SKILL.md")?;
    if name.contains('/') || !is_served(name) {
        return None;
    }
    skill(name).ok().map(|s| s.entry())
}

fn names_bare(text: &str, name: &str) -> bool {
    let bytes = text.as_bytes();
    let word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut from = 0;
    while let Some(at) = text[from..].find(name) {
        let start = from + at;
        let end = start + name.len();
        let before_ok = start == 0 || !(word(bytes[start - 1]) || bytes[start - 1] == b'.');
        let after_ok = end >= bytes.len() || !word(bytes[end]);
        if before_ok && after_ok {
            return true;
        }
        from = end;
    }
    false
}

pub fn unserved_names() -> Vec<&'static str> {
    crate::verbs::native().iter().map(|v| v.name).collect()
}

pub fn check(root: &Path, rule_bodies: &[String]) -> (bool, String) {
    let dir = root.join("skills");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return (true, "no skills".to_string());
    };
    let names = unserved_names();
    let mut findings: Vec<String> = Vec::new();
    let mut count = 0;
    let mut skills: Vec<String> = entries.flatten().map(|e| e.file_name().to_string_lossy().into_owned()).collect();
    skills.sort();
    for skill in &skills {
        let Ok(text) = std::fs::read_to_string(dir.join(skill).join("SKILL.md")) else {
            continue;
        };
        count += 1;
        for name in &names {
            if names_bare(&text, name) {
                findings.push(format!("{skill} names {name} without gm."));
            }
        }
        if skill == "gm" {
            let plain = text.replace("gm.", "");
            for rule in rule_bodies {
                if !plain.contains(rule.as_str()) {
                    let head: String = rule.chars().take(60).collect();
                    findings.push(format!("gm skill lacks rule: {head}"));
                }
            }
        }
    }
    for skill in SERVED_SKILLS {
        let parsed = std::fs::read_to_string(dir.join(skill).join("SKILL.md"))
            .map_err(|e| format!("skills/{skill}/SKILL.md: {e}"))
            .and_then(|text| parse_skill(&text, skill).map(|_| ()));
        if let Err(err) = parsed {
            findings.push(err);
        }
    }
    if findings.is_empty() {
        (true, format!("{count} skill(s) name only served tools; {} parse as served skills", SERVED_SKILLS.join(", ")))
    } else {
        findings.truncate(3);
        (false, findings.join(" | "))
    }
}
