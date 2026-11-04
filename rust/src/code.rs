use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, UNIX_EPOCH};

use oxc_allocator::Allocator;
use oxc_ast::ast::*;
use oxc_ast_visit::{walk, Visit};
use oxc_parser::Parser;
use oxc_span::SourceType;
use oxc_syntax::scope::ScopeFlags;
use regex::Regex;
use serde_json::{json, Value};

use crate::types::{Ctx, Fut, VerbDef, VerbOutcome};

const MAX_FILE_BYTES: u64 = 1_000_000;
const MAX_LIST_BYTES: usize = 64 * 1024 * 1024;
const LIST_TIMEOUT: Duration = Duration::from_secs(30);
const OUTPUT_PAGE: usize = 20_000;
const DEFAULT_LIMIT: u64 = 100;
const MAX_LIMIT: u64 = 1000;
const DEFAULT_DEPTH: u64 = 2;
const MAX_DEPTH: u64 = 5;
const NOTE: &str = "Exact for parsed JavaScript and TypeScript: a call is a call expression whose callee is the name, or a member call .name(). Not resolved: shadowing, aliases and dynamic calls. A file that does not parse is scanned by lines and marked approximate.";
const KEYWORDS: &[&str] = &[
    "await", "break", "case", "catch", "class", "const", "continue", "debugger", "default", "delete", "do", "else", "enum", "export", "extends", "false", "finally", "for", "function", "if", "import", "in", "instanceof", "let", "new", "null", "return", "static", "super", "switch", "this", "throw", "true", "try", "typeof", "var", "void", "while", "with", "yield",
];
const CALLERS_DESCRIPTION: &str = "Call sites of an identifier in the tracked and untracked .js, .mjs, .cjs and .ts files, as path:line [kind] text. kind is linked (a direct call in a file that imports a defining file), direct, or member (.name()). Exact for parsed JavaScript and TypeScript; files that do not parse are scanned by lines and marked. limit is 1 to 1000, default 100.";
const CALLERS_SCHEMA: &str = r#"{"type":"object","properties":{"name":{"type":"string"},"limit":{"type":"number"}},"required":["name"]}"#;
const IMPACT_DESCRIPTION: &str = "The files that define an identifier, the files that call it, and the files that import those, out to depth levels (default 2, max 5). Import edges come from parsed JavaScript and resolve inside the repository. Approximate where a file is scanned by lines.";
const IMPACT_SCHEMA: &str = r#"{"type":"object","properties":{"name":{"type":"string"},"depth":{"type":"number"}},"required":["name"]}"#;

pub const VERBS: &[VerbDef] = &[
    VerbDef {
        name: "callers",
        description: CALLERS_DESCRIPTION,
        schema: CALLERS_SCHEMA,
        call: callers_call,
    },
    VerbDef {
        name: "impact",
        description: IMPACT_DESCRIPTION,
        schema: IMPACT_SCHEMA,
        call: impact_call,
    },
];

fn callers_call(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    Box::pin(callers_verb(ctx, args))
}

fn impact_call(ctx: Arc<Ctx>, args: Value) -> Fut<VerbOutcome> {
    Box::pin(impact_verb(ctx, args))
}

fn fail(error: &str) -> Value {
    json!({ "ok": false, "error": error })
}

fn cut(text: String) -> String {
    match text.char_indices().nth(OUTPUT_PAGE) {
        Some((at, _)) => text[..at].to_string(),
        None => text,
    }
}

fn bad_name(value: Option<&Value>) -> Option<String> {
    let Some(name) = value.and_then(Value::as_str).filter(|name| is_identifier(name)) else {
        return Some("name must be a JavaScript identifier".to_string());
    };
    if KEYWORDS.contains(&name) {
        return Some(format!("{name} is a JavaScript keyword, so it has no callers to list"));
    }
    None
}

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' || first == '$' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

async fn callers_verb(ctx: Arc<Ctx>, args: Value) -> VerbOutcome {
    if let Some(bad) = bad_name(args.get("name")) {
        return Ok(fail(&bad));
    }
    let name = args.get("name").and_then(Value::as_str).unwrap_or_default().to_string();
    let limit = match args.get("limit") {
        None => DEFAULT_LIMIT,
        Some(value) => match value.as_u64().filter(|limit| (1..=MAX_LIMIT).contains(limit)) {
            Some(limit) => limit,
            None => return Ok(fail(&format!("limit must be an integer from 1 to {MAX_LIMIT}"))),
        },
    };
    let index = match load_index(ctx.workspace.clone()).await {
        Ok(index) => index,
        Err(error) => return Ok(fail(&error)),
    };
    let found = scan(&index, &name);
    let mut lines: Vec<String> = found
        .sites
        .iter()
        .take(limit as usize)
        .map(|site| format!("{}:{} [{}] {}", site.rel, site.line, site.kind, site.text.trim()))
        .collect();
    lines.push(format!("({} call sites in {} files; {NOTE})", found.sites.len(), index.files.len()));
    Ok(json!({ "ok": true, "output": cut(lines.join("\n")) }))
}

async fn impact_verb(ctx: Arc<Ctx>, args: Value) -> VerbOutcome {
    if let Some(bad) = bad_name(args.get("name")) {
        return Ok(fail(&bad));
    }
    let name = args.get("name").and_then(Value::as_str).unwrap_or_default().to_string();
    let depth = match args.get("depth") {
        None => DEFAULT_DEPTH,
        Some(value) => match value.as_u64().filter(|depth| *depth <= MAX_DEPTH) {
            Some(depth) => depth,
            None => return Ok(fail(&format!("depth must be an integer from 0 to {MAX_DEPTH}"))),
        },
    };
    let index = match load_index(ctx.workspace.clone()).await {
        Ok(index) => index,
        Err(error) => return Ok(fail(&error)),
    };
    let found = impact_of(&index, &name, depth as usize);
    let mut lines = vec![
        format!("impact of {name}: {} defining file(s), {} calling file(s)", found.defs.len(), found.call_files.len()),
        format!("defines: {}", list_or_none(&found.defs)),
        format!("calls: {}", list_or_none(&found.call_files)),
    ];
    for (level, files) in found.importers.iter().enumerate() {
        lines.push(format!("importers at depth {}: {}", level + 1, list_or_none(files)));
    }
    lines.push(format!("({NOTE})"));
    Ok(json!({ "ok": true, "output": cut(lines.join("\n")) }))
}

fn list_or_none(items: &[String]) -> String {
    if items.is_empty() {
        "(none)".to_string()
    } else {
        items.join(", ")
    }
}

type Sig = (u128, u64);

struct FileEntry {
    rel: String,
    sig: Sig,
    approximate: bool,
    defs: HashSet<String>,
    calls: HashMap<String, Vec<(u32, bool)>>,
    call_lines: HashMap<u32, String>,
    specs: Vec<String>,
}

struct Index {
    files: Vec<Arc<FileEntry>>,
    edges: Vec<Vec<usize>>,
}

fn cache() -> &'static Mutex<HashMap<String, Arc<FileEntry>>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<FileEntry>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

async fn load_index(root: PathBuf) -> Result<Index, String> {
    let listed = list_files(&root).await?;
    tokio::task::spawn_blocking(move || build_index(&root, listed))
        .await
        .map_err(|err| format!("callers and impact failed: {err}"))?
}

async fn list_files(root: &Path) -> Result<Vec<String>, String> {
    let listing = tokio::process::Command::new("git")
        .args(["ls-files", "-co", "--exclude-standard", "-z"])
        .current_dir(root)
        .kill_on_drop(true)
        .output();
    let output = match tokio::time::timeout(LIST_TIMEOUT, listing).await {
        Ok(Ok(output)) if output.status.success() && output.stdout.len() <= MAX_LIST_BYTES => output,
        _ => return Err("callers and impact need a git repository: git ls-files failed".to_string()),
    };
    Ok(String::from_utf8_lossy(&output.stdout)
        .split('\0')
        .filter(|path| is_source(path) && !path.starts_with("vendor/"))
        .map(str::to_string)
        .collect())
}

fn is_source(path: &str) -> bool {
    [".js", ".mjs", ".cjs", ".ts"].iter().any(|suffix| path.ends_with(suffix))
}

fn build_index(root: &Path, listed: Vec<String>) -> Result<Index, String> {
    let mut cache = cache().lock().map_err(|err| err.to_string())?;
    let mut files: Vec<Arc<FileEntry>> = Vec::new();
    for rel in listed {
        if rel.starts_with('/') || rel.split('/').any(|segment| segment == "..") {
            continue;
        }
        let path = root.join(&rel);
        let Ok(meta) = fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.is_file() || meta.len() > MAX_FILE_BYTES {
            continue;
        }
        let sig: Sig = (mtime_nanos(&meta), meta.len());
        let entry = match cache.get(&rel) {
            Some(entry) if entry.sig == sig => entry.clone(),
            _ => {
                let Ok(bytes) = fs::read(&path) else {
                    continue;
                };
                let entry = Arc::new(analyse(&rel, &String::from_utf8_lossy(&bytes), sig));
                cache.insert(rel.clone(), entry.clone());
                entry
            }
        };
        files.push(entry);
    }
    let position: HashMap<String, usize> = files.iter().enumerate().map(|(at, entry)| (entry.rel.clone(), at)).collect();
    cache.retain(|rel, _| position.contains_key(rel));
    let edges = files
        .iter()
        .map(|entry| entry.specs.iter().filter_map(|spec| resolve_spec(&entry.rel, spec, &position)).collect())
        .collect();
    Ok(Index { files, edges })
}

fn mtime_nanos(meta: &fs::Metadata) -> u128 {
    meta.modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|since| since.as_nanos())
        .unwrap_or(0)
}

fn resolve_spec(from: &str, spec: &str, position: &HashMap<String, usize>) -> Option<usize> {
    let directory = match from.rfind('/') {
        Some(at) => &from[..at],
        None => "",
    };
    let joined = if directory.is_empty() { spec.to_string() } else { format!("{directory}/{spec}") };
    let base = normalize(&joined);
    [
        base.clone(),
        format!("{base}.js"),
        format!("{base}.mjs"),
        format!("{base}.cjs"),
        format!("{base}.ts"),
        format!("{base}/index.js"),
        format!("{base}/index.mjs"),
        format!("{base}/index.ts"),
    ]
    .iter()
    .find_map(|candidate| position.get(candidate).copied())
}

fn normalize(path: &str) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    parts.join("/")
}

struct Site {
    rel: String,
    line: u32,
    kind: String,
    text: String,
}

struct Scan {
    defs: HashSet<usize>,
    call_files: HashSet<usize>,
    sites: Vec<Site>,
}

fn scan(index: &Index, name: &str) -> Scan {
    let defs: HashSet<usize> = index
        .files
        .iter()
        .enumerate()
        .filter(|(_, entry)| entry.defs.contains(name))
        .map(|(at, _)| at)
        .collect();
    let mut call_files = HashSet::new();
    let mut sites = Vec::new();
    for (at, entry) in index.files.iter().enumerate() {
        let Some(calls) = entry.calls.get(name) else {
            continue;
        };
        call_files.insert(at);
        let linked = index.edges[at].iter().any(|to| defs.contains(to));
        for (line, member) in calls {
            let kind = if *member {
                "member"
            } else if linked {
                "linked"
            } else {
                "direct"
            };
            let kind = if entry.approximate { format!("{kind}, line scan") } else { kind.to_string() };
            sites.push(Site {
                rel: entry.rel.clone(),
                line: *line,
                kind,
                text: entry.call_lines.get(line).cloned().unwrap_or_default(),
            });
        }
    }
    sites.sort_by(|a, b| a.rel.cmp(&b.rel).then(a.line.cmp(&b.line)));
    Scan { defs, call_files, sites }
}

struct Impact {
    defs: Vec<String>,
    call_files: Vec<String>,
    importers: Vec<Vec<String>>,
}

fn impact_of(index: &Index, name: &str, depth: usize) -> Impact {
    let found = scan(index, name);
    let mut reached: HashSet<usize> = found.defs.union(&found.call_files).copied().collect();
    let mut frontier: HashSet<usize> = reached.clone();
    let mut importers = Vec::new();
    for _ in 0..depth {
        if frontier.is_empty() {
            break;
        }
        let mut next: Vec<usize> = (0..index.files.len())
            .filter(|at| !reached.contains(at) && index.edges[*at].iter().any(|to| frontier.contains(to)))
            .collect();
        next.sort_by(|a, b| index.files[*a].rel.cmp(&index.files[*b].rel));
        reached.extend(next.iter().copied());
        frontier = next.iter().copied().collect();
        importers.push(next.iter().map(|at| index.files[*at].rel.clone()).collect());
    }
    Impact {
        defs: sorted_rels(index, &found.defs),
        call_files: sorted_rels(index, &found.call_files),
        importers,
    }
}

fn sorted_rels(index: &Index, set: &HashSet<usize>) -> Vec<String> {
    let mut rels: Vec<String> = set.iter().map(|at| index.files[*at].rel.clone()).collect();
    rels.sort();
    rels
}

fn analyse(rel: &str, text: &str, sig: Sig) -> FileEntry {
    let allocator = Allocator::default();
    let source_types: &[SourceType] = if rel.ends_with(".ts") {
        &[SourceType::ts()]
    } else {
        &[SourceType::mjs(), SourceType::cjs()]
    };
    for source_type in source_types {
        if let Some(program) = parse(&allocator, text, *source_type) {
            let mut collector = Collector {
                starts: line_starts(text).into_iter().map(|at| at as u32).collect(),
                ..Collector::default()
            };
            collector.visit_program(&program);
            return collector.into_entry(rel, sig, false, text);
        }
    }
    scan_lines(rel, text, sig)
}

pub struct Parsed {
    pub defs: Vec<String>,
    pub calls: Vec<(String, u32, bool)>,
    pub specs: Vec<String>,
}

pub fn file_defs_and_calls(rel: &str, text: &str) -> Option<Parsed> {
    let entry = analyse(rel, text, (0, 0));
    if entry.approximate {
        return None;
    }
    let mut defs: Vec<String> = entry.defs.into_iter().collect();
    defs.sort();
    let mut calls: Vec<(String, u32, bool)> = entry
        .calls
        .into_iter()
        .flat_map(|(name, sites)| sites.into_iter().map(move |(line, member)| (name.clone(), line, member)))
        .collect();
    calls.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    Some(Parsed { defs, calls, specs: entry.specs })
}

fn parse<'a>(allocator: &'a Allocator, text: &'a str, source_type: SourceType) -> Option<Program<'a>> {
    let parsed = Parser::new(allocator, text, source_type).parse();
    if parsed.diagnostics.is_empty() {
        Some(parsed.program)
    } else {
        None
    }
}

#[derive(Default)]
struct Collector {
    starts: Vec<u32>,
    defs: HashSet<String>,
    calls: HashMap<String, Vec<(u32, bool)>>,
    specs: Vec<String>,
}

impl Collector {
    fn line_of(&self, offset: u32) -> u32 {
        self.starts.partition_point(|start| *start <= offset) as u32
    }

    fn record(&mut self, name: &str, line: u32, member: bool) {
        self.calls.entry(name.to_string()).or_default().push((line, member));
    }

    fn push_spec(&mut self, spec: &str) {
        if spec.starts_with('.') {
            self.specs.push(spec.to_string());
        }
    }

    fn into_entry(self, rel: &str, sig: Sig, approximate: bool, text: &str) -> FileEntry {
        let wanted: BTreeSet<u32> = self.calls.values().flatten().map(|(line, _)| *line).collect();
        let call_lines = text
            .split('\n')
            .enumerate()
            .filter(|(index, _)| wanted.contains(&(*index as u32 + 1)))
            .map(|(index, line)| (index as u32 + 1, line.to_string()))
            .collect();
        FileEntry {
            rel: rel.to_string(),
            sig,
            approximate,
            defs: self.defs,
            calls: self.calls,
            call_lines,
            specs: self.specs,
        }
    }
}

impl<'a> Visit<'a> for Collector {
    fn visit_function(&mut self, it: &Function<'a>, flags: ScopeFlags) {
        if it.r#type == FunctionType::FunctionDeclaration {
            if let Some(id) = &it.id {
                self.defs.insert(id.name.to_string());
            }
        }
        walk::walk_function(self, it, flags);
    }

    fn visit_class(&mut self, it: &Class<'a>) {
        if it.r#type == ClassType::ClassDeclaration {
            if let Some(id) = &it.id {
                self.defs.insert(id.name.to_string());
            }
        }
        walk::walk_class(self, it);
    }

    fn visit_variable_declarator(&mut self, it: &VariableDeclarator<'a>) {
        if let BindingPattern::BindingIdentifier(id) = &it.id {
            self.defs.insert(id.name.to_string());
        }
        walk::walk_variable_declarator(self, it);
    }

    fn visit_method_definition(&mut self, it: &MethodDefinition<'a>) {
        if let (false, PropertyKey::StaticIdentifier(id)) = (it.computed, &it.key) {
            self.defs.insert(id.name.to_string());
        }
        walk::walk_method_definition(self, it);
    }

    fn visit_object_property(&mut self, it: &ObjectProperty<'a>) {
        if let (false, PropertyKey::StaticIdentifier(id)) = (it.computed, &it.key) {
            if it.method || matches!(it.value, Expression::FunctionExpression(_) | Expression::ArrowFunctionExpression(_)) {
                self.defs.insert(id.name.to_string());
            }
        }
        walk::walk_object_property(self, it);
    }

    fn visit_import_declaration(&mut self, it: &ImportDeclaration<'a>) {
        if let Some(text) = it.source.value.as_str() {
            self.push_spec(text);
        }
        walk::walk_import_declaration(self, it);
    }

    fn visit_export_all_declaration(&mut self, it: &ExportAllDeclaration<'a>) {
        if let Some(text) = it.source.value.as_str() {
            self.push_spec(text);
        }
        walk::walk_export_all_declaration(self, it);
    }

    fn visit_export_from_declaration(&mut self, it: &ExportFromDeclaration<'a>) {
        if let Some(text) = it.source.value.as_str() {
            self.push_spec(text);
        }
        walk::walk_export_from_declaration(self, it);
    }

    fn visit_import_expression(&mut self, it: &ImportExpression<'a>) {
        if let Expression::StringLiteral(source) = &it.source {
            if let Some(text) = source.value.as_str() {
                self.push_spec(text);
            }
        }
        walk::walk_import_expression(self, it);
    }

    fn visit_call_expression(&mut self, it: &CallExpression<'a>) {
        let line = self.line_of(it.span.start);
        match &it.callee {
            Expression::Identifier(callee) => {
                if callee.name.as_str() == "require" {
                    if let Some(Expression::StringLiteral(source)) = it.arguments.first().and_then(|arg| arg.as_expression()) {
                        if let Some(text) = source.value.as_str() {
                            self.push_spec(text);
                        }
                    }
                }
                self.record(callee.name.as_str(), line, false);
            }
            Expression::StaticMemberExpression(member) => self.record(member.property.name.as_str(), line, true),
            _ => {}
        }
        walk::walk_call_expression(self, it);
    }
}

fn scan_lines(rel: &str, text: &str, sig: Sig) -> FileEntry {
    let mut collector = Collector::default();
    let patterns = line_patterns();
    for (index, line) in text.split('\n').enumerate() {
        let number = index as u32 + 1;
        if line.trim().starts_with("//") {
            continue;
        }
        let mut defined: HashSet<&str> = HashSet::new();
        for caps in patterns.def.captures_iter(line) {
            let name = caps.get(1).or(caps.get(2)).map(|m| m.as_str()).unwrap_or_default();
            defined.insert(name);
            collector.defs.insert(name.to_string());
        }
        for caps in patterns.call.captures_iter(line) {
            let name = &caps[2];
            if KEYWORDS.contains(&name) || defined.contains(name) {
                continue;
            }
            collector.record(name, number, caps.get(1).is_some());
        }
        for caps in patterns.spec.captures_iter(line) {
            if let Some(spec) = caps.get(1).or(caps.get(2)).or(caps.get(3)) {
                collector.push_spec(spec.as_str());
            }
        }
    }
    collector.into_entry(rel, sig, true, text)
}

struct LinePatterns {
    def: Regex,
    call: Regex,
    spec: Regex,
}

fn line_patterns() -> &'static LinePatterns {
    static PATTERNS: OnceLock<LinePatterns> = OnceLock::new();
    PATTERNS.get_or_init(|| LinePatterns {
        def: Regex::new(r"\b(?:function|class)\s+([A-Za-z_$][\w$]*)|\b(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=").expect("def"),
        call: Regex::new(r"(\.)?\b([A-Za-z_$][\w$]*)\s*\(").expect("call"),
        spec: Regex::new(r#"\bfrom\s+['"]([^'"\n]+)['"]|\bimport\s*\(\s*['"]([^'"\n]+)['"]\s*\)|\brequire\s*\(\s*['"]([^'"\n]+)['"]\s*\)"#).expect("spec"),
    })
}

#[derive(Default)]
struct CalleeNames {
    names: Vec<String>,
    seen: HashSet<String>,
}

impl<'a> Visit<'a> for CalleeNames {
    fn visit_call_expression(&mut self, it: &CallExpression<'a>) {
        if let Expression::Identifier(callee) = &it.callee {
            let name = callee.name.as_str();
            if self.seen.insert(name.to_string()) {
                self.names.push(name.to_string());
            }
        }
        walk::walk_call_expression(self, it);
    }
}

fn callees_of_function(function: &Function<'_>) -> Vec<String> {
    let mut names = CalleeNames::default();
    names.visit_formal_parameters(&function.params);
    if let Some(body) = &function.body {
        names.visit_function_body(body);
    }
    names.names
}

fn callees_of_arrow(arrow: &ArrowFunctionExpression<'_>) -> Vec<String> {
    let mut names = CalleeNames::default();
    names.visit_arrow_function_expression(arrow);
    names.names
}

struct Found {
    name: String,
    start: u32,
    end: u32,
    callees: Vec<String>,
    function: bool,
}

fn top_level(program: &Program<'_>) -> Vec<Found> {
    let mut out = Vec::new();
    for statement in &program.body {
        match statement {
            Statement::FunctionDeclaration(function) => found_function(&mut out, function),
            Statement::ClassDeclaration(class) => found_class(&mut out, class),
            Statement::VariableDeclaration(declaration) => found_variables(&mut out, declaration),
            Statement::ExportDeclaration(export) => match &export.declaration {
                Declaration::FunctionDeclaration(function) => found_function(&mut out, function),
                Declaration::ClassDeclaration(class) => found_class(&mut out, class),
                Declaration::VariableDeclaration(declaration) => found_variables(&mut out, declaration),
                _ => {}
            },
            _ => {}
        }
    }
    out
}

fn found_function(out: &mut Vec<Found>, function: &Function<'_>) {
    if let Some(id) = &function.id {
        out.push(Found {
            name: id.name.to_string(),
            start: function.span.start,
            end: function.span.end,
            callees: callees_of_function(function),
            function: true,
        });
    }
}

fn found_class(out: &mut Vec<Found>, class: &Class<'_>) {
    if let Some(id) = &class.id {
        out.push(Found {
            name: id.name.to_string(),
            start: class.span.start,
            end: class.span.end,
            callees: Vec::new(),
            function: false,
        });
    }
}

fn found_variables(out: &mut Vec<Found>, declaration: &VariableDeclaration<'_>) {
    for declarator in &declaration.declarations {
        let BindingPattern::BindingIdentifier(id) = &declarator.id else {
            continue;
        };
        let (span, callees) = match &declarator.init {
            Some(Expression::ArrowFunctionExpression(arrow)) => (arrow.span, callees_of_arrow(arrow)),
            Some(Expression::FunctionExpression(function)) => (function.span, callees_of_function(function)),
            _ => continue,
        };
        out.push(Found {
            name: id.name.to_string(),
            start: span.start,
            end: span.end,
            callees,
            function: true,
        });
    }
}

#[derive(Clone)]
struct TopFn {
    name: String,
    file: String,
    start: usize,
    end: usize,
    callees: Vec<String>,
    function: bool,
}

fn line_starts(text: &str) -> Vec<usize> {
    let mut starts = vec![0];
    starts.extend(text.bytes().enumerate().filter(|(_, byte)| *byte == b'\n').map(|(at, _)| at + 1));
    starts
}

fn top_functions(rel: &str, text: &str) -> Arc<Vec<TopFn>> {
    static CACHE: OnceLock<Mutex<HashMap<String, (u64, Arc<Vec<TopFn>>)>>> = OnceLock::new();
    let digest = {
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        hasher.finish()
    };
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let known = cache.lock().ok().and_then(|cache| cache.get(rel).cloned());
    if let Some((known, defs)) = known {
        if known == digest {
            return defs;
        }
    }
    let allocator = Allocator::default();
    let starts = line_starts(text);
    let line = |offset: u32| starts.partition_point(|start| *start <= offset as usize);
    let defs: Vec<TopFn> = [SourceType::mjs(), SourceType::cjs()]
        .into_iter()
        .find_map(|source_type| parse(&allocator, text, source_type))
        .map(|program| {
            top_level(&program)
                .into_iter()
                .map(|found| TopFn {
                    name: found.name,
                    file: rel.to_string(),
                    start: line(found.start),
                    end: line(found.end),
                    callees: found.callees,
                    function: found.function,
                })
                .collect()
        })
        .unwrap_or_default();
    let defs = Arc::new(defs);
    if let Ok(mut cache) = cache.lock() {
        cache.insert(rel.to_string(), (digest, defs.clone()));
    }
    defs
}

pub struct Closure {
    pub file: String,
    pub start: usize,
    pub text: String,
}

pub fn closure_lines(files: &[(String, String)], hits: &[(String, usize)], anchors: usize, cap: usize) -> Vec<Closure> {
    let mut order: Vec<String> = Vec::new();
    let mut defs: HashMap<String, TopFn> = HashMap::new();
    let mut dupes: HashSet<String> = HashSet::new();
    for (rel, text) in files {
        if ![".js", ".mjs", ".cjs"].iter().any(|suffix| rel.ends_with(suffix)) {
            continue;
        }
        for def in top_functions(rel, text).iter().filter(|def| def.function) {
            if defs.contains_key(&def.name) {
                dupes.insert(def.name.clone());
            } else {
                order.push(def.name.clone());
            }
            defs.insert(def.name.clone(), def.clone());
        }
    }
    let top: HashSet<&str> = hits.iter().take(anchors).map(|(file, _)| file.as_str()).collect();
    let mut anchored_order: Vec<String> = Vec::new();
    let mut anchored: HashMap<String, TopFn> = HashMap::new();
    for (file, line) in hits.iter().take(anchors) {
        for name in &order {
            let def = &defs[name];
            if def.file == *file && def.start <= *line && *line <= def.end {
                if !anchored.contains_key(name) {
                    anchored_order.push(name.clone());
                }
                anchored.insert(name.clone(), def.clone());
            }
        }
    }
    let mut picked_order: Vec<String> = Vec::new();
    let mut picked: HashMap<String, TopFn> = HashMap::new();
    for name in &anchored_order {
        let def = &anchored[name];
        for callee in &def.callees {
            let Some(target) = defs.get(callee) else {
                continue;
            };
            if dupes.contains(callee) || *callee == def.name || target.file == def.file {
                continue;
            }
            if top.contains(target.file.as_str()) || picked.contains_key(&target.file) {
                continue;
            }
            picked_order.push(target.file.clone());
            picked.insert(target.file.clone(), target.clone());
        }
    }
    let texts: HashMap<&str, &str> = files.iter().map(|(rel, text)| (rel.as_str(), text.as_str())).collect();
    picked_order
        .iter()
        .take(cap)
        .map(|file| {
            let target = &picked[file];
            let text = texts
                .get(file.as_str())
                .and_then(|source| source.split('\n').nth(target.start - 1))
                .unwrap_or_default()
                .to_string();
            Closure {
                file: target.file.clone(),
                start: target.start,
                text,
            }
        })
        .collect()
}
