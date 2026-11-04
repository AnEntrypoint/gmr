const CALLERS_DESCRIPTION = 'Call sites of an identifier in the tracked and untracked .js, .mjs, .cjs and .ts files, as path:line [kind] text. kind is linked (a direct call in a file that imports a defining file), direct, or member (.name()). Exact for parsed JavaScript and TypeScript; files that do not parse are scanned by lines and marked. limit is 1 to 1000, default 100.';
const IMPACT_DESCRIPTION = 'The files that define an identifier, the files that call it, and the files that import those, out to depth levels (default 2, max 5). Import edges come from parsed JavaScript and resolve inside the repository. Approximate where a file is scanned by lines.';
const CALLERS_SCHEMA = { type: 'object', properties: { name: { type: 'string' }, limit: { type: 'number' } }, required: ['name'] };
const IMPACT_SCHEMA = { type: 'object', properties: { name: { type: 'string' }, depth: { type: 'number' } }, required: ['name'] };
const NOTE = 'Exact for parsed JavaScript and TypeScript: a call is a call expression whose callee is the name, or a member call .name(). Not resolved: shadowing, aliases and dynamic calls. A file that does not parse is scanned by lines and marked approximate.';
const GIT_FAILED = 'callers and impact need a git repository: git ls-files failed';

const SOURCE = /\.(?:js|mjs|cjs|ts)$/;
const IDENTIFIER = /^[A-Za-z_$][A-Za-z0-9_$]*$/;
const LINE_DEF = /\b(?:function|class)\s+([A-Za-z_$][\w$]*)|\b(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=/g;
const LINE_CALL = /(\.)?\b([A-Za-z_$][\w$]*)\s*\(/g;
const LINE_SPEC = /\bfrom\s+['"]([^'"\n]+)['"]|\bimport\s*\(\s*['"]([^'"\n]+)['"]\s*\)|\brequire\s*\(\s*['"]([^'"\n]+)['"]\s*\)/g;
const KEYWORDS = new Set(['await', 'break', 'case', 'catch', 'class', 'const', 'continue', 'debugger', 'default', 'delete', 'do', 'else', 'enum', 'export', 'extends', 'false', 'finally', 'for', 'function', 'if', 'import', 'in', 'instanceof', 'let', 'new', 'null', 'return', 'static', 'super', 'switch', 'this', 'throw', 'true', 'try', 'typeof', 'var', 'void', 'while', 'with', 'yield']);

const MAX_FILE_BYTES = 1000000;
const MAX_LIST_BYTES = 64 * 1024 * 1024;
const LIST_TIMEOUT_MS = 30000;
const OUTPUT_PAGE = 20000;
const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;
const DEFAULT_DEPTH = 2;
const MAX_DEPTH = 5;

const cache = new Map();

function readLock() {
  return { mode: 'read', paths: [] };
}

function fail(error) {
  return { ok: false, error };
}

function cut(text) {
  return text.slice(0, OUTPUT_PAGE);
}

function listOrNone(items) {
  return items.length === 0 ? '(none)' : items.join(', ');
}

function badName(name) {
  if (typeof name !== 'string' || !IDENTIFIER.test(name)) return 'name must be a JavaScript identifier';
  if (KEYWORDS.has(name)) return `${name} is a JavaScript keyword, so it has no callers to list`;
  return null;
}

async function callers(args, host) {
  const name = args?.name;
  const bad = badName(name);
  if (bad !== null) return fail(bad);
  const limit = args.limit === undefined ? DEFAULT_LIMIT : args.limit;
  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_LIMIT) return fail(`limit must be an integer from 1 to ${MAX_LIMIT}`);
  const index = await loadIndex(host);
  if (index.error) return fail(index.error);
  const found = scan(index, name);
  const lines = found.sites.slice(0, limit).map((site) => `${site.rel}:${site.line} [${site.kind}] ${site.text.trim()}`);
  lines.push(`(${found.sites.length} call sites in ${index.files.length} files; ${NOTE})`);
  return { ok: true, output: cut(lines.join('\n')) };
}

async function impact(args, host) {
  const name = args?.name;
  const bad = badName(name);
  if (bad !== null) return fail(bad);
  const depth = args.depth === undefined ? DEFAULT_DEPTH : args.depth;
  if (!Number.isInteger(depth) || depth < 0 || depth > MAX_DEPTH) return fail(`depth must be an integer from 0 to ${MAX_DEPTH}`);
  const index = await loadIndex(host);
  if (index.error) return fail(index.error);
  const found = impactOf(index, name, depth);
  const lines = [
    `impact of ${name}: ${found.defs.length} defining file(s), ${found.callFiles.length} calling file(s)`,
    `defines: ${listOrNone(found.defs)}`,
    `calls: ${listOrNone(found.callFiles)}`,
  ];
  found.importers.forEach((files, level) => lines.push(`importers at depth ${level + 1}: ${listOrNone(files)}`));
  lines.push(`(${NOTE})`);
  return { ok: true, output: cut(lines.join('\n')) };
}

async function loadIndex(host) {
  let listing;
  try {
    listing = await host.proc.run('git', ['ls-files', '-co', '--exclude-standard', '-z'], { cwd: host.workspace, timeoutMs: LIST_TIMEOUT_MS });
  } catch (error) {
    return { error: GIT_FAILED };
  }
  if (listing.code !== 0 || listing.timedOut || listing.stdout.length > MAX_LIST_BYTES) return { error: GIT_FAILED };
  const listed = listing.stdout.split('\0').filter((path) => SOURCE.test(path) && !path.startsWith('vendor/'));
  const entries = [];
  for (const rel of listed) {
    if (rel.startsWith('/') || rel.split('/').includes('..')) continue;
    const entry = await current(host, rel);
    if (entry !== null) entries.push(entry);
  }
  const position = new Map(entries.map((entry, at) => [entry.rel, at]));
  for (const rel of [...cache.keys()]) if (!position.has(rel)) cache.delete(rel);
  const edges = entries.map((entry) => entry.specs.map((spec) => resolveSpec(entry.rel, spec, position)).filter((to) => to !== null));
  return { files: entries, edges };
}

async function current(host, rel) {
  const path = `${host.workspace}/${rel}`;
  let info;
  try {
    info = await host.fs.stat(path);
  } catch (error) {
    return null;
  }
  if (info === null || info.type !== 'file' || info.size > MAX_FILE_BYTES) return null;
  const sig = `${info.mtimeMs}:${info.size}`;
  const known = cache.get(rel);
  if (known !== undefined && known.sig === sig) return known;
  let text;
  try {
    text = await host.fs.read(path);
  } catch (error) {
    return null;
  }
  const entry = analyse(host, rel, text, sig);
  cache.set(rel, entry);
  return entry;
}

function analyse(host, rel, text, sig) {
  let parsed;
  try {
    parsed = host.parseJs(text, rel);
  } catch (error) {
    parsed = null;
  }
  if (parsed === null || parsed === undefined) return scanLines(rel, text, sig);
  const calls = new Map();
  for (const call of parsed.calls) {
    if (!calls.has(call.name)) calls.set(call.name, []);
    calls.get(call.name).push({ line: call.line, member: call.member });
  }
  return entryOf(rel, sig, false, text, new Set(parsed.defs), calls, parsed.specs.filter((spec) => spec.startsWith('.')));
}

function scanLines(rel, text, sig) {
  const defs = new Set();
  const calls = new Map();
  const specs = [];
  text.split('\n').forEach((line, index) => {
    if (line.trim().startsWith('//')) return;
    const defined = new Set();
    for (const match of line.matchAll(LINE_DEF)) {
      const name = match[1] ?? match[2];
      defined.add(name);
      defs.add(name);
    }
    for (const match of line.matchAll(LINE_CALL)) {
      const name = match[2];
      if (KEYWORDS.has(name) || defined.has(name)) continue;
      if (!calls.has(name)) calls.set(name, []);
      calls.get(name).push({ line: index + 1, member: match[1] !== undefined });
    }
    for (const match of line.matchAll(LINE_SPEC)) {
      const spec = match[1] ?? match[2] ?? match[3];
      if (spec.startsWith('.')) specs.push(spec);
    }
  });
  return entryOf(rel, sig, true, text, defs, calls, specs);
}

function entryOf(rel, sig, approximate, text, defs, calls, specs) {
  const wanted = new Set();
  for (const sites of calls.values()) for (const site of sites) wanted.add(site.line);
  const lines = text.split('\n');
  const callLines = new Map();
  for (const line of wanted) callLines.set(line, lines[line - 1]);
  return { rel, sig, approximate, defs, calls, callLines, specs };
}

function normalize(path) {
  const parts = [];
  for (const part of path.split('/')) {
    if (part === '' || part === '.') continue;
    if (part === '..') parts.pop();
    else parts.push(part);
  }
  return parts.join('/');
}

function resolveSpec(from, spec, position) {
  const at = from.lastIndexOf('/');
  const directory = at < 0 ? '' : from.slice(0, at);
  const base = normalize(directory === '' ? spec : `${directory}/${spec}`);
  const candidates = [base, `${base}.js`, `${base}.mjs`, `${base}.cjs`, `${base}.ts`, `${base}/index.js`, `${base}/index.mjs`, `${base}/index.ts`];
  for (const candidate of candidates) if (position.has(candidate)) return position.get(candidate);
  return null;
}

function scan(index, name) {
  const defs = new Set();
  index.files.forEach((entry, at) => {
    if (entry.defs.has(name)) defs.add(at);
  });
  const callFiles = new Set();
  const sites = [];
  index.files.forEach((entry, at) => {
    const calls = entry.calls.get(name);
    if (calls === undefined) return;
    callFiles.add(at);
    const linked = index.edges[at].some((to) => defs.has(to));
    for (const call of calls) {
      const kind = call.member ? 'member' : linked ? 'linked' : 'direct';
      sites.push({
        rel: entry.rel,
        line: call.line,
        kind: entry.approximate ? `${kind}, line scan` : kind,
        text: entry.callLines.get(call.line) ?? '',
      });
    }
  });
  sites.sort((a, b) => (a.rel < b.rel ? -1 : a.rel > b.rel ? 1 : a.line - b.line));
  return { defs, callFiles, sites };
}

function relsOf(index, set) {
  return [...set].map((at) => index.files[at].rel).sort();
}

function impactOf(index, name, depth) {
  const found = scan(index, name);
  const reached = new Set([...found.defs, ...found.callFiles]);
  let frontier = new Set(reached);
  const importers = [];
  for (let level = 0; level < depth && frontier.size > 0; level += 1) {
    const next = [];
    index.files.forEach((entry, at) => {
      if (!reached.has(at) && index.edges[at].some((to) => frontier.has(to))) next.push(at);
    });
    next.sort((a, b) => {
      const x = index.files[a].rel;
      const y = index.files[b].rel;
      return x < y ? -1 : x > y ? 1 : 0;
    });
    for (const at of next) reached.add(at);
    frontier = new Set(next);
    importers.push(next.map((at) => index.files[at].rel));
  }
  return { defs: relsOf(index, found.defs), callFiles: relsOf(index, found.callFiles), importers };
}

export const verbs = [
  {
    name: 'callers',
    description: CALLERS_DESCRIPTION,
    schema: CALLERS_SCHEMA,
    lock: readLock,
    call: callers,
  },
  {
    name: 'impact',
    description: IMPACT_DESCRIPTION,
    schema: IMPACT_SCHEMA,
    lock: readLock,
    call: impact,
  },
];
