const OUTPUT_PAGE = 20000;
const EMPTY_PATH = 'path must be a non-empty string';
const SERVED_SKILLS = ['gm', 'gm-continue'];
const SKILL_URI = /^skill:\/\/([^/]+)\/(.+)$/;

const DESC_READ_FILE = 'Read a text file, inside or outside the workspace, one page of at most 20000 characters. Lines are the default unit: offset is a 0-based line index, limit a line count (default: as many whole lines as fit), and total (the file in lines) and next (the line index of the next page, null at the end) count lines. truncated is true when the page stops before the end, totalBytes is the file size in bytes, and hint names the read that gets the rest. A line longer than the page is cut to its first 20000 characters, with cutLine true, and next is the line after it. unit "chars" counts offset, limit (1 to 20000, default 20000), total and next in characters instead. A path that starts with skill:// reads a served skill file, the text resources/read returns, and a skill:// uri that no served skill has is an error that names it.';
const SCHEMA_READ_FILE = { type: 'object', properties: { path: { type: 'string' }, offset: { type: 'number' }, limit: { type: 'number' }, unit: { type: 'string' } }, required: ['path'] };
const DESC_WRITE_FILE = 'Write a text file, inside or outside the workspace, creating parent directories.';
const SCHEMA_WRITE_FILE = { type: 'object', properties: { path: { type: 'string' }, content: { type: 'string' } }, required: ['path', 'content'] };
const DESC_EDIT_FILE = 'Replace old with new in a text file, inside or outside the workspace. old must occur once, unless replaceAll is true.';
const SCHEMA_EDIT_FILE = { type: 'object', properties: { path: { type: 'string' }, old: { type: 'string' }, new: { type: 'string' }, replaceAll: { type: 'boolean' } }, required: ['path', 'old', 'new'] };
const DESC_STAT_FILE = 'Type, size and mtime of a path, inside or outside the workspace, as JSON.';
const SCHEMA_STAT_FILE = { type: 'object', properties: { path: { type: 'string' } }, required: ['path'] };
const DESC_LIST_DIR = 'List a directory, inside or outside the workspace. Directories end with "/".';
const SCHEMA_LIST_DIR = { type: 'object', properties: { path: { type: 'string' } }, required: [] };

const failure = (error) => ({ ok: false, error });
const refusal = (reason) => ({ ok: false, refused: true, error: `refused: ${reason}` });
const isWhole = (value) => typeof value === 'number' && Number.isInteger(value) && value >= 0;
const isHigh = (code) => code >= 0xd800 && code <= 0xdbff;
const isLow = (code) => code >= 0xdc00 && code <= 0xdfff;

function cpLength(text) {
  let count = 0;
  for (let i = 0; i < text.length; i += 1) {
    if (isHigh(text.charCodeAt(i)) && isLow(text.charCodeAt(i + 1))) i += 1;
    count += 1;
  }
  return count;
}

function unitAt(text, cp) {
  let at = 0;
  for (let n = 0; n < cp && at < text.length; n += 1) {
    at += isHigh(text.charCodeAt(at)) && isLow(text.charCodeAt(at + 1)) ? 2 : 1;
  }
  return at;
}

function cutNotice(size, hint) {
  return hint === null ? { truncated: false, totalBytes: size } : { truncated: true, totalBytes: size, hint };
}

function lineStarts(text) {
  const starts = [0];
  for (let at = text.indexOf('\n'); at !== -1; at = text.indexOf('\n', at + 1)) starts.push(at + 1);
  if (starts[starts.length - 1] === text.length) starts.pop();
  return starts;
}

function charPage(text, size, offset, limit) {
  const total = cpLength(text);
  const end = Math.min(total, offset + limit);
  const output = offset < end ? text.slice(unitAt(text, offset), unitAt(text, end)) : '';
  const next = end < total ? end : null;
  const hint = next === null ? null : `pass unit: "chars", offset: ${next} and limit: ${OUTPUT_PAGE} to read the rest`;
  return { ok: true, output, total, offset, next, ...cutNotice(size, hint) };
}

function linePage(text, size, offset, limit) {
  const starts = lineStarts(text);
  const total = starts.length;
  if (offset >= total) return { ok: true, unit: 'lines', output: '', total, offset, next: null, ...cutNotice(size, null) };
  const endOf = (line) => (line + 1 < total ? starts[line + 1] : text.length);
  const begin = starts[offset];
  const first = text.slice(begin, endOf(offset));
  const firstChars = cpLength(first);
  if (firstChars > OUTPUT_PAGE) {
    const next = offset + 1 < total ? offset + 1 : null;
    const hint = `line ${offset} is longer than a page: pass unit: "chars", offset: ${cpLength(text.slice(0, begin)) + OUTPUT_PAGE} and limit: ${OUTPUT_PAGE} to read the rest`;
    return { ok: true, unit: 'lines', output: first.slice(0, unitAt(first, OUTPUT_PAGE)), cutLine: true, total, offset, next, ...cutNotice(size, hint) };
  }
  let used = firstChars;
  let count = 1;
  while (count < limit && offset + count < total) {
    const chars = cpLength(text.slice(starts[offset + count], endOf(offset + count)));
    if (chars > OUTPUT_PAGE - used) break;
    used += chars;
    count += 1;
  }
  const endLine = offset + count;
  const next = endLine < total ? endLine : null;
  const hint = next === null ? null : `pass offset: ${next} and limit: ${total - next} to read the rest`;
  return { ok: true, unit: 'lines', output: text.slice(begin, endOf(endLine - 1)), total, offset, next, ...cutNotice(size, hint) };
}

function isWindows(workspace) {
  return /^[A-Za-z]:/.test(workspace) || workspace.includes('\\');
}

function parsePath(path, windows) {
  const root = windows ? /^([A-Za-z]:)?([\\/]*)/ : /^()(\/*)/;
  const splitter = windows ? /[\\/]+/ : /\/+/;
  const head = root.exec(path);
  const parts = [];
  for (const seg of path.slice(head[0].length).split(splitter)) {
    if (seg === '' || seg === '.') continue;
    if (seg === '..') parts.pop();
    else parts.push(seg);
  }
  return { drive: head[1], absolute: head[2].length > 0, parts };
}

function renderPath(parsed, windows) {
  const sep = windows ? '\\' : '/';
  return parsed.drive + (parsed.absolute ? sep : '') + parsed.parts.join(sep);
}

function resolvePath(host, rel) {
  const windows = isWindows(host.workspace);
  const joined = parsePath(rel, windows).absolute ? rel : `${host.workspace}${windows ? '\\' : '/'}${rel}`;
  return renderPath(parsePath(joined, windows), windows);
}

function locate(host, raw) {
  if (typeof raw !== 'string' || raw === '') return null;
  return resolvePath(host, raw);
}

function relativeFrom(host, path) {
  const windows = isWindows(host.workspace);
  const base = parsePath(host.workspace, windows).parts;
  const target = parsePath(path, windows).parts;
  let common = 0;
  while (common < base.length && common < target.length && base[common] === target[common]) common += 1;
  const up = base.slice(common).map(() => '..');
  return [...up, ...target.slice(common)].join(windows ? '\\' : '/');
}

function parentOf(path, windows) {
  const parsed = parsePath(path, windows);
  return parsed.parts.length === 0 ? null : renderPath({ ...parsed, parts: parsed.parts.slice(0, -1) }, windows);
}

function unservedSkill(uri) {
  return failure(`no served skill file has the uri ${uri}`);
}

async function servedSkill(host, uri) {
  const match = SKILL_URI.exec(uri);
  const served = match !== null && SERVED_SKILLS.includes(match[1]) && match[2].split('/').every((seg) => seg !== '' && seg !== '.' && seg !== '..');
  if (!served) return unservedSkill(uri);
  try {
    return { ok: true, output: await host.fs.read(`${host.checkout}/skills/${match[1]}/${match[2]}`) };
  } catch {
    return unservedSkill(uri);
  }
}

async function statOrThrow(host, path) {
  const info = await host.fs.stat(path);
  if (info === null) throw new Error(`ENOENT: no such file or directory, stat '${path}'`);
  return info;
}

async function readFileVerb(args, host) {
  if (typeof args.path === 'string' && args.path.startsWith('skill://')) return servedSkill(host, args.path);
  const path = locate(host, args.path);
  if (path === null) return refusal(EMPTY_PATH);
  const unit = args.unit ?? 'lines';
  if (unit !== 'lines' && unit !== 'chars') return failure('unit must be lines or chars');
  const offset = args.offset ?? 0;
  if (!isWhole(offset)) {
    return failure(unit === 'chars' ? 'offset must be a whole number of characters, zero or more' : 'offset must be a whole number of lines, zero or more');
  }
  if (unit === 'chars') {
    const limit = args.limit ?? OUTPUT_PAGE;
    if (!isWhole(limit) || limit < 1 || limit > OUTPUT_PAGE) return failure(`limit must be a whole number from 1 to ${OUTPUT_PAGE}`);
    const info = await statOrThrow(host, path);
    return charPage(await host.fs.read(path), info.size, offset, limit);
  }
  const limit = args.limit ?? Infinity;
  if (limit !== Infinity && !(isWhole(limit) && limit >= 1)) return failure('limit must be a whole number of lines, one or more');
  const info = await statOrThrow(host, path);
  return linePage(await host.fs.read(path), info.size, offset, limit);
}

async function writeFileVerb(args, host) {
  const path = locate(host, args.path);
  if (path === null) return refusal(EMPTY_PATH);
  if (typeof args.content !== 'string') return failure('content must be a string');
  const parent = parentOf(path, isWindows(host.workspace));
  if (parent !== null) await host.fs.mkdirp(parent);
  await host.fs.write(path, args.content);
  return { ok: true, output: `wrote ${relativeFrom(host, path)} (${args.content.length} chars)` };
}

async function editFileVerb(args, host) {
  const path = locate(host, args.path);
  if (path === null) return refusal(EMPTY_PATH);
  const old = args.old;
  if (typeof old !== 'string' || old === '') return failure('old must be a non-empty string');
  if (typeof args.new !== 'string') return failure('new must be a string');
  const text = await host.fs.read(path);
  const pieces = text.split(old);
  const count = pieces.length - 1;
  if (count === 0) return failure(`old text not found in ${args.path}`);
  if (count > 1 && args.replaceAll !== true) {
    return failure(`old text occurs ${count} times in ${args.path}; add context to make it unique, or set replaceAll`);
  }
  await host.fs.write(path, pieces.join(args.new));
  return { ok: true, output: `edited ${relativeFrom(host, path)} (${count} replacement${count === 1 ? '' : 's'})` };
}

async function statFileVerb(args, host) {
  const path = locate(host, args.path);
  if (path === null) return refusal(EMPTY_PATH);
  let info = await statOrThrow(host, path);
  if (info.type === 'symlink') info = await statOrThrow(host, await host.fs.realpath(path));
  const type = info.type === 'dir' ? 'dir' : info.type === 'file' ? 'file' : 'other';
  const mtime = new Date(Math.max(0, Math.floor(info.mtimeMs))).toISOString();
  return { ok: true, output: JSON.stringify({ type, size: info.size, mtime }) };
}

async function listDirVerb(args, host) {
  const path = locate(host, args.path ?? '.');
  if (path === null) return refusal(EMPTY_PATH);
  const names = await host.fs.list(path);
  return { ok: true, output: names.slice().sort().join('\n') };
}

const READ_LOCK = () => ({ mode: 'read' });

function writeLock(args, host) {
  const path = locate(host, args.path);
  return path === null ? null : { mode: 'write', paths: [path] };
}

export const verbs = [
  { name: 'read_file', description: DESC_READ_FILE, schema: SCHEMA_READ_FILE, lock: READ_LOCK, call: readFileVerb },
  { name: 'write_file', description: DESC_WRITE_FILE, schema: SCHEMA_WRITE_FILE, lock: writeLock, call: writeFileVerb },
  { name: 'edit_file', description: DESC_EDIT_FILE, schema: SCHEMA_EDIT_FILE, lock: writeLock, call: editFileVerb },
  { name: 'stat_file', description: DESC_STAT_FILE, schema: SCHEMA_STAT_FILE, lock: READ_LOCK, call: statFileVerb },
  { name: 'list_dir', description: DESC_LIST_DIR, schema: SCHEMA_LIST_DIR, lock: READ_LOCK, call: listDirVerb },
];
