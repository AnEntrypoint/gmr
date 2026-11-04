import { inState, isVendor, joinPath, normalizePath, plural, relUnder, resolveIn, STATE_DIRS, underDir, sepOf } from '../lib/search-paths.mjs';
import { countTerms, idfOf, offerHit, snippet, termsOf, weightOf } from '../lib/search-rank.mjs';
import { makeMatcher, REGEX_CHARS } from '../lib/search-match.mjs';
import { addFileDefs, closureLines, createClosureIndex, isJsFile } from '../lib/search-closure.mjs';
import { validPage, wikiWrite } from '../lib/search-wiki.mjs';

const LIST_LIMIT = 64 * 1024 * 1024;
const LIST_TIMEOUT_MS = 30000;
const SETTLE_MS = 3000;
const MAX_FILE_BYTES = 1000000;
const PROBE_CHARS = 8000;
const OUTPUT_PAGE = 20000;
const CHUNK = 32;
const DEFAULT_K = 10;
const MAX_K = 50;
const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;
const MAX_PATTERN = 200;
const ROOT_ARGS = ['root', 'projectPath', 'cwd'];
const TAKES = {
  search: ['query', 'k', 'closure'],
  wiki_search: ['query', 'k'],
  grep: ['pattern', 'path', 'mode', 'scope', 'ignoreCase', 'limit', 'offset', 'vendor'],
};
const TRACKED = { name: 'tracked', args: ['ls-files', '-z'], cached: true };
const WIKI_LIST = { name: 'wiki', args: ['ls-files', '-z', '-co', '--exclude-standard', '--', 'wiki'], cached: false };
const ALL_LIST = { name: 'all', args: ['ls-files', '-z', '-c', '-o'], cached: false };
const UNTRACKED_LIST = { name: 'untracked', args: ['ls-files', '-z', '-o', '--exclude-standard'], cached: false };
const LIST_ERROR = 'search and grep need a git repository: git ls-files failed';
const listings = new Map();

function fail(error) {
  return { ok: false, error };
}

function refused(reason) {
  return { ok: false, refused: true, error: `refused: ${reason}` };
}

function intArg(args, key, fallback) {
  const value = args[key];
  if (value === undefined) return fallback;
  return typeof value === 'number' && Number.isInteger(value) ? value : null;
}

async function tryCall(fn) {
  try {
    return await fn();
  } catch {
    return null;
  }
}

async function indexStamp(host, root) {
  const stat = await host.fs.stat(joinPath(joinPath(root, '.git'), 'index'));
  if (stat === null) return null;
  return { key: `${stat.size}:${stat.mtimeMs}`, settled: stat.mtimeMs < Date.now() - SETTLE_MS };
}

async function trackedPaths(host, root, listing) {
  const stamp = await indexStamp(host, root);
  const known = listings.get(listing.name);
  if (listing.cached && stamp !== null && stamp.settled && known !== undefined && known.key === stamp.key && known.root === root) {
    return { paths: known.paths };
  }
  const run = await host.proc.run('git', listing.args, { cwd: root, timeoutMs: LIST_TIMEOUT_MS });
  if (run.code !== 0 || run.timedOut || run.stdout.length > LIST_LIMIT) return { error: LIST_ERROR };
  const paths = run.stdout.split('\0').filter((path) => path !== '');
  if (listing.cached && stamp !== null && stamp.settled) listings.set(listing.name, { key: stamp.key, root, paths });
  return { paths };
}

async function homeOf(host, workspace, args) {
  const given = ROOT_ARGS.filter((key) => args[key] !== undefined);
  if (given.length === 0) return { dir: workspace };
  for (const key of given) {
    if (typeof args[key] !== 'string' || args[key] === '') return { error: `${key} must be a non-empty path` };
  }
  const dirs = new Set(given.map((key) => resolveIn(workspace, args[key])));
  if (dirs.size > 1) return { error: `${given.join(' and ')} name different roots; pass one` };
  const dir = dirs.values().next().value;
  const raw = args[given[0]];
  let stat = await host.fs.stat(dir);
  if (stat !== null && stat.type === 'symlink') {
    const real = await tryCall(() => host.fs.realpath(dir));
    stat = real === null ? null : await host.fs.stat(real);
  }
  if (stat === null) return { error: `no such root: ${raw}` };
  if (stat.type !== 'dir') return { error: `root is not a directory: ${raw}` };
  return { dir };
}

function contextFor(workspace, dir) {
  return { root: dir, workspace, confined: dir === workspace, realRoot: undefined, dirs: new Map() };
}

async function realRootOf(host, ctx) {
  if (ctx.realRoot === undefined) ctx.realRoot = await tryCall(() => host.fs.realpath(ctx.root));
  return ctx.realRoot;
}

async function dirContained(host, ctx, dirAbs) {
  if (!ctx.dirs.has(dirAbs)) {
    const real = await tryCall(() => host.fs.realpath(dirAbs));
    const realRoot = await realRootOf(host, ctx);
    let ok = false;
    if (real !== null && realRoot !== null) {
      const sep = sepOf(realRoot);
      ok = real === realRoot || real.startsWith(realRoot.endsWith(sep) ? realRoot : realRoot + sep);
    }
    ctx.dirs.set(dirAbs, ok);
  }
  return ctx.dirs.get(dirAbs);
}

async function confinedTarget(host, ctx, rel) {
  if (rel === '' || rel.split(/[\\/]/).includes('..')) return null;
  const at = rel.lastIndexOf('/');
  const parent = at < 0 ? ctx.root : joinPath(ctx.root, rel.slice(0, at));
  return (await dirContained(host, ctx, parent)) ? joinPath(ctx.root, rel) : null;
}

async function fileTarget(host, ctx, rel) {
  return ctx.confined ? confinedTarget(host, ctx, rel) : joinPath(ctx.root, rel);
}

function shownAt(ctx, rel) {
  return ctx.confined ? rel : joinPath(ctx.root, rel);
}

async function readText(host, path) {
  const stat = await host.fs.stat(path);
  if (stat === null || stat.type !== 'file' || stat.size > MAX_FILE_BYTES) return null;
  const text = await tryCall(() => host.fs.read(path));
  if (text === null || text.slice(0, PROBE_CHARS).includes('\u0000')) return null;
  return text;
}

function splitLines(text) {
  const lines = text.split('\n');
  for (let k = 0; k < lines.length; k += 1) {
    if (lines[k].endsWith('\r')) lines[k] = lines[k].slice(0, -1);
  }
  return lines;
}

async function forEachFile(host, ctx, rels, visit) {
  for (let from = 0; from < rels.length; from += CHUNK) {
    const part = rels.slice(from, from + CHUNK);
    const texts = await Promise.all(part.map(async (rel) => {
      const target = await fileTarget(host, ctx, rel);
      return target === null ? null : readText(host, target);
    }));
    for (let k = 0; k < part.length; k += 1) {
      if (texts[k] !== null) visit(part[k], splitLines(texts[k]));
    }
  }
}

async function rankedOp(host, ctx, listing, args) {
  const query = args.query;
  if (typeof query !== 'string' || query.trim() === '') return fail('query must be a non-empty string');
  const k = intArg(args, 'k', DEFAULT_K);
  if (k === null || k < 1 || k > MAX_K) return fail(`k must be an integer from 1 to ${MAX_K}`);
  const closure = args.closure === undefined ? false : args.closure;
  if (typeof closure !== 'boolean') return fail('closure must be true or false');
  const terms = termsOf(query);
  if (terms.length === 0) return fail('query has no word characters');
  const listed = await trackedPaths(host, ctx.root, listing);
  if (listed.error) return fail(listed.error);
  const rels = listed.paths;

  const termIndex = new Map(terms.map((term, at) => [term, at]));
  const counts = new Array(terms.length).fill(0);
  const df = new Array(terms.length).fill(0);
  let docs = 0;
  let total = 0;
  await forEachFile(host, ctx, rels, (rel, lines) => {
    for (const text of lines) {
      const len = countTerms(text, termIndex, counts);
      if (len > 0) {
        docs += 1;
        total += len;
        counts.forEach((count, at) => {
          if (count > 0) df[at] += 1;
        });
      }
    }
  });
  const avg = total / Math.max(docs, 1);
  const idf = df.map((d) => idfOf(docs, d));

  const top = [];
  let id = 0;
  const index = closure ? createClosureIndex() : null;
  await forEachFile(host, ctx, rels, (rel, lines) => {
    if (index !== null && isJsFile(rel)) addFileDefs(index, rel, lines);
    lines.forEach((text, at) => {
      const len = countTerms(text, termIndex, counts);
      if (len === 0) return;
      const thisId = id;
      id += 1;
      let score = 0;
      let matched = false;
      counts.forEach((tf, t) => {
        if (tf === 0) return;
        score += idf[t] * weightOf(tf, len, avg);
        matched = true;
      });
      if (matched) offerHit(top, k, score, thisId, rel, at + 1, text);
    });
  });

  const out = top.map((hit) => `${shownAt(ctx, hit.rel)}:${hit.line} ${hit.score.toFixed(3)} ${hit.text}`);
  if (closure) out.push(...closureLines(index, top, (rel) => shownAt(ctx, rel)));
  return { ok: true, output: out.length > 0 ? out.join('\n') : '(no match)' };
}

function skippedDirectory(name, vendor) {
  if (name.startsWith('.') || name === 'runs') return true;
  return !vendor && (name === 'node_modules' || name === 'vendor');
}

async function walkFiles(host, base, all, vendor) {
  const top = await host.fs.stat(base);
  if (top !== null && top.type === 'file') return { paths: [''], skipped: 0 };
  const paths = [];
  let skipped = 0;
  const pending = [''];
  while (pending.length > 0) {
    const dir = pending.pop();
    const at = dir === '' ? base : joinPath(base, dir);
    const entries = await tryCall(() => host.fs.list(at));
    if (entries === null) continue;
    const files = [];
    for (const entry of entries) {
      const isDir = entry.endsWith('/');
      const bare = isDir ? entry.slice(0, -1) : entry;
      const rel = dir === '' ? bare : `${dir}/${bare}`;
      if (!isDir) {
        files.push(rel);
      } else if (STATE_DIRS.has(bare)) {
        continue;
      } else if (!all && skippedDirectory(bare, vendor)) {
        skipped += 1;
      } else {
        pending.push(rel);
      }
    }
    for (let from = 0; from < files.length; from += CHUNK) {
      const part = files.slice(from, from + CHUNK);
      const stats = await Promise.all(part.map((rel) => host.fs.stat(joinPath(base, rel))));
      part.forEach((rel, k) => {
        if (stats[k] !== null && stats[k].type === 'file') paths.push(rel);
      });
    }
  }
  return { paths: paths.sort(), skipped };
}

function walkSkipHint(dirs) {
  if (dirs === 0) return null;
  return `${plural(dirs, 'directory', 'directories')} skipped (dot-directories, node_modules, vendor or runs); use scope: 'all' to include them`;
}

async function skippedHint(host, root, listedPaths, under, vendor) {
  const every = await trackedPaths(host, root, ALL_LIST);
  const untracked = await trackedPaths(host, root, UNTRACKED_LIST);
  if (every.error || untracked.error) return 'skipped files not counted: git ls-files failed or ran past its limit';
  const tracked = new Set(listedPaths);
  const loose = new Set(untracked.paths);
  const skipped = underDir(every.paths, under, true).filter((rel) => {
    if (rel.endsWith('/')) return false;
    return tracked.has(rel) ? !vendor && isVendor(rel) : !inState(rel);
  });
  if (skipped.length === 0) return null;
  const untrackedCount = skipped.filter((rel) => !tracked.has(rel) && loose.has(rel) && !isVendor(rel)).length;
  return `${plural(skipped.length, 'file')} skipped (${untrackedCount} untracked, ${skipped.length - untrackedCount} gitignored or in node_modules); use scope: 'all' to include them`;
}

async function loadHit(host, ctx, base, name, inside) {
  const at = name === '' ? base : joinPath(base, name);
  let target = at;
  let shown = at;
  if (inside && ctx.confined) {
    const rel = relUnder(ctx.root, at);
    shown = rel;
    target = rel === null ? null : await confinedTarget(host, ctx, rel);
  }
  if (target === null) return null;
  const text = await readText(host, target);
  return text === null ? null : { shown, text };
}

async function scanHits(host, ctx, base, names, inside, matcher, limit, offset) {
  const page = { found: [], total: 0, next: null };
  let size = 0;
  for (let from = 0; from < names.length; from += CHUNK) {
    const loaded = await Promise.all(names.slice(from, from + CHUNK).map((name) => loadHit(host, ctx, base, name, inside)));
    for (const entry of loaded) {
      if (entry === null) continue;
      splitLines(entry.text).forEach((line, at) => {
        if (!matcher.test(line)) return;
        page.total += 1;
        if (page.total <= offset || page.next !== null) return;
        const row = `${entry.shown}:${at + 1} ${snippet(line)}`;
        if (page.found.length >= limit || size + row.length > OUTPUT_PAGE) {
          page.next = page.total - 1;
          return;
        }
        size += row.length + 1;
        page.found.push(row);
      });
    }
  }
  return page;
}

async function grepOp(host, ctx, args) {
  const pattern = args.pattern;
  if (typeof pattern !== 'string' || pattern === '') return fail('pattern must be a non-empty string');
  if (pattern.length > MAX_PATTERN) return fail(`pattern must be at most ${MAX_PATTERN} characters`);
  const requested = args.mode === undefined ? 'literal' : args.mode;
  let mode = null;
  if (requested === 'content' || requested === 'literal') mode = 'literal';
  else if (requested === 'regex' || requested === 'word') mode = requested;
  if (mode === null) return fail('mode must be one of literal, regex, word');
  const scope = args.scope === undefined ? 'tracked' : args.scope;
  if (scope !== 'tracked' && scope !== 'all') return fail('scope must be one of tracked, all');
  const ignoreCase = args.ignoreCase === true;
  const limit = intArg(args, 'limit', DEFAULT_LIMIT);
  if (limit === null || limit < 1 || limit > MAX_LIMIT) return fail(`limit must be an integer from 1 to ${MAX_LIMIT}`);
  const vendor = args.vendor === undefined ? false : args.vendor;
  if (typeof vendor !== 'boolean') return fail('vendor must be true or false');
  const path = args.path;
  if (path !== undefined && (typeof path !== 'string' || path === '')) return fail('path must be a non-empty string');
  const offset = intArg(args, 'offset', 0);
  let matcher;
  try {
    matcher = makeMatcher(pattern, mode, ignoreCase);
  } catch (error) {
    return fail(`bad pattern: ${error.message}`);
  }
  const all = scope === 'all';
  const target = resolveIn(ctx.root, path === undefined ? '.' : path);
  if ((await host.fs.stat(target)) === null) return fail(`no such path: ${path === undefined ? '' : path}`);
  const inside = relUnder(ctx.root, target);
  const under = inside === null ? '' : inside;
  const listed = inside === null ? { error: LIST_ERROR } : await trackedPaths(host, ctx.root, TRACKED);

  let source;
  let base;
  let names;
  let walkSkipped = null;
  if (!listed.error) {
    if (all) {
      const every = await trackedPaths(host, ctx.root, ALL_LIST);
      if (every.error) return fail('scope all: git ls-files failed or ran past its 30 s limit');
      const tracked = new Set(listed.paths);
      names = underDir(every.paths, under, true).filter((rel) => !rel.endsWith('/') && (tracked.has(rel) || !inState(rel)));
    } else {
      names = underDir(listed.paths, under, vendor);
    }
    source = 'git';
    base = ctx.root;
  } else {
    const walked = await walkFiles(host, target, all, vendor);
    names = walked.paths;
    source = 'walk';
    base = target;
    walkSkipped = walked.skipped;
  }
  if (offset === null || offset < 0) return fail('offset must be a whole number, zero or more');

  const isInside = inside !== null;
  let page = await scanHits(host, ctx, base, names, isInside, matcher, limit, offset);
  const hints = [];
  if (page.total === 0 && mode === 'literal' && REGEX_CHARS.test(pattern)) {
    let asRegex;
    try {
      asRegex = makeMatcher(pattern, 'regex', ignoreCase);
    } catch (error) {
      return fail(`no literal match, and the pattern is not a valid regex: ${error.message}`);
    }
    page = await scanHits(host, ctx, base, names, isInside, asRegex, limit, offset);
    hints.push(page.total > 0 ? 'no literal match; matched as regex' : 'no match as literal or regex');
  }
  let skipped = null;
  if (!all && page.total === 0) {
    skipped = walkSkipped !== null
      ? walkSkipHint(walkSkipped)
      : await skippedHint(host, ctx.root, listed.paths, under, vendor);
  }
  if (skipped !== null) hints.push(skipped);
  const footer = `(${page.total} matches; showing ${page.found.length > 0 ? offset : 0} to ${offset + page.found.length}; ${page.next === null ? 'no more' : `next offset ${page.next}`})`;
  const lines = [...page.found, footer, ...hints.map((hint) => `hint: ${hint}`)];
  const result = { ok: true, files: source, output: lines.join('\n') };
  if (hints.length > 0) result.hint = hints.join(' | ');
  return result;
}

async function guarded(host, name, args, op) {
  const takes = TAKES[name];
  const unknown = Object.keys(args).filter((key) => !takes.includes(key) && !ROOT_ARGS.includes(key));
  if (unknown.length > 0) {
    return refused(`unknown argument ${unknown.join(', ')}; ${name} takes ${[...takes, 'root'].join(', ')}`);
  }
  const workspace = normalizePath(host.workspace);
  const home = await homeOf(host, workspace, args);
  if (home.error !== undefined) return refused(home.error);
  return op(contextFor(workspace, home.dir));
}

function wikiPaths(args) {
  const page = args?.page;
  return validPage(page) ? [`wiki/${page}.md`, 'wiki/index.md', 'wiki/log.md'] : [];
}

export const verbs = [
  {
    name: 'search',
    description: 'Rank the lines of git-tracked files by BM25 and return the best k as path:line score text. An identifier also matches its parts, so planWaves matches plan and waves. k is 1 to 50, default 10. closure true (default false) adds one line per JavaScript file that defines a top-level function called from the function enclosing a top-10 hit and is not itself a top-10 file, as path:line closure text in place of a score, where line is the first line of that function, at most 20 lines. root (or its aliases projectPath and cwd) names another existing directory to search instead of the workspace; with a root other than the workspace, hits show absolute paths. An unknown argument is refused by name.',
    schema: JSON.parse('{"type":"object","properties":{"query":{"type":"string"},"k":{"type":"number"},"root":{"type":"string"},"closure":{"type":"boolean"}},"required":["query"]}'),
    lock: () => ({ mode: 'read', paths: [] }),
    async call(args, host) {
      const input = args ?? {};
      return guarded(host, 'search', input, (ctx) => rankedOp(host, ctx, TRACKED, input));
    },
  },
  {
    name: 'wiki_search',
    description: 'Rank the lines of the wiki pages (wiki/, tracked or not) by BM25 and return the best k as path:line score text, the same way as search. k is 1 to 50, default 10. root (or its aliases projectPath and cwd) names another existing directory to search instead of the workspace; with a root other than the workspace, hits show absolute paths. An unknown argument is refused by name.',
    schema: JSON.parse('{"type":"object","properties":{"query":{"type":"string"},"k":{"type":"number"},"root":{"type":"string"}},"required":["query"]}'),
    lock: () => ({ mode: 'read', paths: [] }),
    async call(args, host) {
      const input = args ?? {};
      return guarded(host, 'wiki_search', input, (ctx) => rankedOp(host, ctx, WIKI_LIST, input));
    },
  },
  {
    name: 'grep',
    description: 'Find text in the files under path: the workspace by default, or a file or directory of it. A path inside the workspace searches its git-tracked files (or, outside a git repository, every file under it); a path outside the workspace is walked and its hits show absolute paths. files is git or walk, the source used. mode is literal (default; also named content), regex, or word (a whole identifier). If a literal search finds nothing and the pattern has a regex character, the pattern runs once more as a regex; the hint says which pass matched, and a pattern that is not a valid regex is an error. scope is tracked (default) or all: all also searches untracked and gitignored files, node_modules included, and skips .git and the state directories .gm, .agentplug, .agentplug-kv, .gm-new, .playwriter and runs. A tracked search that skips files says how many in a hint and names scope all. Returns path:line text. ignoreCase is optional; limit is 1 to 1000, default 100. Paths under node_modules/ or vendor/ are skipped by tracked unless vendor is true; a walk also skips dot-directories and runs/. all lists files with git ls-files, which has a 30 s limit, so a large tree can take longer. root (or its aliases projectPath and cwd) names another existing directory to search instead of the workspace; with a root other than the workspace, hits show absolute paths. An unknown argument is refused by name.',
    schema: JSON.parse('{"type":"object","properties":{"pattern":{"type":"string"},"path":{"type":"string"},"mode":{"type":"string"},"scope":{"type":"string"},"ignoreCase":{"type":"boolean"},"limit":{"type":"number"},"offset":{"type":"number"},"vendor":{"type":"boolean"},"root":{"type":"string"}},"required":["pattern"]}'),
    lock: () => ({ mode: 'read', paths: [] }),
    async call(args, host) {
      const input = args ?? {};
      return guarded(host, 'grep', input, (ctx) => grepOp(host, ctx, input));
    },
  },
  {
    name: 'wiki_write',
    description: 'Write a wiki page, wiki/<page>.md, with its source ids and its claims. A claim quote must appear in its named source (sources/<id>.txt), or the whole page is refused and nothing is written. Updates wiki/index.md and wiki/log.md. kind is ingest (default), answer or lint. page is lowercase letters, digits and hyphens. contradicts and resolves name pages.',
    schema: JSON.parse('{"type":"object","properties":{"page":{"type":"string"},"title":{"type":"string"},"body":{"type":"string"},"summary":{"type":"string"},"sources":{"type":"array","items":{"type":"string"}},"claims":{"type":"array","items":{"type":"object"}},"kind":{"type":"string"},"contradicts":{"type":"array","items":{"type":"string"}},"resolves":{"type":"array","items":{"type":"string"}}},"required":["page","title","body","summary","sources"]}'),
    lock: (args) => ({ mode: 'write', paths: wikiPaths(args) }),
    async call(args, host) {
      return wikiWrite(host, normalizePath(host.workspace), args ?? {});
    },
  },
];
