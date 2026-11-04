const CARGO_TIMEOUT_MS = 600000;
const LS_TIMEOUT_MS = 60000;
const BINARY_PROBE = 8000;
const TAIL_LINES = 12;
const RUNTIME_DIRS = ['runs', 'node_modules', '.gm', '.agentplug-kv'];
const NOT_COVERED = [
  'semantic correctness of the change: the gate compiles rust/ and reads the docs paths, and does not judge what the change means',
  'behaviour the program does not exercise: the gate runs cargo check, not the program, and has no smoke run',
  'doc content beyond path existence: the docs check finds the repo paths in DESIGN.md, not whether a claim in the docs is true',
  'docs paths in AGENTS.md and README.md: only DESIGN.md is checked',
  'CI: none is configured, and no CI result is read',
];
const ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
const LOOKUP = [];
for (let i = 0; i < ALPHABET.length; i += 1) LOOKUP[ALPHABET.charCodeAt(i)] = i;
LOOKUP['='.charCodeAt(0)] = 0;

export function messageOf(err) {
  return err && err.message !== undefined ? String(err.message) : String(err);
}

export async function runTool(host, file, args, opts) {
  let reply;
  try {
    reply = await host.proc.run(file, args, { stdin: '', ...opts });
  } catch (err) {
    return { ok: false, exitCode: null, output: '', error: messageOf(err) };
  }
  if (reply.timedOut) {
    return { ok: false, exitCode: null, output: '', error: `timed out after ${opts.timeoutMs} ms` };
  }
  return { ok: reply.code === 0, exitCode: reply.code ?? null, output: `${reply.stdout ?? ''}${reply.stderr ?? ''}`, error: undefined };
}

export async function runGate(host, root, scope = null) {
  const tracked = await trackedFiles(host, root);
  const checks = [await timed(host, 'ascii', () => asciiCheck(host, root, tracked, scope))];
  const notCovered = [...NOT_COVERED];
  if (await isFile(host, `${root}/DESIGN.md`)) {
    checks.push(await timed(host, 'docs', () => docsCheck(host, root, tracked)));
  } else {
    notCovered.push(`docs: no DESIGN.md in ${root}, so the docs check did not run`);
  }
  if (await isFile(host, `${root}/skills/gm/SKILL.md`)) {
    notCovered.push(`skills, wiki: the JIT gate does not run the skill and wiki checks that the Rust gate runs, in ${root}`);
  } else {
    notCovered.push(`skills, wiki: no skills/gm/SKILL.md in ${root}, so it is not a gm-new checkout and these checks did not run`);
  }
  if (rustInScope(scope)) {
    checks.push(await timed(host, 'cargo', () => cargoCheck(host, root)));
  } else {
    notCovered.push('cargo: no rust/ path in this commit, so cargo check of rust/ did not run');
  }
  const failed = checks.filter((check) => !check.ok).map((check) => check.name);
  return { ok: failed.length === 0, failed, checks, notCovered };
}

async function timed(host, name, check) {
  const started = host.now().mono;
  let verdict;
  try {
    verdict = await check();
  } catch (err) {
    verdict = { ok: false, detail: messageOf(err) };
  }
  return { name, ok: verdict.ok, durMs: Math.round(host.now().mono - started), detail: verdict.detail };
}

async function statOrNull(host, path) {
  try {
    return (await host.fs.stat(path)) ?? null;
  } catch {
    return null;
  }
}

async function isFile(host, path) {
  const meta = await statOrNull(host, path);
  return meta !== null && meta.type === 'file';
}

async function trackedFiles(host, root) {
  const listing = await runTool(host, 'git', ['ls-files', '-z'], { cwd: root, timeoutMs: LS_TIMEOUT_MS });
  if (!listing.ok) throw new Error(listing.error ?? listing.output);
  return listing.output.split('\0').filter((file) => file !== '');
}

function inScope(file, scope) {
  return scope === null || scope.some((path) => file === path || file.startsWith(`${path}/`));
}

function rustInScope(scope) {
  return scope === null || scope.some((path) => path === 'rust' || path.startsWith('rust/'));
}

function scanBase64(encoded) {
  const pad = encoded.endsWith('==') ? 2 : encoded.endsWith('=') ? 1 : 0;
  const total = (encoded.length / 4) * 3 - pad;
  let binary = false;
  let high = false;
  let at = 0;
  for (let i = 0; i < encoded.length && at < total; i += 4) {
    const bits = (LOOKUP[encoded.charCodeAt(i)] << 18) | (LOOKUP[encoded.charCodeAt(i + 1)] << 12) | (LOOKUP[encoded.charCodeAt(i + 2)] << 6) | LOOKUP[encoded.charCodeAt(i + 3)];
    for (let k = 0; k < 3 && at < total; k += 1) {
      const byte = (bits >> (16 - 8 * k)) & 0xff;
      if (byte === 0 && at < BINARY_PROBE) binary = true;
      if (byte >= 0x80) high = true;
      at += 1;
    }
  }
  return { binary, high };
}

async function asciiCheck(host, root, tracked, scope) {
  const bad = [];
  let count = 0;
  for (const rel of tracked) {
    if (!inScope(rel, scope)) continue;
    const path = `${root}/${rel}`;
    const meta = await statOrNull(host, path);
    if (meta === null || meta.type !== 'file') continue;
    let encoded;
    try {
      encoded = await host.fs.readBytes(path);
    } catch (err) {
      throw new Error(`${rel}: ${messageOf(err)}`);
    }
    const { binary, high } = scanBase64(encoded);
    if (binary) continue;
    count += 1;
    if (high) bad.push(rel);
  }
  return bad.length === 0 ? { ok: true, detail: `${count} text files are ASCII` } : { ok: false, detail: `non-ASCII in ${bad.join(', ')}` };
}

async function docsCheck(host, root, tracked) {
  const text = await host.fs.read(`${root}/DESIGN.md`);
  const indexed = new Set(tracked);
  const tops = new Set(tracked.map((file) => file.split('/')[0]));
  const parts = text.split('`');
  const missing = [];
  let checked = 0;
  for (let index = 0; index < parts.length; index += 1) {
    const segment = parts[index];
    if (index % 2 === 0 || segment === '' || /\s/.test(segment)) continue;
    const token = stripLineSuffix(segment);
    if (!isRepoPath(token, tops)) continue;
    checked += 1;
    const exists = token.endsWith('/') ? tracked.some((file) => file.startsWith(token)) : indexed.has(token);
    if (!exists) missing.push(token);
  }
  if (missing.length === 0) return { ok: true, detail: `${checked} paths in DESIGN.md exist` };
  return { ok: false, detail: `missing: ${missing.map((token) => `DESIGN.md: ${token}`).join(' | ')}` };
}

function stripLineSuffix(token) {
  let end = token;
  for (let round = 0; round < 2; round += 1) {
    const at = end.lastIndexOf(':');
    if (at >= 0 && at + 1 < end.length && /^[0-9]+$/.test(end.slice(at + 1))) {
      end = end.slice(0, at);
    } else {
      break;
    }
  }
  return end;
}

function isRepoPath(token, tops) {
  if (/[<*=]/.test(token) || token.includes('://') || token.startsWith('/')) return false;
  if (RUNTIME_DIRS.some((dir) => token === dir || token.startsWith(`${dir}/`))) return false;
  return token.includes('/') && tops.has(token.split('/')[0]);
}

async function cargoCheck(host, root) {
  const crate = `${root}/rust`;
  if (!(await isFile(host, `${crate}/Cargo.toml`))) return { ok: false, detail: 'rust/Cargo.toml is missing' };
  const run = await runTool(host, 'cargo', ['check', '--color', 'never'], { cwd: crate, timeoutMs: CARGO_TIMEOUT_MS });
  if (run.ok) return { ok: true, detail: 'cargo check of rust/ passes' };
  const lines = (run.error ?? run.output).split(/\r?\n/).filter((line) => line.trim() !== '');
  const tail = lines.slice(Math.max(0, lines.length - TAIL_LINES));
  return { ok: false, detail: `cargo check of rust/ failed: ${tail.join(' | ')}` };
}
