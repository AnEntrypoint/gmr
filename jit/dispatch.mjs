import { failureKind } from './lib/retry.mjs';

const NETWORK_VERBS = new Set(['fetch', 'ci_status']);
const RETRIES = 3;
const BASE_MS = 1000;
const CAP_MS = 16000;
const JITTER = 0.2;

export function backoffMs(attempt) {
  const base = Math.min(BASE_MS * 2 ** Math.min(Math.max(attempt - 1, 0), 16), CAP_MS);
  return Math.round(base * (1 + JITTER * (2 * Math.random() - 1)));
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const messageOf = (err) => (err !== null && typeof err === 'object' && 'message' in err ? String(err.message) : String(err));

const registry = new Map();

export function register(file, verbs) {
  if (!Array.isArray(verbs)) throw new Error(`${file} does not export a verbs array`);
  const names = [];
  for (const verb of verbs) {
    if (verb === null || typeof verb !== 'object' || typeof verb.name !== 'string' || typeof verb.call !== 'function') {
      throw new Error(`${file} exports a verb without a name or a call function`);
    }
    names.push(verb.name);
  }
  for (const verb of verbs) registry.set(verb.name, { verb, file });
  return names;
}

export function listing() {
  return [...registry.values()].map(({ verb, file }) => ({
    name: verb.name,
    description: typeof verb.description === 'string' ? verb.description : '',
    inputSchema: verb.schema && typeof verb.schema === 'object' ? verb.schema : { type: 'object' },
    file,
  }));
}

export function has(name) {
  return registry.has(name);
}

const isAbsolute = (path) => path.startsWith('/') || path.startsWith('\\') || /^[A-Za-z]:[\\/]/.test(path);

function lockPaths(paths, host) {
  const root = String(host.workspace || '').replace(/[\\/]+$/, '');
  return (Array.isArray(paths) ? paths : [])
    .filter((path) => typeof path === 'string' && path !== '')
    .map((path) => (isAbsolute(path) || root === '' ? path : `${root}/${path.replace(/^\.[\\/]/, '')}`));
}

async function locked(verb, args, host) {
  const want = typeof verb.lock === 'function' ? verb.lock(args, host) : null;
  const paths = want ? lockPaths(want.paths, host) : [];
  const whole = want && want.mode === 'write' && paths.length === 0 && host.workspace ? [String(host.workspace)] : paths;
  const token = want ? await host.lock.acquire(want.mode, whole) : null;
  try {
    return await verb.call(args, host);
  } finally {
    if (token !== null && token !== undefined) host.lock.release(token);
  }
}

async function once(verb, args, host) {
  try {
    const result = await locked(verb, args, host);
    return result === undefined ? { ok: true } : result;
  } catch (err) {
    return { ok: false, error: messageOf(err) };
  }
}

async function retrying(name, verb, args, host) {
  for (let attempt = 1; ; attempt += 1) {
    const result = await once(verb, args, host);
    const kind = failureKind(result);
    const settled = result !== null && typeof result === 'object';
    if (kind === null || attempt > RETRIES) {
      if (settled) {
        result.attempts = attempt;
        if (kind !== null) result.exhausted = true;
      }
      return result;
    }
    const delayMs = backoffMs(attempt);
    await host.log.append('retry', { tool: name, attempt, errorType: kind, delayMs });
    await sleep(delayMs);
  }
}

export async function dispatch(name, args, host) {
  const entry = registry.get(name);
  if (!entry) return { ok: false, error: `unknown verb: ${name}` };
  const input = args === null || args === undefined ? {} : args;
  return NETWORK_VERBS.has(name) ? retrying(name, entry.verb, input, host) : once(entry.verb, input, host);
}
