const NETWORK_VERBS = new Set(['fetch', 'ci_status']);
const RETRIES = 3;
const BASE_MS = 1000;
const CAP_MS = 16000;
const JITTER = 0.2;

const TRANSIENT_CODES = [
  'ECONNRESET',
  'ECONNREFUSED',
  'ECONNABORTED',
  'ETIMEDOUT',
  'EAI_AGAIN',
  'UND_ERR_SOCKET',
  'UND_ERR_CONNECT_TIMEOUT',
  'UND_ERR_HEADERS_TIMEOUT',
  'UND_ERR_BODY_TIMEOUT',
];

const TEXT_PATTERNS = [
  [/connection refused|econnrefused/i, 'ECONNREFUSED'],
  [/failed to connect|error connecting to/i, 'connect'],
  [/connection reset|econnreset|reset by peer|und_err_socket/i, 'ECONNRESET'],
  [/timed out|timeout|etimedout|aborterror|aborted/i, 'ETIMEDOUT'],
  [/hung up unexpectedly|remote end hung up|socket hang up/i, 'hung-up'],
  [/temporary failure in name resolution/i, 'EAI_AGAIN'],
];

const STATUS_PATTERN = /(?:HTTP|returned error:?|status code)\s*(408|429|5\d\d)\b/i;
const URL_PATTERN = /[a-z][a-z0-9+.-]*:\/\/\S*/gi;
const HOST_PATTERN = /\b[a-z0-9-]+(?:\.[a-z0-9-]+)+\b/gi;

export function errorType(message) {
  if (typeof message !== 'string' || message === '') return null;
  const stripped = message.replace(URL_PATTERN, ' ').replace(HOST_PATTERN, ' ');
  const code = TRANSIENT_CODES.find((c) => stripped.includes(c));
  if (code) return code;
  const text = TEXT_PATTERNS.find(([pattern]) => pattern.test(stripped));
  if (text) return text[1];
  const status = STATUS_PATTERN.exec(stripped);
  return status ? `HTTP-${status[1]}` : null;
}

export function backoffMs(attempt) {
  const base = Math.min(BASE_MS * 2 ** Math.min(Math.max(attempt - 1, 0), 16), CAP_MS);
  return Math.round(base * (1 + JITTER * (2 * Math.random() - 1)));
}

const KINDS = new Set([...TRANSIENT_CODES, ...TEXT_PATTERNS.map(([, kind]) => kind)]);
const STATUS_KIND = /^HTTP-(408|429|5\d\d)$/;

function codeKind(code) {
  if (typeof code !== 'string' || code === '') return null;
  return KINDS.has(code) || STATUS_KIND.test(code) ? code : errorType(code);
}

function failureKind(result) {
  if (result === null || typeof result !== 'object') return null;
  if (result.ok === true) return typeof result.status === 'number' ? errorType(`HTTP ${result.status}`) : null;
  const codes = Array.isArray(result.codes) ? result.codes : [];
  for (const code of [result.code, result.errorType, ...codes]) {
    const kind = codeKind(code);
    if (kind) return kind;
  }
  return errorType([result.error, result.stderr].filter((v) => typeof v === 'string').join('\n'));
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
