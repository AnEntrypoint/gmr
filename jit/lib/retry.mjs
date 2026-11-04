export const TRANSIENT_CODES = [
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

export const TEXT_PATTERNS = [
  [/connection refused|econnrefused/i, 'ECONNREFUSED'],
  [/failed to connect|error connecting to/i, 'connect'],
  [/connection reset|econnreset|reset by peer|und_err_socket/i, 'ECONNRESET'],
  [/timed out|timeout|etimedout|aborterror|aborted/i, 'ETIMEDOUT'],
  [/hung up unexpectedly|remote end hung up|socket hang up/i, 'hung-up'],
  [/temporary failure in name resolution/i, 'EAI_AGAIN'],
];

export const STATUS_PATTERN = /(?:HTTP|returned error:?|status code)\s*(408|429|5\d\d)\b/i;
export const URL_PATTERN = /[a-z][a-z0-9+.-]*:\/\/\S*/gi;
export const HOST_PATTERN = /\b[a-z0-9-]+(?:\.[a-z0-9-]+)+\b/gi;

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

const KINDS = new Set([...TRANSIENT_CODES, ...TEXT_PATTERNS.map(([, kind]) => kind)]);
const STATUS_KIND = /^HTTP-(408|429|5\d\d)$/;

function codeKind(code) {
  if (typeof code !== 'string' || code === '') return null;
  return KINDS.has(code) || STATUS_KIND.test(code) ? code : errorType(code);
}

export function failureKind(result) {
  if (result === null || typeof result !== 'object') return null;
  if (result.ok === true) return typeof result.status === 'number' ? errorType(`HTTP ${result.status}`) : null;
  const codes = Array.isArray(result.codes) ? result.codes : [];
  for (const code of [result.code, result.errorType, ...codes]) {
    const kind = codeKind(code);
    if (kind) return kind;
  }
  return errorType([result.error, result.stderr].filter((v) => typeof v === 'string').join('\n'));
}
