export const K1 = 1.2;
export const B = 0.75;

const SNIPPET_UNITS = 300;
const WORD_RUN = /[A-Za-z0-9_]+/g;
const UPPER_OR_UNDERSCORE = /[A-Z_]/;

export function snippet(text) {
  if (text.length <= SNIPPET_UNITS) return text;
  let units = 0;
  let end = 0;
  for (const ch of text) {
    if (units + ch.length > SNIPPET_UNITS) break;
    units += ch.length;
    end += ch.length;
  }
  return `${text.slice(0, end)} ...`;
}

function isUpper(code) {
  return code >= 65 && code <= 90;
}

function isLower(code) {
  return code >= 97 && code <= 122;
}

function isDigit(code) {
  return code >= 48 && code <= 57;
}

function partsOf(word) {
  const parts = [];
  let start = 0;
  for (let i = 0; i < word.length; i += 1) {
    const code = word.charCodeAt(i);
    if (code === 95) {
      if (i > start) parts.push(word.slice(start, i));
      start = i + 1;
    } else if (i > 0 && isUpper(code) && (isLower(word.charCodeAt(i - 1)) || isDigit(word.charCodeAt(i - 1)))) {
      if (i > start) parts.push(word.slice(start, i));
      start = i;
    }
  }
  if (word.length > start) parts.push(word.slice(start));
  return parts;
}

export function forEachToken(text, visit) {
  WORD_RUN.lastIndex = 0;
  let match = WORD_RUN.exec(text);
  while (match !== null) {
    const word = match[0];
    visit(word.toLowerCase());
    if (UPPER_OR_UNDERSCORE.test(word)) {
      const parts = partsOf(word);
      if (parts.length > 1) {
        for (const part of parts) visit(part.toLowerCase());
      }
    }
    match = WORD_RUN.exec(text);
  }
}

export function termsOf(query) {
  const seen = new Set();
  const terms = [];
  forEachToken(query, (token) => {
    if (!seen.has(token)) {
      seen.add(token);
      terms.push(token);
    }
  });
  return terms;
}

export function countTerms(text, termIndex, counts) {
  counts.fill(0);
  let len = 0;
  forEachToken(text, (token) => {
    len += 1;
    const at = termIndex.get(token);
    if (at !== undefined) counts[at] += 1;
  });
  return len;
}

export function idfOf(docs, df) {
  return Math.log(1 + (docs - df + 0.5) / (df + 0.5));
}

export function weightOf(tf, len, avg) {
  return (tf * (K1 + 1)) / (tf + K1 * (1 - B + (B * len) / avg));
}

function better(score, id, other, otherId) {
  return score > other || (score === other && id < otherId);
}

export function offerHit(top, k, score, id, rel, line, text) {
  if (top.length === k) {
    const worst = top[k - 1];
    if (!better(score, id, worst.score, worst.id)) return;
    top.pop();
  }
  let at = top.findIndex((hit) => better(score, id, hit.score, hit.id));
  if (at < 0) at = top.length;
  top.splice(at, 0, { score, id, rel, line, text: snippet(text) });
}
