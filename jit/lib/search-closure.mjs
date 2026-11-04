import { snippet } from './search-rank.mjs';

const HEAD_FN = /^(?:export\s+)?(?:async\s+)?function\s*\*?\s*([A-Za-z_$][\w$]*)\s*\(/;
const HEAD_VAR = /^(?:export\s+)?(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=\s*(?:async\s+)?(?:function\b|\([^()]*\)\s*=>|\(\s*\)\s*=>|[A-Za-z_$][\w$]*\s*=>)/;
const JS_FILE = /\.(m?js|cjs)$/;
const REGEX_AFTER_WORD = new Set(['return', 'typeof', 'case', 'do', 'else', 'in', 'of', 'void', 'yield', 'await', 'throw', 'new', 'delete', 'instanceof']);
const NOT_CALLEE = new Set(['if', 'for', 'while', 'switch', 'catch', 'with', 'return', 'typeof', 'void', 'delete', 'await', 'yield', 'in', 'of', 'do', 'else', 'case', 'throw', 'import', 'super', 'instanceof', 'function', 'class', 'new', 'async', 'var', 'let', 'const', 'extends', 'default']);

export function isJsFile(rel) {
  return JS_FILE.test(rel);
}

function isIdStart(code) {
  return (code >= 65 && code <= 90) || (code >= 97 && code <= 122) || code === 95 || code === 36 || code >= 128;
}

function isIdPart(code) {
  return isIdStart(code) || (code >= 48 && code <= 57);
}

function endOfString(s, from, quote) {
  for (let j = from; j < s.length; j += 1) {
    const ch = s.charAt(j);
    if (ch === '\\') j += 1;
    else if (ch === quote) return j + 1;
  }
  return s.length;
}

function endOfTemplate(s, from) {
  for (let j = from; j < s.length; j += 1) {
    const ch = s.charAt(j);
    if (ch === '\\') j += 1;
    else if (ch === '`') return j + 1;
  }
  return -1;
}

function endOfRegex(s, from) {
  let inClass = false;
  for (let j = from; j < s.length; j += 1) {
    const ch = s.charAt(j);
    if (ch === '\\') j += 1;
    else if (ch === '[') inClass = true;
    else if (ch === ']') inClass = false;
    else if (ch === '/' && !inClass) {
      let end = j + 1;
      while (end < s.length && isIdPart(s.charCodeAt(end))) end += 1;
      return end;
    }
  }
  return -1;
}

function regexAllowed(kind, value) {
  if (kind === '') return true;
  if (kind === 'id') return REGEX_AFTER_WORD.has(value);
  if (kind === 'p') return value !== ')' && value !== ']' && value !== '}';
  return false;
}

function* tokenize(lines, from) {
  let inBlock = false;
  let inTemplate = false;
  let kind = '';
  let value = '';
  for (let li = from; li < lines.length; li += 1) {
    const s = lines[li];
    const n = s.length;
    let i = 0;
    if (inBlock) {
      const close = s.indexOf('*/');
      if (close < 0) {
        yield { t: 'eol', v: '', line: li };
        continue;
      }
      inBlock = false;
      i = close + 2;
    } else if (inTemplate) {
      const close = endOfTemplate(s, 0);
      if (close < 0) {
        yield { t: 'eol', v: '', line: li };
        continue;
      }
      inTemplate = false;
      i = close;
    }
    while (i < n) {
      const c = s.charAt(i);
      if (c === ' ' || c === '\t' || c === '\r' || c === '\f' || c === '\v') {
        i += 1;
      } else if (c === '/' && s.charAt(i + 1) === '/') {
        break;
      } else if (c === '/' && s.charAt(i + 1) === '*') {
        const close = s.indexOf('*/', i + 2);
        if (close < 0) {
          inBlock = true;
          break;
        }
        i = close + 2;
      } else if (c === '"' || c === "'") {
        i = endOfString(s, i + 1, c);
        kind = 'lit';
        value = '';
        yield { t: 'lit', v: '', line: li };
      } else if (c === '`') {
        const close = endOfTemplate(s, i + 1);
        yield { t: 'lit', v: '', line: li };
        kind = 'lit';
        value = '';
        if (close < 0) {
          inTemplate = true;
          break;
        }
        i = close;
      } else if (c === '/' && regexAllowed(kind, value) && endOfRegex(s, i + 1) >= 0) {
        i = endOfRegex(s, i + 1);
        kind = 'lit';
        value = '';
        yield { t: 'lit', v: '', line: li };
      } else if (isIdStart(s.charCodeAt(i))) {
        let j = i + 1;
        while (j < n && isIdPart(s.charCodeAt(j))) j += 1;
        value = s.slice(i, j);
        kind = 'id';
        yield { t: 'id', v: value, line: li };
        i = j;
      } else if (s.charCodeAt(i) >= 48 && s.charCodeAt(i) <= 57) {
        let j = i + 1;
        while (j < n && (isIdPart(s.charCodeAt(j)) || s.charAt(j) === '.')) j += 1;
        kind = 'num';
        value = '';
        yield { t: 'num', v: '', line: li };
        i = j;
      } else if (c === '=' && s.charAt(i + 1) === '>') {
        kind = 'p';
        value = '=>';
        yield { t: 'p', v: '=>', line: li };
        i += 2;
      } else {
        kind = 'p';
        value = c;
        yield { t: 'p', v: c, line: li };
        i += 1;
      }
    }
    yield { t: 'eol', v: '', line: li };
  }
}

function scanDef(lines, start, arrow) {
  const callees = [];
  const seen = new Set();
  let depth = 0;
  let opened = false;
  let prev = '';
  let candidate = null;
  for (const tok of tokenize(lines, start)) {
    if (tok.t === 'eol') {
      candidate = null;
      if (arrow && depth === 0 && !opened && prev !== '=>') return { end: tok.line, callees };
      continue;
    }
    if (candidate !== null && tok.t === 'p' && tok.v === '(' && !seen.has(candidate)) {
      seen.add(candidate);
      callees.push(candidate);
    }
    candidate = null;
    if (tok.t === 'id') {
      if (!NOT_CALLEE.has(tok.v) && prev !== '.' && prev !== 'new' && prev !== 'function') candidate = tok.v;
      prev = tok.v;
    } else if (tok.t === 'p') {
      if (tok.v === '(' || tok.v === '[' || tok.v === '{') {
        depth += 1;
        if (tok.v === '{') opened = true;
      } else if (tok.v === ')' || tok.v === ']' || tok.v === '}') {
        depth -= 1;
        if (tok.v === '}' && depth === 0 && opened) return { end: tok.line, callees };
      } else if (tok.v === ';' && depth === 0) {
        return { end: tok.line, callees };
      }
      prev = tok.v;
    } else {
      prev = '#lit';
    }
  }
  return { end: lines.length - 1, callees };
}

function topFunctions(rel, lines) {
  const out = [];
  let at = 0;
  while (at < lines.length) {
    const fn = HEAD_FN.exec(lines[at]);
    const head = fn !== null ? fn : HEAD_VAR.exec(lines[at]);
    if (head === null) {
      at += 1;
      continue;
    }
    const { end, callees } = scanDef(lines, at, fn === null);
    out.push({ name: head[1], file: rel, start: at + 1, end: end + 1, callees, lineText: lines[at] });
    at = end + 1;
  }
  return out;
}

export function createClosureIndex() {
  return { order: [], defs: new Map(), dupes: new Set() };
}

export function addFileDefs(index, rel, lines) {
  for (const def of topFunctions(rel, lines)) {
    if (index.defs.has(def.name)) index.dupes.add(def.name);
    else index.order.push(def.name);
    index.defs.set(def.name, def);
  }
}

export function closureLines(index, hits, shownAt, anchorCount = 10, cap = 20) {
  const anchors = hits.slice(0, anchorCount);
  const top = new Set(anchors.map((hit) => hit.rel));
  const anchored = new Map();
  const anchoredOrder = [];
  for (const hit of anchors) {
    for (const name of index.order) {
      const def = index.defs.get(name);
      if (def.file === hit.rel && def.start <= hit.line && hit.line <= def.end) {
        if (!anchored.has(name)) anchoredOrder.push(name);
        anchored.set(name, def);
      }
    }
  }
  const pickedOrder = [];
  const picked = new Map();
  for (const name of anchoredOrder) {
    const def = anchored.get(name);
    for (const callee of def.callees) {
      const target = index.defs.get(callee);
      if (target === undefined) continue;
      if (index.dupes.has(callee) || callee === def.name || target.file === def.file) continue;
      if (top.has(target.file) || picked.has(target.file)) continue;
      pickedOrder.push(target.file);
      picked.set(target.file, target);
    }
  }
  return pickedOrder.slice(0, cap).map((file) => {
    const target = picked.get(file);
    return `${shownAt(file)}:${target.start} closure ${snippet(target.lineText.replace(/\r+$/, ''))}`;
  });
}
