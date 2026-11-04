const WORD_PARTS = '\\p{Alphabetic}\\p{M}\\p{Nd}\\p{Pc}';
const WORD = `[${WORD_PARTS}]`;
const NOT_WORD = `[^${WORD_PARTS}]`;
const POSIX = new Map([
  ['alpha', 'a-zA-Z'],
  ['digit', '0-9'],
  ['alnum', 'a-zA-Z0-9'],
  ['upper', 'A-Z'],
  ['lower', 'a-z'],
  ['space', '\\t\\n\\x0b\\f\\r '],
  ['blank', '\\t '],
  ['punct', '!-\\/:-@\\[-`{-~'],
  ['xdigit', '0-9A-Fa-f'],
  ['word', 'A-Za-z0-9_'],
  ['cntrl', '\\x00-\\x1f\\x7f'],
  ['print', ' -~'],
  ['graph', '!-~'],
  ['ascii', '\\x00-\\x7f'],
]);
const SYNTAX = '^$\\.*+?()[]{}|/';
const CLASS_SYNTAX = '^$\\.*+?()[]{}|/-';
const LOOKAROUND = ['(?=', '(?!', '(?<=', '(?<!'];
const SIMPLE = new Map([
  ['n', '\\n'],
  ['t', '\\t'],
  ['r', '\\r'],
  ['f', '\\f'],
  ['v', '\\v'],
  ['a', '\\x07'],
]);
const HEX_RUN = /^[0-9a-fA-F]+$/;

export const REGEX_CHARS = /[.|()[\]{}*+?^$\\]/;

function bad(message) {
  return new Error(message);
}

function escapeAt(p, at, inClass) {
  const c = p.charAt(at + 1);
  if (c === '') throw bad('trailing backslash');
  if (/[0-9]/.test(c)) throw bad('backreferences and octal escapes are not supported');
  const next = at + 2;
  if (SIMPLE.has(c)) return { text: SIMPLE.get(c), next };
  switch (c) {
    case 'd':
      return { text: '\\p{Nd}', next };
    case 'D':
      return { text: '\\P{Nd}', next };
    case 's':
      return { text: '\\p{White_Space}', next };
    case 'S':
      return { text: '\\P{White_Space}', next };
    case 'w':
      return { text: inClass ? WORD_PARTS : WORD, next };
    case 'W':
      if (inClass) throw bad('\\W in a class is not supported');
      return { text: NOT_WORD, next };
    case 'b':
      if (inClass) throw bad('word boundary in a class is not supported');
      return { text: `(?:(?<=${WORD})(?!${WORD})|(?<!${WORD})(?=${WORD}))`, next };
    case 'B':
      if (inClass) throw bad('word boundary in a class is not supported');
      return { text: `(?:(?<=${WORD})(?=${WORD})|(?<!${WORD})(?!${WORD}))`, next };
    case 'A':
    case 'z':
      if (inClass) throw bad(`\\${c} in a class is not supported`);
      return { text: c === 'A' ? '^' : '$', next };
    case 'x': {
      if (p.charAt(at + 2) === '{') {
        const end = p.indexOf('}', at + 3);
        if (end < 0) throw bad('unclosed hex escape');
        const hex = p.slice(at + 3, end);
        if (!HEX_RUN.test(hex) || parseInt(hex, 16) > 0x10ffff) throw bad('invalid hex escape');
        return { text: `\\u{${hex}}`, next: end + 1 };
      }
      const hex = p.slice(at + 2, at + 4);
      if (!/^[0-9a-fA-F]{2}$/.test(hex)) throw bad('invalid hex escape');
      return { text: `\\x${hex}`, next: at + 4 };
    }
    case 'u': {
      const hex = p.slice(at + 2, at + 6);
      if (!/^[0-9a-fA-F]{4}$/.test(hex)) throw bad('invalid unicode escape');
      return { text: `\\u${hex}`, next: at + 6 };
    }
    case 'U': {
      const hex = p.slice(at + 2, at + 10);
      if (!/^[0-9a-fA-F]{8}$/.test(hex) || parseInt(hex, 16) > 0x10ffff) throw bad('invalid unicode escape');
      return { text: `\\u{${hex}}`, next: at + 10 };
    }
    case 'p':
    case 'P': {
      if (p.charAt(at + 2) === '{') {
        const end = p.indexOf('}', at + 3);
        if (end < 0) throw bad('unclosed property name');
        return { text: `\\${c}{${p.slice(at + 3, end)}}`, next: end + 1 };
      }
      const name = p.charAt(at + 2);
      if (name === '') throw bad('missing property name');
      return { text: `\\${c}{${name}}`, next: at + 3 };
    }
    case 'Q': {
      if (inClass) throw bad('\\Q in a class is not supported');
      const end = p.indexOf('\\E', at + 2);
      const stop = end < 0 ? p.length : end;
      let text = '';
      for (const ch of p.slice(at + 2, stop)) text += SYNTAX.includes(ch) ? `\\${ch}` : ch;
      return { text, next: end < 0 ? p.length : end + 2 };
    }
    default:
      if (/[A-Za-z]/.test(c)) throw bad(`unrecognized escape \\${c}`);
      return { text: (inClass ? CLASS_SYNTAX : SYNTAX).includes(c) ? `\\${c}` : c, next };
  }
}

export function translateRegex(pattern, ignoreCase) {
  let flags = ignoreCase ? 'iu' : 'u';
  let at = 0;
  const lead = /^\(\?([a-zA-Z]+)\)/.exec(pattern);
  if (lead !== null) {
    for (const flag of lead[1]) {
      if (flag !== 'i' && flag !== 'm' && flag !== 's') throw bad(`inline flag ${flag} is not supported`);
      if (!flags.includes(flag)) flags += flag;
    }
    at = lead[0].length;
  }
  const dotAll = flags.includes('s');
  let out = '';
  let inClass = false;
  while (at < pattern.length) {
    const c = pattern.charAt(at);
    if (c === '\\') {
      const escaped = escapeAt(pattern, at, inClass);
      out += escaped.text;
      at = escaped.next;
    } else if (inClass) {
      if (c === ']') {
        inClass = false;
        out += c;
        at += 1;
      } else if (c === '[') {
        const posix = /^\[:(\^?)([a-z]+):\]/.exec(pattern.slice(at));
        if (posix === null || posix[1] !== '' || !POSIX.has(posix[2])) throw bad('nested character classes are not supported');
        out += POSIX.get(posix[2]);
        at += posix[0].length;
      } else if ((c === '&' || c === '-' || c === '~') && pattern.charAt(at + 1) === c) {
        throw bad('character class set operations are not supported');
      } else {
        out += c;
        at += 1;
      }
    } else if (c === '[') {
      out += '[';
      at += 1;
      inClass = true;
      if (pattern.charAt(at) === '^') {
        out += '^';
        at += 1;
      }
      if (pattern.charAt(at) === ']') {
        out += '\\]';
        at += 1;
      }
    } else if (c === '(') {
      if (LOOKAROUND.some((look) => pattern.startsWith(look, at))) {
        throw bad('look-around, including look-ahead and look-behind, is not supported');
      }
      if (pattern.startsWith('(?P<', at)) {
        out += '(?<';
        at += 4;
      } else if (pattern.startsWith('(?:', at)) {
        out += '(?:';
        at += 3;
      } else if (pattern.startsWith('(?<', at)) {
        out += '(?<';
        at += 3;
      } else if (pattern.charAt(at + 1) === '?') {
        throw bad('this group syntax is not supported');
      } else {
        out += '(';
        at += 1;
      }
    } else if (c === '.') {
      out += dotAll ? '.' : '[^\\n]';
      at += 1;
    } else if (c === '{') {
      const quant = /^\{\d+(?:,\d*)?\}/.exec(pattern.slice(at));
      if (quant !== null) {
        out += quant[0];
        at += quant[0].length;
      } else {
        out += '\\u007b';
        at += 1;
      }
    } else if (c === '}') {
      out += '\\u007d';
      at += 1;
    } else if (c === ']') {
      out += '\\u005d';
      at += 1;
    } else {
      out += c;
      at += 1;
    }
  }
  if (inClass) throw bad('unclosed character class');
  return { source: out, flags };
}

export function compileRegex(pattern, ignoreCase) {
  const { source, flags } = translateRegex(pattern, ignoreCase);
  return new RegExp(source, flags);
}

export function hasWord(hay, needle) {
  if (needle === '') return false;
  const end0 = needle.length;
  let at = hay.indexOf(needle);
  while (at >= 0) {
    const end = at + end0;
    if ((at === 0 || !isWordCode(hay.charCodeAt(at - 1))) && (end === hay.length || !isWordCode(hay.charCodeAt(end)))) {
      return true;
    }
    at = hay.indexOf(needle, at + 1);
  }
  return false;
}

function isWordCode(code) {
  return (code >= 48 && code <= 57) || (code >= 65 && code <= 90) || (code >= 97 && code <= 122) || code === 95;
}

export function makeMatcher(pattern, mode, ignoreCase) {
  if (mode === 'regex') {
    const re = compileRegex(pattern, ignoreCase);
    return { test: (text) => re.test(text) };
  }
  const needle = ignoreCase ? pattern.toLowerCase() : pattern;
  if (mode === 'word') {
    return { test: (text) => hasWord(ignoreCase ? text.toLowerCase() : text, needle) };
  }
  return { test: (text) => (ignoreCase ? text.toLowerCase() : text).includes(needle) };
}
