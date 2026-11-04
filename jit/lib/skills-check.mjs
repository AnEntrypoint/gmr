import { MCP_RULES } from '../kernel/feed.mjs';
import { verbs as codeVerbs } from '../verbs/code.mjs';
import { verbs as execVerbs } from '../verbs/exec.mjs';
import { verbs as fsVerbs } from '../verbs/fs.mjs';
import { verbs as netVerbs } from '../verbs/net.mjs';
import { verbs as reviewVerbs } from '../verbs/review.mjs';
import { verbs as rowVerbs } from '../verbs/rows.mjs';
import { verbs as searchVerbs } from '../verbs/search.mjs';

const IMPORTED_VERBS = [codeVerbs, execVerbs, fsVerbs, netVerbs, reviewVerbs, rowVerbs, searchVerbs];
const GIT_VERBS_FILE = 'jit/verbs/git.mjs';
const GIT_VERB = /\bverb\('([a-z][a-z0-9_]*)'/g;
const NATIVE_ONLY_VERBS = ['query_log'];
const SERVED_SKILLS = ['gm', 'gm-continue'];
const FINDING_CAP = 3;
const RULE_HEAD = 60;
const FRONTMATTER_OPEN = '---\n';
const YAML_SYNTAX_START = "-?:,[]{}#&*!|>'\"%@`";
const FIELD_KEY = /^[A-Za-z][A-Za-z0-9_-]*$/;
const RULE_LINE = /^(\d+)\. (.*)$/;
const WORD = /[A-Za-z0-9_]/;

const head = (text, cap) => Array.from(text).slice(0, cap).join('');
const messageOf = (err) => (err && err.message !== undefined ? String(err.message) : String(err));

function ruleBodies() {
  return MCP_RULES.split('\n').flatMap((line) => {
    const match = RULE_LINE.exec(line);
    return match ? [match[2]] : [];
  });
}

async function verbNames(host) {
  const names = new Set();
  for (const verbs of IMPORTED_VERBS) {
    for (const verb of verbs) names.add(verb.name);
  }
  for (const match of (await host.fs.read(`${host.checkout}/${GIT_VERBS_FILE}`)).matchAll(GIT_VERB)) {
    names.add(match[1]);
  }
  for (const name of NATIVE_ONLY_VERBS) names.add(name);
  return [...names];
}

function namesBare(text, name) {
  let from = 0;
  for (;;) {
    const at = text.indexOf(name, from);
    if (at < 0) return false;
    const end = at + name.length;
    const beforeOk = at === 0 || !(WORD.test(text[at - 1]) || text[at - 1] === '.');
    const afterOk = end >= text.length || !WORD.test(text[end]);
    if (beforeOk && afterOk) return true;
    from = end;
  }
}

function frontmatterInnerEnd(text) {
  if (!text.startsWith(FRONTMATTER_OPEN)) return null;
  const at = text.slice(FRONTMATTER_OPEN.length).indexOf('\n---');
  return at < 0 ? null : FRONTMATTER_OPEN.length + at;
}

function fieldOf(line) {
  const colon = line.indexOf(':');
  if (colon < 0) return null;
  const key = line.slice(0, colon);
  if (!FIELD_KEY.test(key)) return null;
  const after = line.slice(colon + 1);
  let at = 0;
  while (at < after.length && /\s/.test(after[at]) && after[at] !== '\r') at += 1;
  const value = after.slice(at);
  if (at === 0 || value.includes('\r')) return null;
  return { key, value };
}

function plainProblem(value) {
  if (value.length > 0 && YAML_SYNTAX_START.includes(value[0])) return 'starts with a character YAML reads as syntax';
  if (value.includes(': ') || value.endsWith(':')) return "has ': ' inside it, which YAML reads as a mapping";
  if (value.includes(' #')) return "has ' #' inside it, which YAML reads as a comment";
  return null;
}

// Throws the message the Rust check reports, so the two gates name the same fault.
function parseSkill(text, dir) {
  const innerEnd = frontmatterInnerEnd(text);
  if (innerEnd === null) throw new Error(`skills/${dir}/SKILL.md has no frontmatter`);
  const frontmatter = new Map();
  for (const line of text.slice(FRONTMATTER_OPEN.length, innerEnd).split('\n')) {
    const field = fieldOf(line);
    if (field === null) throw new Error(`skills/${dir}/SKILL.md: frontmatter line is not key: value: ${head(line, 40)}`);
    const problem = plainProblem(field.value);
    if (problem !== null) throw new Error(`skills/${dir}/SKILL.md: ${field.key} ${problem}; reword it`);
    frontmatter.set(field.key, field.value);
  }
  for (const key of ['name', 'description']) {
    if ((frontmatter.get(key) ?? '').trim() === '') throw new Error(`skills/${dir}/SKILL.md has no ${key}`);
  }
  const name = frontmatter.get('name');
  if (name !== dir) throw new Error(`skills/${dir}/SKILL.md: name is ${name}, and its URI ends in ${dir}; they must match`);
  return frontmatter;
}

async function statOrNull(host, path) {
  try {
    return (await host.fs.stat(path)) ?? null;
  } catch {
    return null;
  }
}

export async function skillsCheck(host, root) {
  const dir = `${root}/skills`;
  if ((await statOrNull(host, dir)) === null) return { ok: true, detail: 'no skills' };
  const names = await verbNames(host);
  const rules = ruleBodies();
  const findings = [];
  let count = 0;
  const skills = (await host.fs.list(dir)).map((entry) => entry.replace(/\/$/, '')).sort();
  for (const skill of skills) {
    let text;
    try {
      text = await host.fs.read(`${dir}/${skill}/SKILL.md`);
    } catch {
      continue;
    }
    count += 1;
    for (const name of names) {
      if (namesBare(text, name)) findings.push(`${skill} names ${name} without gm.`);
    }
    if (skill === 'gm') {
      const plain = text.split('gm.').join('');
      for (const rule of rules) {
        if (!plain.includes(rule)) findings.push(`gm skill lacks rule: ${head(rule, RULE_HEAD)}`);
      }
    }
  }
  for (const skill of SERVED_SKILLS) {
    let text;
    try {
      text = await host.fs.read(`${dir}/${skill}/SKILL.md`);
    } catch (err) {
      findings.push(`skills/${skill}/SKILL.md: ${messageOf(err)}`);
      continue;
    }
    try {
      parseSkill(text, skill);
    } catch (err) {
      findings.push(messageOf(err));
    }
  }
  if (findings.length === 0) {
    return { ok: true, detail: `${count} skill(s) name only served tools; ${SERVED_SKILLS.join(', ')} parse as served skills` };
  }
  return { ok: false, detail: findings.slice(0, FINDING_CAP).join(' | ') };
}
