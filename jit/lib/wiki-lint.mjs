import { validPage } from './search-wiki.mjs';

const KINDS = ['ingest', 'answer', 'lint'];
const SOURCE = /^[0-9a-f]{12}$/;
const ISO = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$/;
const FINDING_CAP = 3;
const LOG_DATE = /^\d{4}-\d{2}-\d{2}$/;
const LOG_DATE_LENGTH = 10;

const textOf = (value) => (typeof value === 'string' ? value : '');
const stringsOf = (value) => (Array.isArray(value) ? value.filter((item) => typeof item === 'string') : []);

async function statOrNull(host, path) {
  try {
    return (await host.fs.stat(path)) ?? null;
  } catch {
    return null;
  }
}

async function readOr(host, path, fallback) {
  try {
    return await host.fs.read(path);
  } catch {
    return fallback;
  }
}

async function sourceText(host, root, id) {
  if (!SOURCE.test(id)) return null;
  return readOr(host, `${root}/sources/${id}.txt`, null);
}

function isoMs(text) {
  const match = ISO.exec(text);
  if (match === null) return null;
  const [, year, month, day, hour, minute, second, fraction] = match;
  const millis = fraction === undefined ? 0 : Number(`${fraction}00`.slice(0, 3));
  return Date.UTC(Number(year), Number(month) - 1, Number(day), Number(hour), Number(minute), Number(second)) + millis;
}

function newer(a, b) {
  const [x, y] = [isoMs(a), isoMs(b)];
  return x !== null && y !== null && x > y;
}

function parsePage(text) {
  if (!text.startsWith('---\n')) return null;
  const rest = text.slice(4);
  const at = rest.indexOf('\n---\n');
  if (at < 0) return null;
  const page = { title: '', updated: '', sources: [], claims: [], contradicts: [], resolves: [], body: rest.slice(at + 5) };
  for (const line of rest.slice(0, at).split('\n')) {
    const sep = line.indexOf(': ');
    if (sep <= 0) continue;
    let value;
    try {
      value = JSON.parse(line.slice(sep + 2));
    } catch {
      return null;
    }
    switch (line.slice(0, sep)) {
      case 'title':
        page.title = textOf(value);
        break;
      case 'updated':
        page.updated = textOf(value);
        break;
      case 'sources':
        page.sources = stringsOf(value);
        break;
      case 'claims':
        page.claims = Array.isArray(value) ? value.map((claim) => [textOf(claim?.quote), textOf(claim?.source)]) : [];
        break;
      case 'contradicts':
        page.contradicts = stringsOf(value);
        break;
      case 'resolves':
        page.resolves = stringsOf(value);
        break;
      default:
        break;
    }
  }
  return page;
}

function entryName(row) {
  if (!row.startsWith('- [[')) return null;
  const rest = row.slice(4);
  const close = rest.indexOf(']]');
  if (close < 0) return null;
  const name = rest.slice(0, close);
  const tail = rest.slice(close + 2);
  if (!tail.startsWith(' - ') || tail.length <= 3) return null;
  return validPage(name) ? name : null;
}

function linksIn(text) {
  const out = [];
  let from = 0;
  for (;;) {
    const found = text.indexOf('[[', from);
    if (found < 0) return out;
    const start = found + 2;
    const bracket = text.indexOf(']', start);
    if (bracket > start && text[bracket + 1] === ']') {
      out.push(text.slice(start, bracket));
      from = bracket + 2;
    } else {
      from = found + 1;
    }
  }
}

function logTitle(row) {
  if (!row.startsWith('## [')) return null;
  const rest = row.slice(4);
  if (rest.length < LOG_DATE_LENGTH || !LOG_DATE.test(rest.slice(0, LOG_DATE_LENGTH))) return null;
  const after = rest.slice(LOG_DATE_LENGTH);
  if (!after.startsWith('] ')) return null;
  const entry = after.slice(2);
  for (const kind of KINDS) {
    if (!entry.startsWith(kind) || !entry.slice(kind.length).startsWith(' | ')) continue;
    const title = entry.slice(kind.length + 3);
    if (title !== '') return title;
  }
  return null;
}

async function registryOf(host, root, problems) {
  const file = `${root}/sources/index.jsonl`;
  if ((await statOrNull(host, file)) === null) return [];
  const out = [];
  for (const line of (await readOr(host, file, '')).split('\n').filter((l) => l !== '')) {
    let value;
    try {
      value = JSON.parse(line);
    } catch {
      problems.push('sources/index.jsonl is not JSON lines');
      return [];
    }
    out.push({ id: textOf(value?.id), url: textOf(value?.url), at: textOf(value?.at) });
  }
  return out;
}

async function pagesOf(host, dir, problems) {
  const files = (await host.fs.list(dir)).filter((name) => name.endsWith('.md') && name !== 'index.md' && name !== 'log.md').sort();
  const pages = [];
  for (const file of files) {
    const page = parsePage(await readOr(host, `${dir}/${file}`, ''));
    if (page === null) problems.push(`wiki/${file} has no frontmatter`);
    else pages.push({ name: file.slice(0, -3), page });
  }
  return pages;
}

function checkIndexAndLinks(pages, indexed, indexText, problems) {
  const names = new Set(pages.map((p) => p.name));
  const indexNames = new Set(indexed);
  for (const { name } of pages) {
    if (!indexNames.has(name)) problems.push(`page ${name} has no entry in wiki/index.md`);
  }
  for (const name of indexed) {
    if (!names.has(name)) problems.push(`wiki/index.md lists ${name}, which has no page`);
  }
  for (const target of linksIn(indexText)) {
    if (!names.has(target)) problems.push(`wiki/index.md links to ${target}, which has no page`);
  }
  const linked = new Set();
  for (const { name, page } of pages) {
    for (const target of linksIn(page.body)) {
      if (!names.has(target)) problems.push(`page ${name} links to ${target}, which has no page`);
      else if (target !== name) linked.add(target);
    }
  }
  for (const { name } of pages) {
    if (!linked.has(name)) problems.push(`page ${name} is an orphan: no other page links to it`);
  }
  return names;
}

async function checkSourcesAndClaims(host, root, pages, logTitles, problems) {
  for (const { name, page } of pages) {
    if (!logTitles.has(page.title)) problems.push(`page ${name} has no line in wiki/log.md`);
    for (const id of page.sources) {
      if ((await sourceText(host, root, id)) === null) problems.push(`page ${name} cites source ${id}, which is not in sources/`);
    }
    for (const [quote, source] of page.claims) {
      if (!page.sources.includes(source)) problems.push(`page ${name} has a claim from ${source}, which it does not list`);
      const text = await sourceText(host, root, source);
      if (text !== null && !text.includes(quote)) problems.push(`page ${name}: a quote is no longer in source ${source}`);
    }
  }
}

function checkRegistry(pages, registry, problems) {
  for (const { name, page } of pages) {
    for (const id of page.sources) {
      const entry = registry.find((e) => e.id === id);
      if (!entry) continue;
      const latest = registry.filter((e) => e.url === entry.url).at(-1);
      if (latest && latest.id !== id && newer(latest.at, page.updated)) {
        problems.push(`page ${name} cites source ${id}, but ${entry.url} was archived again as ${latest.id} at ${latest.at}`);
      }
    }
  }
}

function checkContradictions(pages, names, problems) {
  for (const { name, page } of pages) {
    for (const other of page.contradicts) {
      if (!names.has(other)) {
        problems.push(`page ${name} contradicts ${other}, which has no page`);
        continue;
      }
      const resolved = pages.some(
        ({ name: later, page: body }) => later !== name && (body.resolves.includes(name) || body.resolves.includes(other)) && newer(body.updated, page.updated),
      );
      if (!resolved) problems.push(`open contradiction: ${name} contradicts ${other}`);
    }
    for (const other of page.resolves) {
      if (!names.has(other)) problems.push(`page ${name} resolves ${other}, which has no page`);
    }
  }
}

export async function lintWiki(host, root) {
  const dir = `${root}/wiki`;
  if ((await statOrNull(host, dir)) === null) return { skipped: true, problems: [], pages: 0, claims: 0 };
  const problems = [];
  const indexPath = `${dir}/index.md`;
  const logPath = `${dir}/log.md`;
  if ((await statOrNull(host, indexPath)) === null) problems.push('wiki/index.md is missing');
  if ((await statOrNull(host, logPath)) === null) problems.push('wiki/log.md is missing');
  const indexText = await readOr(host, indexPath, '');
  const logText = await readOr(host, logPath, '');
  const indexed = [...new Set(indexText.split('\n').map(entryName).filter((name) => name !== null))];
  const pages = await pagesOf(host, dir, problems);
  const names = checkIndexAndLinks(pages, indexed, indexText, problems);
  const logTitles = new Set(logText.split('\n').map(logTitle).filter((title) => title !== null));
  await checkSourcesAndClaims(host, root, pages, logTitles, problems);
  const registry = await registryOf(host, root, problems);
  checkRegistry(pages, registry, problems);
  checkContradictions(pages, names, problems);
  const claims = pages.reduce((sum, { page }) => sum + page.claims.length, 0);
  return { skipped: false, problems, pages: pages.length, claims };
}

export async function wikiCheck(host, root) {
  const lint = await lintWiki(host, root);
  if (lint.skipped) return { ok: true, detail: 'no wiki' };
  if (lint.problems.length > 0) {
    return { ok: false, detail: `${lint.problems.length} problems: ${lint.problems.slice(0, FINDING_CAP).join(' | ')}` };
  }
  return { ok: true, detail: `${lint.pages} pages, ${lint.claims} claims, lint clean` };
}
