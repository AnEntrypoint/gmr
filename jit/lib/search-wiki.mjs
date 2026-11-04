import { joinPath } from './search-paths.mjs';

const PAGE = /^[a-z0-9][a-z0-9-]{0,63}$/;
const SOURCE = /^[0-9a-f]{12}$/;
const ASCII = /^[\x00-\x7f]*$/;
const KINDS = ['ingest', 'answer', 'lint'];
const SOURCES_ERROR = 'sources must be a non-empty list of source ids';
const PAGE_LIST_ERROR = 'contradicts and resolves must be lists of page names';

export function validPage(name) {
  return typeof name === 'string' && PAGE.test(name);
}

function validSource(id) {
  return typeof id === 'string' && SOURCE.test(id);
}

function oneLine(text) {
  return typeof text === 'string' && text.trim() !== '' && !/[\r\n]/.test(text) && ASCII.test(text);
}

export function asciiText(text) {
  let out = '';
  for (let k = 0; k < text.length; k += 1) {
    const code = text.charCodeAt(k);
    out += code < 128 ? text.charAt(k) : `\\u${code.toString(16).padStart(4, '0')}`;
  }
  return out;
}

function sourcePath(root, id) {
  return joinPath(joinPath(root, 'sources'), `${id}.txt`);
}

async function sourceExists(host, root, id) {
  if (!validSource(id)) return false;
  const stat = await host.fs.stat(sourcePath(root, id));
  return stat !== null && stat.type === 'file';
}

async function sourceText(host, root, id, cache) {
  if (!cache.has(id)) {
    let text = null;
    if (validSource(id)) {
      try {
        text = await host.fs.read(sourcePath(root, id));
      } catch {
        text = null;
      }
    }
    cache.set(id, text);
  }
  return cache.get(id);
}

async function readOr(host, path, fallback) {
  try {
    return await host.fs.read(path);
  } catch {
    return fallback;
  }
}

function pageList(value) {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) return null;
  return value.every(validPage) ? value.slice() : null;
}

export async function wikiWrite(host, root, args) {
  const refuse = (reason) => ({ ok: false, error: `refused: ${reason}` });
  const page = args.page;
  if (!validPage(page)) return refuse('page must be lowercase letters, digits and hyphens, at most 64 characters');
  const title = args.title === undefined || args.title === null ? page : args.title;
  if (!oneLine(title)) return refuse('title must be one line of ASCII');
  const body = args.body;
  if (typeof body !== 'string' || !ASCII.test(body)) return refuse('body must be ASCII text; write non-ASCII as \\uXXXX');
  const summary = args.summary;
  if (!oneLine(summary)) return refuse('summary must be one line of ASCII');
  const kind = args.kind === undefined || args.kind === null ? 'ingest' : args.kind;
  if (!KINDS.includes(kind)) return refuse(`kind must be one of ${KINDS.join(', ')}`);
  const sources = args.sources === undefined || args.sources === null ? [] : args.sources;
  if (!Array.isArray(sources) || sources.length === 0 || !sources.every(validSource)) return refuse(SOURCES_ERROR);
  for (const id of sources) {
    if (!(await sourceExists(host, root, id))) return refuse(`source ${id} is not in sources/`);
  }
  const claimList = args.claims === undefined || args.claims === null ? [] : args.claims;
  if (!Array.isArray(claimList)) return refuse('claims must be a list of {quote, source}');
  const cache = new Map();
  const claims = [];
  for (let i = 0; i < claimList.length; i += 1) {
    const claim = claimList[i];
    const quote = typeof claim?.quote === 'string' && claim.quote !== '' ? claim.quote : null;
    const source = typeof claim?.source === 'string' ? claim.source : null;
    if (quote === null || source === null || !sources.includes(source)) {
      return refuse(`claim ${i + 1} needs a quote and a source that is listed in sources`);
    }
    const text = await sourceText(host, root, source, cache);
    const quoteAscii = asciiText(quote);
    if (text === null || !text.includes(quoteAscii)) return refuse(`claim ${i + 1}: the quote is not in source ${source}`);
    claims.push({ quote: quoteAscii, source });
  }
  const contradicts = pageList(args.contradicts);
  if (contradicts === null) return refuse(PAGE_LIST_ERROR);
  const resolves = pageList(args.resolves);
  if (resolves === null) return refuse(PAGE_LIST_ERROR);

  const dir = joinPath(root, 'wiki');
  await host.fs.mkdirp(dir);
  const now = new Date().toISOString();
  const front = [
    '---',
    `title: ${JSON.stringify(title)}`,
    `updated: ${JSON.stringify(now)}`,
    `sources: ${JSON.stringify(sources)}`,
    `claims: ${JSON.stringify(claims)}`,
    `contradicts: ${JSON.stringify(contradicts)}`,
    `resolves: ${JSON.stringify(resolves)}`,
    '---',
  ].join('\n');
  const bodyNl = body.endsWith('\n') ? body : `${body}\n`;
  await host.fs.write(joinPath(dir, `${page}.md`), `${front}\n${bodyNl}`);

  const indexPath = joinPath(dir, 'index.md');
  const rows = (await readOr(host, indexPath, '# Index\n\n')).split('\n');
  if (rows[rows.length - 1] === '') rows.pop();
  const entry = `- [[${page}]] - ${summary}`;
  const prefix = `- [[${page}]] - `;
  const at = rows.findIndex((row) => row.startsWith(prefix));
  if (at >= 0) rows[at] = entry;
  else rows.push(entry);
  await host.fs.write(indexPath, `${rows.join('\n')}\n`);

  const logPath = joinPath(dir, 'log.md');
  const logText = (await host.fs.stat(logPath)) === null ? '# Log\n\n' : await host.fs.read(logPath);
  await host.fs.write(logPath, `${logText}## [${now.slice(0, 10)}] ${kind} | ${title}\n`);

  await host.log.append('wiki', { page, writeKind: kind, sources: sources.length, claims: claims.length });
  const result = JSON.stringify({
    claims: claims.length,
    ok: true,
    output: `wrote wiki/${page}.md, its index entry and its log line`,
    page: `wiki/${page}.md`,
  });
  return { ok: true, output: result };
}
