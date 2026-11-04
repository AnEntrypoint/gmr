const ROW_DIR = 'work/rows';
const KINDS = { prd: 'work item', unknown: 'open question' };
const STATUSES = ['open', 'resolved', 'all'];
const FIELDS = ['id', 'kind', 'title', 'body', 'status', 'createdAt', 'witnessEvent', 'witnessCommit', 'resolvedAt'];
const CHECK_TIMEOUT_MS = 15000;
const SHA = /^[0-9a-f]{7,40}$/;
const str = { type: 'string' };
const num = { type: 'number' };
const obj = (properties, required = []) => ({ type: 'object', properties, required });

const fail = (error) => ({ ok: false, error });
const textOf = (row, key) => (typeof row[key] === 'string' ? row[key] : '');
const compare = (a, b) => (a === b ? 0 : a < b ? -1 : 1);
const isoNow = () => new Date().toISOString();
const slashes = (path) => path.replace(/\\/g, '/');
const dirOf = (path) => path.slice(0, path.lastIndexOf('/'));
const validId = (kind, id) => typeof id === 'string' && id.startsWith(`${kind}-`) && /^[a-z0-9]+$/.test(id.slice(kind.length + 1));
const rowPath = (id) => `${ROW_DIR}/${id}.yml`;
const newId = (kind) => `${kind}-${Date.now().toString(36)}${Math.floor(Math.random() * 0x10000).toString(16).padStart(4, '0')}`;

function parseScalar(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

function parseRow(source) {
  const row = {};
  for (const line of source.split('\n')) {
    const at = line.indexOf(': ');
    if (at < 1) continue;
    const key = line.slice(0, at);
    if (!/^[A-Za-z]+$/.test(key)) continue;
    row[key] = parseScalar(line.slice(at + 2));
  }
  return row;
}

function renderRow(row) {
  return FIELDS.filter((field) => row[field] !== undefined)
    .map((field) => `${field}: ${JSON.stringify(row[field])}\n`)
    .join('');
}

function sortByCreated(rows) {
  return rows.sort((a, b) => compare(textOf(a, 'createdAt'), textOf(b, 'createdAt')) || compare(textOf(a, 'id'), textOf(b, 'id')));
}

async function confined(host, root, rel) {
  const parts = rel.split('/');
  let probe = root;
  for (let n = parts.length; n > 0; n -= 1) {
    const candidate = `${root}/${parts.slice(0, n).join('/')}`;
    if (await host.fs.stat(candidate)) {
      probe = candidate;
      break;
    }
  }
  const realRoot = slashes(await host.fs.realpath(root));
  let real;
  try {
    real = slashes(await host.fs.realpath(probe));
  } catch {
    throw new Error('path leads through a link that does not resolve');
  }
  if (real !== realRoot && !real.startsWith(`${realRoot}/`)) {
    throw new Error('path leads through a link outside the workspace');
  }
  return `${root}/${rel}`;
}

async function readRows(host, root) {
  const dir = await confined(host, root, ROW_DIR);
  if (!(await host.fs.stat(dir))) return [];
  const rows = [];
  for (const name of await host.fs.list(dir)) {
    if (name.endsWith('.yml')) rows.push(parseRow(await host.fs.read(`${dir}/${name}`)));
  }
  return rows;
}

async function readRow(host, root, id) {
  const path = await confined(host, root, rowPath(id));
  if (!(await host.fs.stat(path))) return null;
  return parseRow(await host.fs.read(path));
}

async function writeRow(host, root, row) {
  const path = await confined(host, root, rowPath(row.id));
  await host.fs.mkdirp(dirOf(path));
  const temp = `${path}.${Math.random().toString(36).slice(2, 10)}.tmp`;
  await host.fs.write(temp, renderRow(row));
  await host.fs.rename(temp, path);
}

async function eventInLog(host, event) {
  return (await host.log.read({})).some((entry) => entry.seq === event);
}

async function commitExists(host, root, sha) {
  try {
    const r = await host.proc.run('git', ['cat-file', '-e', `${sha}^{commit}`], { cwd: root, timeoutMs: CHECK_TIMEOUT_MS });
    return r.code === 0 && !r.timedOut;
  } catch {
    return false;
  }
}

async function addRow(host, kind, args) {
  const title = args.title;
  if (typeof title !== 'string' || title.trim() === '') return fail('title must be a non-empty string');
  const body = args.body === undefined ? '' : args.body;
  if (typeof body !== 'string') return fail('body must be a string');
  const row = { id: newId(kind), kind, title: title.trim(), body, status: 'open', createdAt: isoNow() };
  await writeRow(host, host.workspace, row);
  return { ok: true, output: row.id };
}

async function listRows(host, kind, args) {
  const status = args.status === undefined ? 'open' : args.status;
  if (!STATUSES.includes(status)) return fail(`status must be one of ${STATUSES.join(', ')}`);
  const rows = sortByCreated(
    (await readRows(host, host.workspace)).filter((row) => textOf(row, 'kind') === kind && (status === 'all' || textOf(row, 'status') === status)),
  );
  const output = rows.length
    ? rows.map((row) => `${textOf(row, 'id')} ${textOf(row, 'status')} ${textOf(row, 'title')}`).join('\n')
    : '(none)';
  return { ok: true, output };
}

async function resolveRow(host, kind, args) {
  const id = typeof args.id === 'string' ? args.id : '';
  if (!validId(kind, id)) return fail(`id must name a ${kind} row, with an id that starts ${kind}-`);
  const event = args.event;
  if (!Number.isSafeInteger(event) || event < 1) return fail('event must be a log seq number');
  if (!(await eventInLog(host, event))) return fail(`event ${event} is not in this run's log`);
  const commit = args.commit;
  if (typeof commit !== 'string' || !SHA.test(commit)) return fail('commit must be 7 to 40 lowercase hex digits');
  if (!(await commitExists(host, host.workspace, commit))) return fail(`no commit ${commit} in this repository`);
  const row = await readRow(host, host.workspace, id);
  if (row === null) return fail(`no row ${id}`);
  if (textOf(row, 'status') === 'resolved') return fail(`row ${id} is already resolved`);
  Object.assign(row, { status: 'resolved', witnessEvent: event, witnessCommit: commit, resolvedAt: isoNow() });
  await writeRow(host, host.workspace, row);
  return { ok: true, output: `${id} resolved by event ${event} at commit ${commit}` };
}

async function deleteRow(host, args) {
  const id = typeof args.id === 'string' ? args.id : '';
  if (!validId('prd', id)) return fail('id must name a prd row, with an id that starts prd-');
  const row = await readRow(host, host.workspace, id);
  if (row === null) return fail(`no row ${id}`);
  if (textOf(row, 'status') === 'resolved') return fail(`row ${id} is resolved; a resolved PRD is history and stays`);
  await host.fs.remove(await confined(host, host.workspace, rowPath(id)));
  return { ok: true, output: `${id} deleted` };
}

function rowLock(kind, args) {
  const id = args?.id;
  return validId(kind, id) ? rowPath(id) : ROW_DIR;
}

function rowVerbs(kind) {
  const label = KINDS[kind];
  return [
    {
      name: `${kind}_add`,
      description: `Add a ${label} row, one file under ${ROW_DIR}. Returns its id.`,
      schema: obj({ title: str, body: str }, ['title']),
      lock: () => ({ mode: 'write', paths: [ROW_DIR] }),
      call: (args, host) => addRow(host, kind, args),
    },
    {
      name: `${kind}_list`,
      description: `List ${label} rows, one line each: id, status, title. status is open (default), resolved or all.`,
      schema: obj({ status: str }),
      lock: () => ({ mode: 'read' }),
      call: (args, host) => listRows(host, kind, args),
    },
    {
      name: `${kind}_resolve`,
      description: `Resolve a ${label} row. The witness is a seq of this run's log and a commit that exists in this repository.`,
      schema: obj({ id: str, event: num, commit: str }, ['id', 'event', 'commit']),
      lock: (args) => ({ mode: 'write', paths: [rowLock(kind, args)] }),
      call: (args, host) => resolveRow(host, kind, args),
    },
  ];
}

export const verbs = [
  ...rowVerbs('prd'),
  ...rowVerbs('unknown'),
  {
    name: 'prd_delete',
    description: 'Delete a work item (PRD) row that is not resolved: the work is dropped and its file is removed, and git keeps it. An open question is resolved, not deleted.',
    schema: obj({ id: str }, ['id']),
    lock: (args) => ({ mode: 'write', paths: [rowLock('prd', args)] }),
    call: (args, host) => deleteRow(host, args),
  },
];

export async function open_rows(root, host) {
  return sortByCreated((await readRows(host, root)).filter((row) => textOf(row, 'status') === 'open'));
}

export async function row_summary(root, kind, limit, host) {
  const rows = sortByCreated((await readRows(host, root)).filter((row) => textOf(row, 'kind') === kind));
  const open = rows.filter((row) => textOf(row, 'status') === 'open');
  const window = open.slice(0, limit).map((row) => ({ id: textOf(row, 'id'), title: textOf(row, 'title') }));
  return { total: rows.length, open: open.length, window, more: open.length - window.length };
}
