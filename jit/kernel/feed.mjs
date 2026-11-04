export const NEXT_CONTEXT_EVENTS_CAP = 20000;
export const H1 = 14;
export const H2 = 12;
const LINE_CAP = 400;
const ARG_CAP = 200;
const ARG_HEAD = 80;
const FAIL_LINES = 60;
const ARRAY_OVERHEAD = 2;
const OUTPUT_PAGE = 20000;
const MAX_WAIT_MS = 300000;
const DEFAULT_MAX_STEPS = 100000;
const RECENT_RESULTS = 50;
const ROW_DIR = 'work/rows';
const SKIP = new Set(['t', 'mono', 'inFlight', 'startMono', 'durMs', 'actionSeq', 'step', 'seq', 'kind']);
const KEEP_WHOLE = new Set(['witness', 'error', 'reason', 'gate', 'gaps', 'paths']);
const KEEP_ZONE2 = new Set(['end', 'commit', 'refusal', 'error', 'restart', 'job-killed', 'review', 'review-packet', 'review-refused', 'lint', 'wiki', 'recovery', 'parse_error']);
const ZONE2_DROPPED_TOOLS = new Set(['list_dir', 'query_log', 'prd_list', 'unknown_list', 'callers', 'impact', 'git_log', 'git_diff', 'git_show', 'search', 'grep', 'wiki_search', 'review_join']);
const ERROR_LINE = /\b(error|errors|fail|failed|failure|fatal|warn|warning|exception|traceback|denied|not ok)\b/i;
const REPLY_TEXT = '## Reply\n\nOne JSON object {"tool": "...", "args": {...}}, or a JSON array of them when the actions are independent. Nothing else.';

export const MCP_RULES = `# Rules

1. Paths may point inside or outside the workspace. An outside path should be absolute.
2. Call next_context at the start of every step. Each call is one step; stepsLeft counts the steps the run has left.
3. Verify before you finish: run what you built and read its output.
4. finish needs a witness: one string that quotes the evidence you observed. Calls sent with finish may be refused.
5. Events older than the window are not in next_context. Query them with query_log. Do not guess.
6. Never print, store or request secrets, environment variables or API keys.
7. If the task cannot be done, finish with a witness that states why, with the evidence.
`;

export const DEFAULT_WITNESS = 'Finish only with a witness: one string that states the evidence (command output, file content or query result) showing the task is done.';

const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const orNull = (v) => (v === undefined ? null : v);
const refuse = (reason) => ({ ok: false, refused: true, error: `refused: ${reason}` });
const joinPath = (dir, ...parts) => [dir.replace(/[\\/]+$/, ''), ...parts].join('/');
const baseName = (path) => path.replace(/[\\/]+$/, '').split(/[\\/]/).pop();

function utf8Bytes(text) {
  let bytes = 0;
  for (let i = 0; i < text.length; i += 1) {
    const c = text.charCodeAt(i);
    if (c < 0x80) bytes += 1;
    else if (c < 0x800) bytes += 2;
    else if (c >= 0xd800 && c <= 0xdbff && i + 1 < text.length) {
      bytes += 4;
      i += 1;
    } else bytes += 3;
  }
  return bytes;
}

function headChars(text, cap) {
  if (text.length <= cap) return text;
  let count = 0;
  let end = 0;
  for (const ch of text) {
    if (count === cap) break;
    count += 1;
    end += ch.length;
  }
  return text.slice(0, end);
}

export function clip(text, cap) {
  if (text.length <= cap) return text;
  return `${headChars(text, cap)} ...[${text.length} chars]`;
}

export function argsOf(args) {
  const out = {};
  if (!isObject(args)) return out;
  for (const [key, value] of Object.entries(args)) {
    if (typeof value === 'string' && value.length > ARG_CAP) out[key] = { chars: value.length, head: clip(value, ARG_HEAD) };
    else if (Array.isArray(value) && value.length > 10) out[key] = { items: value.length };
    else out[key] = value;
  }
  return out;
}

export function outputDigest(text, failed) {
  if (text === '') return { lines: 0, bytes: 0 };
  const lines = text.split('\n');
  const n = lines.length;
  const digest = {
    lines: n,
    bytes: utf8Bytes(text),
    head: lines.slice(0, 2).map((l) => clip(l, LINE_CAP)),
    tail: lines.slice(Math.max(0, n - 2)).map((l) => clip(l, LINE_CAP)),
  };
  const errors = lines.filter((l) => ERROR_LINE.test(l)).map((l) => clip(l, LINE_CAP));
  if (errors.length > 0) digest.errors = errors;
  if (failed) {
    digest.all = lines.slice(0, FAIL_LINES).map((l) => clip(l, LINE_CAP));
    if (n > FAIL_LINES) digest.more = n - FAIL_LINES;
  }
  return digest;
}

function outputBytes(event) {
  if (Number.isInteger(event.outputBytes) && event.outputBytes >= 0) return event.outputBytes;
  return typeof event.output === 'string' ? utf8Bytes(event.output) : 0;
}

const fullOf = (event) => ({ seq: orNull(event.seq), bytes: outputBytes(event) });
const actionArgs = (action) => (action && isObject(action.args) ? action.args : {});
const argOf = (action, key) => orNull(actionArgs(action)[key]);
const bytesOf = (action, key) => (typeof actionArgs(action)[key] === 'string' ? utf8Bytes(actionArgs(action)[key]) : null);

function resultBase(event, action) {
  const r = { seq: orNull(event.seq), tool: orNull(event.tool), ok: orNull(event.ok), args: argsOf(action?.args) };
  if ('exitCode' in event) r.exit = event.exitCode;
  if ('error' in event) r.error = event.error;
  return r;
}

export function resultZone1(event, action) {
  const r = resultBase(event, action);
  if ('outputDigest' in event) r.out = event.outputDigest;
  else if (typeof event.output === 'string') {
    const failed = event.ok === false || (typeof event.exitCode === 'number' && event.exitCode !== 0);
    r.out = outputDigest(event.output, failed);
  }
  r.full = fullOf(event);
  return r;
}

export function resultZone2(event, action) {
  return { ...resultBase(event, action), full: fullOf(event) };
}

function fieldsOf(event, whole) {
  const r = { seq: orNull(event.seq), kind: orNull(event.kind) };
  for (const [key, value] of Object.entries(event)) {
    if (SKIP.has(key)) continue;
    const keep = whole && KEEP_WHOLE.has(key);
    r[key] = typeof value === 'string' && !keep ? clip(value, ARG_CAP) : value;
  }
  return r;
}

export const shortKind = (event) => fieldsOf(event, false);
export const keptKind = (event) => (KEEP_ZONE2.has(event.kind) ? fieldsOf(event, true) : null);

function toolRecord(tool, zone, event, action) {
  const standard = () => (zone === 1 ? resultZone1(event, action) : resultZone2(event, action));
  if (zone === 2 && ZONE2_DROPPED_TOOLS.has(tool)) return null;
  switch (tool) {
    case 'write_file':
      return { ...standard(), args: { path: argOf(action, 'path'), bytes: bytesOf(action, 'content') } };
    case 'edit_file':
      return { ...standard(), args: { path: argOf(action, 'path'), oldBytes: bytesOf(action, 'old'), newBytes: bytesOf(action, 'new') } };
    case 'finish':
      return { seq: orNull(event.seq), tool: 'finish', ok: orNull(event.ok), witness: argOf(action, 'witness'), gaps: argOf(action, 'gaps'), full: fullOf(event) };
    case 'fetch':
      return { ...standard(), source: orNull(event.source), sourceNew: orNull(event.sourceNew) };
    case 'wiki_write':
      if (zone === 2) {
        const claims = actionArgs(action).claims;
        return { seq: orNull(event.seq), tool: 'wiki_write', ok: orNull(event.ok), page: argOf(action, 'page'), claims: Array.isArray(claims) ? claims.length : 0, full: fullOf(event) };
      }
      return standard();
    default:
      return standard();
  }
}

function recordOf(event, zone, actionAt, answered) {
  const kind = event.kind ?? '';
  if (kind === 'action') {
    if (answered.has(event.seq) || zone !== 1) return null;
    return { seq: event.seq, kind: 'action', tool: orNull(event.tool), args: argsOf(event.args), unanswered: true };
  }
  if (kind === 'result' || kind === 'code-call') {
    const action = kind === 'code-call' ? event : actionAt(event.actionSeq);
    return toolRecord(event.tool ?? '', zone, event, action);
  }
  if (kind === 'code-load') return zone === 1 ? shortKind(event) : null;
  return zone === 1 ? shortKind(event) : keptKind(event);
}

function freshRecordOf(event, actionAt) {
  if (event.kind === 'action') return { seq: orNull(event.seq), kind: 'action', tool: orNull(event.tool), args: argsOf(event.args) };
  return recordOf(event, 1, actionAt, new Set()) ?? shortKind(event);
}

const costOf = (v) => JSON.stringify(v).length + 1;

function planFresh(fresh, lastSeq, room) {
  const tailOf = (k) => (k < fresh.length ? { kind: 'range', seqs: [fresh[k].index + 1, lastSeq], count: lastSeq - fresh[k].index } : null);
  const tailCost = (k) => (k < fresh.length ? costOf(tailOf(k)) : 0);
  const recordTotal = fresh.reduce((sum, f) => sum + f.recordCost, 0);
  if (recordTotal <= room) {
    let spare = room - recordTotal;
    let full = 0;
    while (full < fresh.length && Math.max(0, fresh[full].cost - fresh[full].recordCost) <= spare) {
      spare -= Math.max(0, fresh[full].cost - fresh[full].recordCost);
      full += 1;
    }
    const items = fresh.map((f, i) => (i < full ? f.event : f.record));
    return { full, items, used: room - spare };
  }
  let used = 0;
  let full = 0;
  while (full < fresh.length && used + fresh[full].cost + tailCost(full + 1) <= room) {
    used += fresh[full].cost;
    full += 1;
  }
  const items = fresh.slice(0, full).map((f) => f.event);
  const tail = tailOf(full);
  if (tail) {
    used += costOf(tail);
    items.push(tail);
  }
  return { full, items, used };
}

export function feedOf(window, zone2Start, zone1Start, newStart, actionAt) {
  const n = window.length > 0 ? Math.max(zone2Start, window[window.length - 1].seq) : zone2Start;
  const start = Math.min(newStart, n);
  const answered = new Set(window.map((e) => e.actionSeq).filter((s) => Number.isInteger(s)));
  const seen = [];
  const fresh = [];
  for (const event of window) {
    const index = event.seq - 1;
    if (index < start) {
      const record = recordOf(event, index >= zone1Start ? 1 : 2, actionAt, answered);
      if (record) seen.push({ index, record, cost: costOf(record) });
    } else {
      const record = freshRecordOf(event, actionAt);
      fresh.push({ index, event, cost: costOf(event), record, recordCost: costOf(record) });
    }
  }
  const room = NEXT_CONTEXT_EVENTS_CAP - ARRAY_OVERHEAD;
  const plan = planFresh(fresh, n, room);
  let spare = Math.max(0, room - plan.used);
  let keptFrom = seen.length;
  while (keptFrom > 0 && seen[keptFrom - 1].cost <= spare) {
    spare -= seen[keptFrom - 1].cost;
    keptFrom -= 1;
  }
  return {
    older: keptFrom < seen.length ? seen[keptFrom].index : start,
    records: seen.length - keptFrom + plan.items.length,
    fullEvents: plan.full,
    compactFrom: plan.full < fresh.length ? fresh[plan.full].index + 1 : null,
    events: [...seen.slice(keptFrom).map((s) => s.record), ...plan.items],
  };
}

export function renderPrefix(rules, task, witness, tools) {
  return [rules.trim(), `## Task\n\n${task}`, `## Witness requirement\n\n${witness}`, `## Tools\n\n${tools}`, REPLY_TEXT].join('\n\n');
}

export function queryHint(shown, total) {
  const reach = 'query_log reads any event (after: seq - 1 returns it first)';
  return total > shown ? `${reach}. This step's new events are seqs ${shown + 1} to ${total}.` : `${reach}.`;
}

export function waitMsOf(args) {
  const ms = args?.waitMs;
  return typeof ms === 'number' && Number.isFinite(ms) && ms > 0 ? Math.floor(Math.min(ms, MAX_WAIT_MS)) : 0;
}

export function digest(reply) {
  const out = {};
  for (const key of ['ok', 'error', 'prefixSha', 'total']) if (reply && key in reply) out[key] = reply[key];
  return out;
}

function recoveryNote(recovery) {
  const lastSeq = orNull(recovery.lastSeq);
  const open = recovery.open ?? [];
  const list = (Array.isArray(open) ? open : []).map((x) => `seq ${JSON.stringify(orNull(x.seq))} ${typeof x.tool === 'string' ? x.tool : ''} ${JSON.stringify(orNull(x.args))}`);
  return {
    lastSeq,
    open,
    note: `This run resumed after a restart, at last seq ${JSON.stringify(lastSeq)}. These actions started and have no end event: ${list.join('; ')}. Before you repeat any of them, check the outside effects: git log and git status, the remote, and the row files under work/rows. Do not repeat a push, a commit, a fetch that posted, or a row resolve unless those checks show that it did not happen.`,
  };
}

function pick(event, keys) {
  const out = {};
  for (const key of keys) if (key in event) out[key] = event[key];
  return out;
}

async function readOr(host, path, fallback) {
  try {
    return await host.fs.read(path);
  } catch {
    return fallback;
  }
}

async function endedJob(host, event) {
  const job = typeof event.job === 'string' ? event.job : '';
  const jobs = joinPath(host.runDir, 'jobs');
  let text = '';
  if ('body' in event) {
    try {
      const state = JSON.parse(await host.fs.read(joinPath(jobs, `${job}.json`)));
      const value = 'value' in state ? state.value : state.error;
      if (value !== undefined) text = typeof value === 'string' ? value : JSON.stringify(value);
    } catch {
      text = '';
    }
  } else {
    text = await readOr(host, joinPath(jobs, `${job}.out`), '');
  }
  return {
    seq: orNull(event.seq),
    job,
    state: orNull(event.state),
    exitCode: orNull(event.exitCode),
    signal: orNull(event.signal),
    output: headChars(text, OUTPUT_PAGE),
    next: text.length > OUTPUT_PAGE ? OUTPUT_PAGE : null,
  };
}

const isContextResult = (e) => e.kind === 'result' && e.tool === 'next_context' && e.ok === true;

function lastWhere(list, test) {
  for (let i = list.length - 1; i >= 0; i -= 1) if (test(list[i])) return list[i];
  return undefined;
}

async function lastContextResult(host) {
  const recent = await host.log.read({ kind: 'result', last: RECENT_RESULTS });
  const hit = lastWhere(recent, isContextResult);
  if (hit || recent.length < RECENT_RESULTS) return hit ?? null;
  return lastWhere(await host.log.read({ kind: 'result' }), isContextResult) ?? null;
}

async function stepsTaken(host) {
  return (await host.log.read({ kind: 'result' })).filter(isContextResult).length;
}

async function lastSeq(host) {
  const [last] = await host.log.read({ last: 1 });
  return last ? last.seq : 0;
}

async function taskMeta(host) {
  try {
    return JSON.parse(await host.fs.read(joinPath(host.runDir, 'task.json')));
  } catch {
    return {};
  }
}

async function taskOf(host, meta) {
  const [latest] = await host.log.read({ kind: 'task', last: 1 });
  if (latest && typeof latest.text === 'string') return latest.text;
  return typeof meta.task === 'string' ? meta.task : '';
}

async function prefixShaOf(host, rules, task, args) {
  const tools = typeof args.toolsText === 'string' ? args.toolsText : '';
  return host.hash.sha256(renderPrefix(rules, task, DEFAULT_WITNESS, tools));
}

function maxStepsOf(args, meta) {
  if (Number.isInteger(args.maxSteps)) return args.maxSteps;
  return Number.isInteger(meta.maxSteps) ? meta.maxSteps : DEFAULT_MAX_STEPS;
}

async function actionsBefore(host, window, base) {
  const wanted = window.filter((e) => e.kind === 'result' && Number.isInteger(e.actionSeq) && e.actionSeq <= base).map((e) => e.actionSeq);
  if (wanted.length === 0) return new Map();
  const from = Math.min(...wanted);
  const actions = await host.log.read({ kind: 'action', after: from - 1 });
  return new Map(actions.filter((a) => a.seq <= base).map((a) => [a.seq, a]));
}

function pendingOf(args) {
  if (!Array.isArray(args.pending)) return [];
  return args.pending.flatMap((r) => {
    if (typeof r !== 'string') return [r];
    try {
      return [JSON.parse(r)];
    } catch {
      return [];
    }
  });
}

async function childContext(args, host) {
  const runId = baseName(host.runDir);
  if (args.run !== undefined && args.run !== runId) return refuse(`this connection's run is ${runId}, not ${typeof args.run === 'string' ? args.run : JSON.stringify(args.run)}`);
  const meta = await taskMeta(host);
  const [rules, task, total] = await Promise.all([readOr(host, joinPath(host.runDir, 'rules.md'), MCP_RULES), taskOf(host, meta), lastSeq(host)]);
  const maxSteps = maxStepsOf(args, meta);
  const steps = Number.isInteger(args.step) ? args.step : await stepsTaken(host);
  return {
    ok: true,
    role: 'child',
    run: runId,
    note: `child of run ${runId}: the session that opened the run ends it, so a child does not send finish`,
    stepsLeft: Math.max(0, maxSteps - steps),
    maxSteps,
    rules,
    task: task === '' ? null : task,
    prefixSha: await prefixShaOf(host, rules, task, args),
    total,
  };
}

export async function nextContext(args = {}, host) {
  if (args.child === true) return childContext(args, host);
  const meta = await taskMeta(host);
  const maxSteps = maxStepsOf(args, meta);
  const step = Number.isInteger(args.step) ? args.step : (await stepsTaken(host)) + 1;
  if (step > maxSteps) {
    const text = `step limit ${maxSteps} reached; a restart of the server opens a new run`;
    await endRun(host, 'max_steps', null, undefined, step - 1);
    return refuse(text);
  }
  const [rules, task, total, previous] = await Promise.all([
    readOr(host, joinPath(host.runDir, 'rules.md'), MCP_RULES),
    taskOf(host, meta),
    lastSeq(host),
    lastContextResult(host),
  ]);
  const shown = Number.isInteger(previous?.total) ? Math.min(previous.total, total) : 0;
  const zone1Start = Math.max(0, shown - H1);
  const zone2Start = Math.max(0, zone1Start - H2);
  const window = (await host.log.read({ after: zone2Start })).filter((e) => Number.isInteger(e.seq) && e.seq > zone2Start && e.seq <= total);
  const since = window.filter((e) => e.seq > shown);
  const killedEvents = since.filter((e) => e.kind === 'job-killed');
  const killedNames = new Set(killedEvents.map((e) => e.job).filter((j) => typeof j === 'string'));
  const recoveryEvent = lastWhere(since, (e) => e.kind === 'recovery');
  const ended = await Promise.all(
    since
      .filter((e) => e.kind === 'job' && e.state !== 'running' && !(e.state === 'killed' && killedNames.has(e.job)))
      .map((e) => endedJob(host, e)),
  );
  const lists = [
    ['jobsKilled', killedEvents.map((e) => pick(e, ['seq', 'job', 'reason', 'lifetimeMs', 'tail']))],
    ['jobsStarted', since.filter((e) => e.kind === 'job' && e.state === 'running' && 'body' in e).map((e) => pick(e, ['seq', 'job']))],
    ['jobsRunning', since.filter((e) => e.kind === 'job-running').map((e) => pick(e, ['seq', 'job', 'runningMs', 'prd']))],
    ['jobsEnded', ended],
    ['jobsStuck', since.filter((e) => e.kind === 'job-stuck').map((e) => pick(e, ['seq', 'job', 'body', 'sinceProgressMs']))],
    ['jobsUnstuck', since.filter((e) => e.kind === 'job-unstuck').map((e) => pick(e, ['seq', 'job']))],
  ];
  const earlier = await actionsBefore(host, window, zone2Start);
  const bySeq = new Map(window.map((e) => [e.seq, e]));
  const actionAt = (seq) => bySeq.get(seq) ?? earlier.get(seq);
  const feed = feedOf(window, zone2Start, zone1Start, Math.min(shown, total), actionAt);
  const runId = baseName(host.runDir);
  const reply = {
    ok: true,
    step,
    run: runId,
    stepsLeft: Math.max(0, maxSteps - step),
    maxSteps,
    prefixSha: await prefixShaOf(host, rules, task, args),
    rules,
    task: task === '' ? null : task,
  };
  if (task === '') reply.taskNotice = 'No task yet. The gm skill prompt, with its task argument, gives the task of this run.';
  reply.witness = DEFAULT_WITNESS;
  reply.total = total;
  reply.pendingCommands = { wait: typeof args.waitCommand === 'string' ? args.waitCommand : `"gmnew" events --dir "${host.runDir}" --wait` };
  Object.assign(reply, feed);
  reply.query = queryHint(shown, total);
  if (recoveryEvent) reply.recovery = recoveryNote(recoveryEvent);
  for (const [key, list] of lists) if (list.length > 0) reply[key] = list;
  const pending = pendingOf(args);
  if (pending.length > 0) reply.pending = pending;
  if (typeof args.pendingError === 'string') reply.pendingError = args.pendingError;
  return reply;
}

function round3(value) {
  return Math.round(value * 1000) / 1000;
}

function merged(intervals) {
  const out = [];
  for (const [from, to] of [...intervals].sort((a, b) => a[0] - b[0])) {
    const last = out[out.length - 1];
    if (last && from <= last[1]) last[1] = Math.max(last[1], to);
    else out.push([from, to]);
  }
  return out;
}

const spanTotal = (intervals) => intervals.reduce((sum, [from, to]) => sum + (to - from), 0);

async function writeSummary(host) {
  const events = await host.log.read({});
  if (events.some((e) => e.kind === 'summary')) return;
  const endMono = host.now().mono;
  const firstMono = events.length > 0 && typeof events[0].mono === 'number' ? events[0].mono : null;
  const wall = firstMono === null ? 0 : endMono - firstMono;
  const spans = (kinds) => events.filter((e) => kinds.has(e.kind) && typeof e.startMono === 'number').map((e) => [e.startMono, typeof e.mono === 'number' ? e.mono : 0]);
  const tool = spans(new Set(['result', 'refusal']));
  const model = spans(new Set(['reply', 'retry', 'error']));
  const mcp = events.find((e) => e.kind === 'start')?.mode === 'mcp';
  await host.log.append('summary', {
    maxInFlight: events.reduce((max, e) => Math.max(max, Number.isInteger(e.inFlight) ? e.inFlight : 0), 0),
    meanInFlight: wall > 0 ? round3(spanTotal(tool) / wall) : 0,
    modelMs: round3(spanTotal(merged(model))),
    toolMs: round3(spanTotal(merged(tool))),
    idleMs: round3(Math.max(0, wall - spanTotal(merged([...tool, ...model])))),
    contextCalls: mcp ? events.filter((e) => e.kind === 'result' && e.tool === 'next_context').length : events.filter((e) => e.kind === 'reply').length,
  });
}

async function endRun(host, status, witness, gaps, steps) {
  const [already] = await host.log.read({ kind: 'end', last: 1 });
  if (already) return;
  await writeSummary(host);
  const fields = { status, steps, witness };
  if (gaps !== undefined) fields.gaps = gaps;
  await host.log.append('end', fields);
  await Promise.resolve()
    .then(() => host.fs.remove(joinPath(host.runDir, 'code')))
    .catch(() => null);
}

function parseRow(source) {
  const row = {};
  for (const line of source.split('\n')) {
    const at = line.indexOf(': ');
    if (at <= 0) continue;
    const key = line.slice(0, at);
    if (!/^[A-Za-z]+$/.test(key)) continue;
    const raw = line.slice(at + 2);
    try {
      row[key] = JSON.parse(raw);
    } catch {
      row[key] = raw;
    }
  }
  return row;
}

const rowText = (row, key) => (typeof row[key] === 'string' ? row[key] : '');

async function openRows(host) {
  const dir = joinPath(host.workspace, ROW_DIR);
  if (!(await host.fs.stat(dir))) return [];
  const names = (await host.fs.list(dir)).filter((name) => name.endsWith('.yml'));
  const rows = await Promise.all(names.map(async (name) => parseRow(await host.fs.read(joinPath(dir, name)))));
  const byCreated = (a, b) => {
    const [ca, cb, ia, ib] = [rowText(a, 'createdAt'), rowText(b, 'createdAt'), rowText(a, 'id'), rowText(b, 'id')];
    if (ca !== cb) return ca < cb ? -1 : 1;
    if (ia !== ib) return ia < ib ? -1 : 1;
    return 0;
  };
  return rows.filter((row) => rowText(row, 'status') === 'open').sort(byCreated);
}

function runningJobs(host) {
  const listed = host.jobs.list();
  return (Array.isArray(listed) ? listed : []).filter((j) => j && (j.state === 'running' || j.running === true));
}

export async function finish(args = {}, host) {
  const witness = args.witness;
  if (typeof witness !== 'string' || witness.trim() === '') return refuse('finish needs a non-empty witness string');
  const gaps = args.gaps;
  if (gaps !== undefined && !(Array.isArray(gaps) && gaps.every((g) => typeof g === 'string' && g.trim() !== ''))) return refuse('gaps must be an array of non-empty strings');
  const stopBackground = args.stopBackground === true;
  const running = Number.isInteger(args.backgroundRunning) ? args.backgroundRunning : runningJobs(host).length;
  if (running > 0 && !stopBackground) {
    return refuse(`${running} background run(s) still running; job_status watches them, and finish with stopBackground: true kills them`);
  }
  let open;
  try {
    open = (await openRows(host)).map((row) => [rowText(row, 'id'), rowText(row, 'title')]);
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : String(error) };
  }
  const stopRows = args.stopRows === true;
  if (open.length > 0 && !stopRows) {
    const names = open.slice(0, 5).map(([id, title]) => `${id} ${title}`);
    const more = open.length > 5 ? '; ...' : '';
    return refuse(`${open.length} row(s) open: ${names.join('; ')}${more}. Work them, or finish with stopRows: true to end with them listed as gaps`);
  }
  const endGaps = stopRows ? [...(gaps ?? []), ...open.map(([id, title]) => `open row ${id}: ${title}`)] : gaps;
  if (stopBackground) {
    await Promise.all(runningJobs(host).map((j) => host.jobs.stop(j.job ?? j.name, 'stopBackground')));
  }
  const steps = Number.isInteger(args.step) ? args.step : await stepsTaken(host);
  return { ok: true, finished: true, output: witness, end: { witness, gaps: endGaps ?? null, steps } };
}
