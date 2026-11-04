const OUTPUT_PAGE = 20000;
const VERDICTS = ['pass', 'fail', 'needs-changes'];

function jsText(value) {
  if (value === undefined) return 'undefined';
  if (value === null) return 'null';
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
}

function sortKeys(value) {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value !== null && typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value).sort()) out[key] = sortKeys(value[key]);
    return out;
  }
  return value;
}

function isAbsolutePath(path) {
  return path.startsWith('/') || path.startsWith('\\') || /^[A-Za-z]:[\\/]/.test(path);
}

function resolvePath(root, rel) {
  const full = isAbsolutePath(rel) ? rel : `${root}/${rel}`;
  const drive = /^[A-Za-z]:/.exec(full);
  const prefix = drive ? drive[0] : '';
  const rest = full.slice(prefix.length);
  const parts = [];
  for (const part of rest.split(/[\\/]/)) {
    if (part === '' || part === '.') continue;
    if (part === '..') parts.pop();
    else parts.push(part);
  }
  const lead = /^[\\/]/.test(rest) ? '/' : '';
  return `${prefix}${lead}${parts.join('/')}`;
}

async function fileAt(host, root, rel) {
  if (typeof rel !== 'string' || rel === '') return null;
  const at = resolvePath(root, rel);
  let stat = await host.fs.stat(at);
  if (stat && stat.type === 'symlink') {
    try {
      stat = await host.fs.stat(await host.fs.realpath(at));
    } catch {
      return null;
    }
  }
  return stat && stat.type === 'file' ? at : null;
}

async function shaOfFile(host, path) {
  try {
    return host.hash.sha256(await host.fs.read(path));
  } catch {
    return null;
  }
}

function packetFields(event) {
  return {
    spec: typeof event.spec === 'string' ? event.spec : '',
    specSha: typeof event.specSha === 'string' ? event.specSha : '',
  };
}

async function reviewPacket(host, args) {
  const root = host.workspace;
  const refuse = async (reason) => {
    await host.log.append('review-refused', { reason, stage: 'packet' });
    return { ok: false, error: `refused: ${reason}` };
  };
  let refs;
  if (args.refs === undefined || args.refs === null) refs = [];
  else if (Array.isArray(args.refs) && args.refs.every((ref) => typeof ref === 'string')) refs = args.refs;
  else return refuse('refs must be a list of paths');
  const spec = typeof args.spec === 'string' ? args.spec : '';
  const specAt = await fileAt(host, root, spec);
  if (specAt === null) return refuse(`spec ${jsText(args.spec)} is not a file`);
  const missing = [];
  for (const ref of refs) if ((await fileAt(host, root, ref)) === null) missing.push(ref);
  if (missing.length > 0) return refuse(`referenced paths do not exist: ${missing.join(', ')}`);
  let specSha;
  try {
    specSha = host.hash.sha256(await host.fs.read(specAt));
  } catch {
    return { ok: false, error: 'spec is not readable' };
  }
  const sortedRefs = [...refs].sort();
  const hash = host.hash.sha256(JSON.stringify({ refs: sortedRefs, spec, specSha }));
  await host.log.append('review-packet', { packet: hash, spec, specSha, refs: sortedRefs });
  const result = JSON.stringify({ ok: true, packet: hash, refs: sortedRefs, spec, specSha });
  return { ok: true, output: result };
}

async function reviewRecord(host, args) {
  const root = host.workspace;
  const packetArg = args.packet;
  const refuse = async (reason) => {
    const fields = {};
    if (packetArg !== undefined) fields.packet = packetArg;
    fields.reason = reason;
    fields.stage = 'record';
    await host.log.append('review-refused', fields);
    return { ok: false, error: `refused: ${reason}` };
  };
  if (typeof packetArg !== 'string') return refuse(`no review packet ${jsText(packetArg)} in this log`);
  const packet = packetArg;
  const events = await host.log.read({});
  let made = null;
  let reviewed = false;
  for (const event of events) {
    const same = event.packet === packet;
    if (event.kind === 'review-packet' && same && made === null) made = packetFields(event);
    if (event.kind === 'review' && same) reviewed = true;
  }
  if (made === null) return refuse(`no review packet ${packet} in this log`);
  if (reviewed) return refuse(`packet ${packet} already has a record`);
  const verdict = args.verdict;
  if (typeof verdict !== 'string' || !VERDICTS.includes(verdict)) return refuse('verdict must be one of pass, fail, needs-changes');
  const reviewer = args.reviewer;
  if (typeof reviewer !== 'string' || reviewer.trim() === '') return refuse('reviewer must name the fresh reviewer that the host spawned');
  let notes = '';
  if (args.notes !== undefined && args.notes !== null) {
    if (typeof args.notes !== 'string') return refuse('notes must be a string');
    notes = args.notes;
  }
  const specAt = await fileAt(host, root, made.spec);
  const nowSha = specAt === null ? null : await shaOfFile(host, specAt);
  if (nowSha !== made.specSha) {
    return refuse(`the spec changed since packet ${packet}: packet sha ${made.specSha}, now ${nowSha ?? 'missing'}`);
  }
  await host.log.append('review', {
    packet,
    verdict,
    reviewer,
    notes,
    isolation: 'host-attested',
    specSha: made.specSha,
  });
  return { ok: true, output: JSON.stringify({ ok: true, packet, verdict }) };
}

async function specShaAt(host, root, commit, spec) {
  if (typeof commit !== 'string' || commit === '' || spec === null) return null;
  try {
    const shown = await host.proc.run('git', ['show', `${commit}:${spec}`], { cwd: root, timeoutMs: 10000 });
    if (shown.code !== 0 || shown.timedOut) return null;
    return host.hash.sha256(shown.stdout);
  } catch {
    return null;
  }
}

async function currentSpecSha(host, root, spec) {
  if (spec === null) return null;
  const at = await fileAt(host, root, spec);
  return at === null ? null : shaOfFile(host, at);
}

function place(row, covered, item, reason) {
  if (covered) row.outcomes.push(item);
  else row.unattributed.push({ ...item, reason });
}

async function reviewJoin(host) {
  const root = host.workspace;
  const events = await host.log.read({});
  const packets = new Map();
  for (const event of events) {
    if (event.kind === 'review-packet' && typeof event.packet === 'string') packets.set(event.packet, packetFields(event));
  }
  const rows = [];
  let row = null;
  let commitCovered = true;
  for (const event of events) {
    const kind = event.kind ?? '';
    const seq = event.seq ?? null;
    if (kind === 'review') {
      const made = typeof event.packet === 'string' ? packets.get(event.packet) : undefined;
      row = {
        packet: event.packet ?? null,
        verdict: event.verdict ?? null,
        reviewer: event.reviewer ?? null,
        isolation: event.isolation ?? null,
        seq,
        spec: made ? made.spec : null,
        specSha: made ? made.specSha : null,
        outcomes: [],
        unattributed: [],
      };
      rows.push(row);
    } else if (row === null) {
      continue;
    } else if (kind === 'commit') {
      const specAt = await specShaAt(host, root, event.sha, row.spec);
      commitCovered = specAt !== null && specAt === row.specSha;
      const gateOk = Array.isArray(event.gate) ? event.gate.every((check) => !!(check && check.ok)) : null;
      const item = { seq, kind: 'commit', gateOk, sha: event.sha ?? null, specSha: specAt };
      place(row, commitCovered, item, specAt === null ? 'no spec at the commit' : 'after-edit');
    } else if (kind === 'result' && event.tool === 'git_commit') {
      const ok = event.ok === true;
      place(row, ok ? commitCovered : true, { seq, kind: 'git_commit', ok }, 'its commit is after-edit');
    } else if (kind === 'end') {
      const now = await currentSpecSha(host, root, row.spec);
      const covered = now !== null && now === row.specSha;
      const item = { seq, kind: 'end', status: event.status ?? null, witness: event.witness ?? null, specSha: now };
      place(row, covered, item, now === null ? 'no spec on disk' : 'after-edit');
    }
  }
  const text = JSON.stringify(sortKeys(rows), null, 1);
  const chars = Array.from(text);
  const output = chars.length > OUTPUT_PAGE
    ? `${chars.slice(0, OUTPUT_PAGE).join('')}\n(truncated at ${OUTPUT_PAGE} characters)`
    : text;
  return { ok: true, output };
}

export const verbs = [
  {
    name: 'review_packet',
    description: 'Open a review of a spec file and the files it refers to. The references must exist. Returns the packet hash that review_record takes. A missing reference, or a spec that is not a file, is refused and logged.',
    schema: {
      type: 'object',
      properties: { spec: { type: 'string' }, refs: { type: 'array', items: { type: 'string' } } },
      required: ['spec'],
    },
    lock: () => ({ mode: 'read', paths: [] }),
    async call(args, host) {
      return reviewPacket(host, args);
    },
  },
  {
    name: 'review_record',
    description: 'Record one reviewer verdict (pass, fail or needs-changes) on a packet, with the reviewer and its notes. Refused when the spec changed since the packet, when the packet already has a record, or when the verdict is unknown. Each record says isolation host-attested.',
    schema: {
      type: 'object',
      properties: {
        packet: { type: 'string' },
        verdict: { type: 'string' },
        reviewer: { type: 'string' },
        notes: { type: 'string' },
      },
      required: ['packet', 'verdict', 'reviewer'],
    },
    lock: () => ({ mode: 'write', paths: [] }),
    async call(args, host) {
      return reviewRecord(host, args);
    },
  },
  {
    name: 'review_join',
    description: 'Join the review records of this run with the commits and the ends after them: each commit with its gate outcome, and each end with its status and witness. A commit made after its spec changed is unattributed.',
    schema: { type: 'object', properties: {}, required: [] },
    lock: () => ({ mode: 'write', paths: [] }),
    async call(args, host) {
      return reviewJoin(host);
    },
  },
];
