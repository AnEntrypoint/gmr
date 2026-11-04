const OUTPUT_CAP = 200000;
const DEFAULT_TIMEOUT_MS = 60000;
const MAX_TIMEOUT_MS = 600000;
const MAX_WAIT_MS = 30000;
const JOB_NAME = /^[A-Za-z0-9_-]{1,64}$/;

const DESC_RUN_SHELL = 'Run a shell command in the workspace and wait for it. Default timeout 60000 ms. Its process group is killed when it returns, so background work must go through job_start.';
const SCHEMA_RUN_SHELL = { type: 'object', properties: { command: { type: 'string' }, timeoutMs: { type: 'number' } }, required: ['command'] };
const DESC_JOB_START = 'Start a shell command as a named background job and return at once. Poll it with job_status.';
const SCHEMA_JOB_START = { type: 'object', properties: { job: { type: 'string' }, command: { type: 'string' }, timeoutMs: { type: 'number' } }, required: ['job', 'command'] };
const DESC_JOB_STATUS = 'State (running, exited or killed), exitCode, signal and output of a job as plain fields, with logPath (the job output file). output is the last 20000 characters, with total (its length) and truncated; with offset, output is the 20000 characters from that offset, with next. waitMs polls until the job leaves running, at most 30000 ms. Offsets and lengths count characters, not lines.';
const SCHEMA_JOB_STATUS = { type: 'object', properties: { job: { type: 'string' }, waitMs: { type: 'number' }, offset: { type: 'number' } }, required: ['job'] };
const DESC_JOB_STOP = 'Stop a background shell job or a background run_code body (body-<seq>): a shell job\'s process group is killed, a body is stopped at its next verb call, and the job ends killed with reason cancelled. The job event is logged before the answer. A job that already ended is an error result.';
const SCHEMA_JOB_STOP = { type: 'object', properties: { job: { type: 'string' } }, required: ['job'] };

const fail = (error) => ({ ok: false, error });

function numberOf(args, key) {
  const value = args[key];
  if (typeof value === 'number') return value;
  if (typeof value === 'string') {
    const text = value.trim();
    return text === '' ? NaN : Number(text);
  }
  return NaN;
}

function timeoutOf(args) {
  const n = numberOf(args, 'timeoutMs');
  return n > 0 ? Math.trunc(Math.min(n, MAX_TIMEOUT_MS)) : DEFAULT_TIMEOUT_MS;
}

function waitOf(args) {
  const n = numberOf(args, 'waitMs');
  return n > 0 ? Math.trunc(Math.min(n, MAX_WAIT_MS)) : 0;
}

function commandOf(args) {
  const command = args.command;
  return typeof command === 'string' && command.trim() !== '' ? command : null;
}

function jobNameOf(args) {
  const job = args.job;
  return typeof job === 'string' && JOB_NAME.test(job) ? job : null;
}

function isAbsent(value) {
  return value === undefined || value === null;
}

export const verbs = [
  {
    name: 'run_shell',
    description: DESC_RUN_SHELL,
    schema: SCHEMA_RUN_SHELL,
    lock() {
      return { mode: 'shell' };
    },
    async call(args, host) {
      const command = commandOf(args);
      if (command === null) return fail('command must be a non-empty string');
      const timeoutMs = timeoutOf(args);
      const r = await host.proc.shell(command, { cwd: host.workspace, timeoutMs });
      const output = `${r.stdout ?? ''}${r.stderr ?? ''}`.slice(0, OUTPUT_CAP);
      if (r.timedOut) return { ok: false, timedOut: true, error: `timed out after ${timeoutMs} ms`, output };
      return { ok: r.code === 0, exitCode: r.code ?? null, signal: r.signal ?? null, output };
    },
  },
  {
    name: 'job_start',
    description: DESC_JOB_START,
    schema: SCHEMA_JOB_START,
    lock() {
      return { mode: 'write' };
    },
    async call(args, host) {
      const command = commandOf(args);
      if (command === null) return fail('command must be a non-empty string');
      const timeoutMs = timeoutOf(args);
      const job = jobNameOf(args);
      if (job === null) return fail('a background job needs job: 1 to 64 letters, digits, _ or -');
      const r = await host.jobs.start(job, command, { timeoutMs });
      return { ok: true, state: 'running', output: typeof r.output === 'string' ? r.output : `started job ${job}` };
    },
  },
  {
    name: 'job_status',
    description: DESC_JOB_STATUS,
    schema: SCHEMA_JOB_STATUS,
    lock() {
      return { mode: 'read' };
    },
    async call(args, host) {
      const job = jobNameOf(args);
      if (job === null) return fail('job must be a job name');
      const waitMs = waitOf(args);
      const offset = args.offset;
      if (!isAbsent(offset) && !(Number.isInteger(offset) && offset >= 0)) {
        return fail('offset must be a whole number of characters, zero or more');
      }
      const opts = { waitMs };
      if (!isAbsent(offset)) opts.offset = offset;
      const status = await host.jobs.status(job, opts);
      if (!status) return fail(`no job named ${job}`);
      if (status.ok === false) return status;
      return { ...status, ok: true, exitCode: status.exitCode ?? null, signal: status.signal ?? null };
    },
  },
  {
    name: 'job_stop',
    description: DESC_JOB_STOP,
    schema: SCHEMA_JOB_STOP,
    lock() {
      return { mode: 'write' };
    },
    async call(args, host) {
      const job = jobNameOf(args);
      if (job === null) return fail('job must be a job name');
      const r = await host.jobs.stop(job, 'cancelled');
      if (r && typeof r === 'object' && 'ok' in r) return r;
      return { ok: true, state: 'killed', reason: 'cancelled', output: `stopped job ${job}` };
    },
  },
];
