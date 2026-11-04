import { messageOf, runGate, runTool } from '../lib/gate.mjs';

const GIT_TIMEOUT_MS = 120000;
const CHECK_TIMEOUT_MS = 15000;
const OUTPUT_CAP = 200000;
const REF_ERROR = 'ref must be a branch, tag or commit name';
const PATHS_ERROR = 'paths must be a non-empty array of strings';
const DOT_ERROR = 'refused: "." names the whole workspace; name each path explicitly';
const REF_PATTERN = /^[A-Za-z0-9._\/@-]+$/;
const SCOPE = 'cwd, when given, is the directory whose repository the verb acts on: absolute, or relative to the workspace root; a directory outside a git repository is refused. Omitted, the workspace is used.';
const STR = { type: 'string' };
const NUM = { type: 'number' };
const BOOL = { type: 'boolean' };
const PATHS = { type: 'array', items: { type: 'string' } };

const fail = (error) => ({ ok: false, error });

function isAbsolute(path) {
  return /^(?:[A-Za-z]:)?[\\/]/.test(path);
}

function joinPath(base, rel) {
  return isAbsolute(rel) ? rel : `${base}/${rel}`;
}

function normalizePath(text) {
  const slashed = text.replace(/\\/g, '/');
  const lead = /^(?:[A-Za-z]:)?\//.exec(slashed);
  const prefix = lead === null ? '' : lead[0];
  const parts = [];
  for (const segment of slashed.slice(prefix.length).split('/')) {
    if (segment === '' || segment === '.') continue;
    if (segment === '..') parts.pop();
    else parts.push(segment);
  }
  return prefix + parts.join('/');
}

function relOf(base, at) {
  if (at === base) return '';
  const lead = base.endsWith('/') ? base : `${base}/`;
  return at.startsWith(lead) ? at.slice(lead.length) : null;
}

function within(child, base) {
  return relOf(base, child) !== null;
}

function parentOf(path) {
  const at = path.lastIndexOf('/');
  if (at < 0) return null;
  if (at === 0) return path.length > 1 ? '/' : null;
  if (/^[A-Za-z]:$/.test(path.slice(0, at))) return path.length > at + 1 ? path.slice(0, at + 1) : null;
  return path.slice(0, at);
}

async function statOrNull(host, path) {
  try {
    return (await host.fs.stat(path)) ?? null;
  } catch {
    return null;
  }
}

async function isDirectory(host, path) {
  const meta = await statOrNull(host, path);
  if (meta === null) return false;
  if (meta.type === 'dir') return true;
  if (meta.type !== 'symlink') return false;
  try {
    const target = await statOrNull(host, await host.fs.realpath(path));
    return target !== null && target.type === 'dir';
  } catch {
    return false;
  }
}

async function realpathOr(host, path) {
  try {
    return normalizePath(await host.fs.realpath(path));
  } catch {
    return path;
  }
}

function cap(text) {
  return text.length > OUTPUT_CAP ? text.slice(0, OUTPUT_CAP) : text;
}

async function gitRun(host, dir, args, timeoutMs = GIT_TIMEOUT_MS) {
  return runTool(host, 'git', ['--literal-pathspecs', ...args], { cwd: dir, timeoutMs });
}

function verbResult(sub, cmd) {
  const output = cap(cmd.output).trim();
  if (cmd.ok) return { ok: true, output: output === '' ? '(no output)' : output };
  return { ok: false, error: cmd.error ?? `git ${sub} exited ${cmd.exitCode ?? 'null'}`, output };
}

async function confine(host, root, rel) {
  if (rel === '') return { refused: 'path must be a non-empty string' };
  if (rel.split(/[\\/]/).includes('..')) return { refused: 'path has a .. segment' };
  const target = normalizePath(joinPath(root, rel));
  if (!within(target, root)) return { refused: 'path is outside the workspace' };
  const realRoot = await realpathOr(host, root);
  let probe = target;
  while ((await statOrNull(host, probe)) === null) {
    const parent = parentOf(probe);
    if (parent === null) break;
    probe = parent;
  }
  let real;
  try {
    real = normalizePath(await host.fs.realpath(probe));
  } catch {
    return { refused: 'path leads through a link that does not resolve' };
  }
  return within(real, realRoot) ? { path: target } : { refused: 'path leads through a link outside the workspace' };
}

async function repoScope(host, args) {
  const root = normalizePath(host.workspace);
  if (args.cwd === undefined) return { scope: { root, dir: root } };
  const cwd = args.cwd;
  if (typeof cwd !== 'string' || cwd === '') return { error: 'cwd must be a non-empty path' };
  const dir = normalizePath(joinPath(root, cwd));
  if (!(await isDirectory(host, dir))) return { error: `no such directory: ${cwd}` };
  const inside = await gitRun(host, dir, ['rev-parse', '--show-toplevel'], CHECK_TIMEOUT_MS);
  return inside.ok ? { scope: { root, dir } } : { error: `cwd ${cwd} is not inside a git repository` };
}

async function inRepo(host, args, run) {
  const found = await repoScope(host, args);
  return found.error === undefined ? run(found.scope) : fail(found.error);
}

async function pathspecs(host, scope, list) {
  if (!Array.isArray(list) || list.length === 0 || !list.every((item) => typeof item === 'string')) {
    return { error: PATHS_ERROR };
  }
  const rels = [];
  for (const text of list) {
    let at;
    if (scope.dir === scope.root) {
      const confined = await confine(host, scope.root, text);
      if (confined.refused !== undefined) return { error: confined.refused };
      at = confined.path;
    } else {
      at = normalizePath(joinPath(scope.dir, text));
    }
    const rel = relOf(scope.dir, at);
    if (rel === null) return { error: `path ${text} is outside ${scope.dir}` };
    if (rel === '') return { error: DOT_ERROR };
    rels.push(rel);
  }
  return { rels };
}

function specTail(rels) {
  return rels.length > 0 ? ['--', ...rels] : [];
}

function refArg(args, fallback) {
  let found;
  if (args.ref === undefined && fallback !== null) found = fallback;
  else if (typeof args.ref === 'string') found = args.ref;
  else return { error: REF_ERROR };
  const valid = found.length > 0 && found.length <= 200 && !found.startsWith('-') && REF_PATTERN.test(found);
  return valid ? { ref: found } : { error: REF_ERROR };
}

async function gitStatus(args, host) {
  return inRepo(host, args, async ({ dir }) => verbResult('status', await gitRun(host, dir, ['status', '--porcelain=v1', '--branch'])));
}

async function gitAdd(args, host) {
  return inRepo(host, args, async (scope) => {
    const picked = await pathspecs(host, scope, args.paths);
    if (picked.error !== undefined) return fail(picked.error);
    return verbResult('add', await gitRun(host, scope.dir, ['add', ...specTail(picked.rels)]));
  });
}

async function gitRm(args, host) {
  return inRepo(host, args, async (scope) => {
    const picked = await pathspecs(host, scope, args.paths);
    if (picked.error !== undefined) return fail(picked.error);
    return verbResult('rm', await gitRun(host, scope.dir, ['rm', ...specTail(picked.rels)]));
  });
}

async function gitCommit(args, host) {
  return inRepo(host, args, async (scope) => {
    const { message } = args;
    if (typeof message !== 'string' || message.trim() === '') return fail('message must be a non-empty string');
    const picked = await pathspecs(host, scope, args.paths);
    if (picked.error !== undefined) return fail(picked.error);
    const rels = picked.rels;
    let gate;
    try {
      gate = await runGate(host, scope.dir, rels);
    } catch (err) {
      return fail(messageOf(err));
    }
    if (!gate.ok) {
      const reasons = gate.checks.filter((check) => !check.ok).map((check) => `${check.name}: ${check.detail}`);
      return { ok: false, error: `refused: the commit gate failed: ${reasons.join('; ')}`, gate: gate.checks, notCovered: gate.notCovered };
    }
    const result = verbResult('commit', await gitRun(host, scope.dir, ['commit', '-m', message, ...specTail(rels)]));
    if (result.ok) {
      const head = await gitRun(host, scope.dir, ['rev-parse', '--short', 'HEAD'], CHECK_TIMEOUT_MS);
      host.log.append('commit', {
        sha: head.ok ? head.output.trim() : null,
        message: message.split('\n')[0],
        paths: rels,
        gate: gate.checks,
        notCovered: gate.notCovered,
      });
    }
    return { ...result, gate: gate.checks, notCovered: gate.notCovered };
  });
}

async function gitPush(args, host) {
  return inRepo(host, args, async ({ dir }) => {
    const head = await gitRun(host, dir, ['rev-parse', '--abbrev-ref', 'HEAD'], CHECK_TIMEOUT_MS);
    const branch = head.ok ? head.output.trim() : null;
    host.log.append('stage', { tool: 'git_push', stage: 'branch', branch });
    if (branch !== 'main') {
      return fail(`refused: git_push sends only main, and the current branch is ${branch ?? 'unknown'}`);
    }
    const result = verbResult('push', await gitRun(host, dir, ['push', '--set-upstream', 'origin', 'main']));
    host.log.append('stage', { tool: 'git_push', stage: 'push', ok: result.ok });
    return result;
  });
}

async function gitLog(args, host) {
  return inRepo(host, args, async (scope) => {
    const count = args.count === undefined ? 20 : args.count;
    if (!Number.isInteger(count) || count < 1 || count > 200) return fail('count must be an integer from 1 to 200');
    let rels = [];
    if (args.path !== undefined) {
      const picked = await pathspecs(host, scope, [args.path]);
      if (picked.error !== undefined) return fail(picked.error);
      rels = picked.rels;
    }
    return verbResult('log', await gitRun(host, scope.dir, ['log', '--oneline', '--no-color', '-n', String(count), ...specTail(rels)]));
  });
}

async function gitDiff(args, host) {
  return inRepo(host, args, async (scope) => {
    let rels = [];
    if (args.paths !== undefined) {
      const picked = await pathspecs(host, scope, args.paths);
      if (picked.error !== undefined) return fail(picked.error);
      rels = picked.rels;
    }
    const head = ['diff', '--no-color'];
    if (args.staged === true) head.push('--cached');
    return verbResult('diff', await gitRun(host, scope.dir, [...head, ...specTail(rels)]));
  });
}

async function gitShow(args, host) {
  return inRepo(host, args, async (scope) => {
    const named = refArg(args, 'HEAD');
    if (named.error !== undefined) return fail(named.error);
    return verbResult('show', await gitRun(host, scope.dir, ['show', '--no-color', '--stat', named.ref]));
  });
}

async function gitFetch(args, host) {
  return inRepo(host, args, async ({ dir }) => verbResult('fetch', await gitRun(host, dir, ['fetch', 'origin'])));
}

async function gitPull(args, host) {
  return inRepo(host, args, async ({ dir }) => verbResult('pull', await gitRun(host, dir, ['pull', '--ff-only'])));
}

async function gitCheckout(args, host) {
  return inRepo(host, args, async (scope) => {
    const named = refArg(args, null);
    if (named.error !== undefined) return fail(named.error);
    return verbResult('checkout', await gitRun(host, scope.dir, ['checkout', named.ref]));
  });
}

async function gitBranch(args, host) {
  return inRepo(host, args, async ({ dir }) => verbResult('branch', await gitRun(host, dir, ['branch', '--no-color', '--list'])));
}

function verb(name, description, properties, required, mode, run) {
  return {
    name,
    description: `${description} ${SCOPE}`,
    schema: { type: 'object', properties, required },
    lock: () => ({ mode, paths: [] }),
    call: (args, host) => run(args ?? {}, host),
  };
}

export const verbs = [
  verb('git_status', 'Working tree status: the branch and the changed paths (porcelain v1).', { cwd: STR }, [], 'read', gitStatus),
  verb('git_add', 'Stage the given workspace paths. Each path is explicit; "." is refused.', { cwd: STR, paths: PATHS }, ['paths'], 'write', gitAdd),
  verb('git_rm', 'Remove the given workspace paths from the disk and the index, with git rm, so the removal is staged. Each path is explicit; "." is refused. Commit it with git_commit, which runs the commit gate.', { cwd: STR, paths: PATHS }, ['paths'], 'write', gitRm),
  verb('git_commit', 'Commit only the given paths, with the message. Stage new files with git_add first. The commit gate runs first: ASCII of every tracked text file in the paths, every path that DESIGN.md names exists, and cargo check of rust/ when rust/ is committed. A failed check refuses the commit. gate reports each check with its time, and notCovered lists what the gate does not check.', { cwd: STR, message: STR, paths: PATHS }, ['message', 'paths'], 'write', gitCommit),
  verb('git_push', 'Push main to origin and set its upstream. Refused unless the current branch is main.', { cwd: STR }, [], 'shell', gitPush),
  verb('git_log', 'The last count commits, one line each, optionally only for one path (count 1 to 200, default 20).', { cwd: STR, count: NUM, path: STR }, [], 'read', gitLog),
  verb('git_diff', 'Diff of the given paths (default: all), or of the staged index when staged is true.', { cwd: STR, paths: PATHS, staged: BOOL }, [], 'read', gitDiff),
  verb('git_show', 'Commit message and changed files for a ref (default HEAD).', { cwd: STR, ref: STR }, [], 'read', gitShow),
  verb('git_fetch', 'Fetch origin. The working tree is not changed.', { cwd: STR }, [], 'shell', gitFetch),
  verb('git_pull', 'Fast-forward pull of the current branch from its upstream. Never merges.', { cwd: STR }, [], 'shell', gitPull),
  verb('git_checkout', 'Switch to an existing branch, tag or commit. Creates nothing.', { cwd: STR, ref: STR }, ['ref'], 'write', gitCheckout),
  verb('git_branch', 'List local branches. The current branch is marked with *.', { cwd: STR }, [], 'read', gitBranch),
];
