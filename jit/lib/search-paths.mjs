export const STATE_DIRS = new Set(['.git', '.gm', '.agentplug', '.agentplug-kv', '.gm-new', '.playwriter', 'runs']);

export function sepOf(path) {
  return path.includes('\\') && !path.includes('/') ? '\\' : '/';
}

export function isAbsolutePath(path) {
  return path.startsWith('/') || path.startsWith('\\') || /^[A-Za-z]:[\\/]/.test(path);
}

export function normalizePath(path) {
  const sep = sepOf(path);
  const drive = /^[A-Za-z]:/.exec(path);
  const prefix = drive === null ? '' : drive[0];
  let rest = drive === null ? path : path.slice(2);
  const lead = rest.startsWith('/') || rest.startsWith('\\');
  if (lead) rest = rest.slice(1);
  const parts = [];
  for (const seg of rest.split(/[\\/]+/)) {
    if (seg === '' || seg === '.') continue;
    if (seg === '..') parts.pop();
    else parts.push(seg);
  }
  return prefix + (lead ? sep : '') + parts.join(sep);
}

export function joinPath(base, rel) {
  if (rel === '') return base;
  const last = base.charAt(base.length - 1);
  return last === '/' || last === '\\' ? base + rel : base + sepOf(base) + rel;
}

export function resolveIn(base, rel) {
  return normalizePath(isAbsolutePath(rel) ? rel : joinPath(base, rel));
}

export function relUnder(root, target) {
  if (target === root) return '';
  const sep = sepOf(root);
  const prefix = root.endsWith(sep) ? root : root + sep;
  if (!target.startsWith(prefix)) return null;
  const rest = target.slice(prefix.length);
  return sep === '\\' ? rest.split('\\').join('/') : rest;
}

export function isVendor(rel) {
  const at = rel.lastIndexOf('/');
  return at >= 0 && rel.slice(0, at).split('/').some((part) => part === 'node_modules' || part === 'vendor');
}

export function inState(rel) {
  const at = rel.lastIndexOf('/');
  return at >= 0 && rel.slice(0, at).split('/').some((part) => STATE_DIRS.has(part));
}

export function underDir(paths, dir, vendor) {
  return paths.filter((rel) => {
    const inside = dir === '' || rel === dir || (rel.startsWith(dir) && rel.charAt(dir.length) === '/');
    return inside && (vendor || !isVendor(rel));
  });
}

export function plural(count, one, many = `${one}s`) {
  return `${count} ${count === 1 ? one : many}`;
}
