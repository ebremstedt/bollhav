// Thin fetch wrappers over the FastAPI backend (see gui/backend/app.py).
// Paths are proxied to the backend by vite (see vite.config.js).
const json = (r) => r.json();
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// The current selection, carried on every request so the whole UI — graph,
// panels, tag filter — reads the same place:
//   catalog: which state database (null = the backend's default catalog)
//   env:     which library schema in it (null = prod, z_bollhav)
let _catalog = null;
let _env = null;
export function setApiCatalog(catalog) {
  _catalog = catalog || null;
}
export function setApiEnv(env) {
  _env = env || null;
}
// `?catalog=…&env=…` (only what is set), with `sep` as the first separator
const sel = (sep) => {
  const parts = [];
  if (_catalog) parts.push(`catalog=${encodeURIComponent(_catalog)}`);
  if (_env) parts.push(`env=${encodeURIComponent(_env)}`);
  return parts.length ? `${sep}${parts.join("&")}` : "";
};

// A prod library is served from the backend's cache. Right after the backend
// started that cache may still be computing (503 + Retry-After); wait and ask
// again rather than fail, for up to about three minutes.
const cached = async (path) => {
  for (let attempt = 0; ; attempt++) {
    const r = await fetch(path);
    if (r.status === 503 && attempt < 60) {
      await sleep(3000);
      continue;
    }
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }
};

// Runtime UI config (env-var driven on the backend): a model / tag expression
// to pre-narrow the graph on load. Falls back to empty config if unavailable.
export const getConfig = () => fetch("/config").then(json).catch(() => ({}));

// The catalogs (state databases) the backend reads, its default first.
export const getCatalogs = () => fetch("/catalogs").then(json);

// The bollhav library schemas in the selected catalog (prod + suffixed envs).
export const getEnvironments = () => fetch(`/environments${sel("?")}`).then(json);

// {cached, computed_at, refreshing, refresh_seconds} for the selection: is it
// served from the cache, and how fresh is that cache.
export const getFreshness = () => fetch(`/freshness${sel("?")}`).then(json);

// Ask the backend to recompute the selected catalog's cache now (background;
// poll getFreshness for the new computed_at).
export const refreshCatalog = () =>
  fetch(`/refresh${sel("?")}`, { method: "POST" }).then(json);

// Recompute one model in the cache right now: its status lights, gaps entry,
// grid row and recent runs / errors.
export const refreshModel = (name) =>
  fetch(`/refresh/${encodeURIComponent(name)}${sel("?")}`, { method: "POST" }).then(json);

export const getGraph = () => cached(`/graph${sel("?")}`);

// A model's recent runs / errors (newest first), capped at `limit` — the
// side panel's "number of runs" choice.
export const getState = (name, limit = 100) =>
  fetch(`/state/${encodeURIComponent(name)}?limit=${limit}${sel("&")}`).then(json);

export const getErrors = (name, limit = 100) =>
  fetch(`/errors?full_name=${encodeURIComponent(name)}&limit=${limit}${sel("&")}`).then(json);

// All recent errors across every model (newest first), capped at `limit`.
// Powers the historical Runs tab (errors view).
export const getAllErrors = (limit = 50) => cached(`/errors?limit=${limit}${sel("&")}`);

// All recent run/interval state rows across every model (newest first),
// capped at `limit`. Powers the Runs tab (runs view).
export const getAllRuns = (limit = 50) => cached(`/runs?limit=${limit}${sel("&")}`);

// Per-model run history for the grid view: [{full_name, runs:[…]}], each
// model's most recent `limit` runs (newest first).
export const getGrid = (limit = 40) => cached(`/grid?limit=${limit}${sel("&")}`);

// Reset part of a model's state so the next run redoes it (POST). `body` is
// one of `{all: true}`, `{intervals: [{since, until}, …]}` or
// `{range: {since, until}}` → `{full_name, reset: n}`. Throws with the
// backend's message on a refusal (read-only deployment, bad request).
export const resetState = async (name, body) => {
  const r = await fetch(`/state/${encodeURIComponent(name)}/reset${sel("?")}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    let msg = r.statusText;
    try {
      msg = (await r.json()).detail || msg;
    } catch {}
    throw new Error(msg);
  }
  return r.json();
};

// Per-model backfill gaps: [{full_name, has_contract, begin, end, gaps:[…],
// contract_seconds, gap_seconds, covered_seconds, pct_covered, status_counts}].
// The spans of each model's contract window not yet `applied` in state. Powers
// the Gaps tab.
export const getGaps = () => cached(`/gaps${sel("?")}`);

// The model's stored property bag (library.metadata): write_mode, tags,
// description, bounds, batching, columns, … Fetched lazily on ⓘ hover.
export const getModelMeta = (name) =>
  fetch(`/model/${encodeURIComponent(name)}${sel("?")}`).then(json);

// Models matching a bollhav tag expression (the `[group]` syntax). Returns
// `[{name, tags}]` (tags = the model's own tags that matched, for highlighting),
// or null on a malformed expression (backend 400).
export const getMatch = (expr) =>
  fetch(`/match?expr=${encodeURIComponent(expr)}${sel("&")}`).then((r) =>
    r.ok ? r.json().then((d) => d.models) : null,
  );
