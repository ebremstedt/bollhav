// Thin fetch wrappers over the FastAPI backend (see gui/backend/app.py).
// Paths are proxied to the backend by vite (see vite.config.js).
const json = (r) => r.json();

// The currently-selected environment (a bollhav library schema, e.g. a suffixed
// dev/PR env). null = prod (z_bollhav). Every read appends `?env=`/`&env=` so
// the whole UI — graph, panels, tag filter — reads from the same env.
let _env = null;
export function setApiEnv(env) {
  _env = env || null;
}
const envQ = (sep) => (_env ? `${sep}env=${encodeURIComponent(_env)}` : "");

// Runtime UI config (env-var driven on the backend): a model / tag expression
// to pre-narrow the graph on load. Falls back to empty config if unavailable.
export const getConfig = () => fetch("/config").then(json).catch(() => ({}));

// The bollhav library schemas in the connected DB (prod + suffixed envs).
export const getEnvironments = () => fetch("/environments").then(json);

export const getGraph = () => fetch(`/graph${envQ("?")}`).then(json);

// A model's recent runs / errors (newest first), capped at `limit` — the
// side panel's "number of runs" choice.
export const getState = (name, limit = 100) =>
  fetch(`/state/${encodeURIComponent(name)}?limit=${limit}${envQ("&")}`).then(json);

export const getErrors = (name, limit = 100) =>
  fetch(`/errors?full_name=${encodeURIComponent(name)}&limit=${limit}${envQ("&")}`).then(json);

// All recent errors across every model (newest first), capped at `limit`.
// Powers the historical Runs tab (errors view).
export const getAllErrors = (limit = 50) =>
  fetch(`/errors?limit=${limit}${envQ("&")}`).then(json);

// All recent run/interval state rows across every model (newest first),
// capped at `limit`. Powers the Runs tab (runs view).
export const getAllRuns = (limit = 50) =>
  fetch(`/runs?limit=${limit}${envQ("&")}`).then(json);

// Per-model run history for the grid view: [{full_name, runs:[…]}], each
// model's most recent `limit` runs (newest first).
export const getGrid = (limit = 40) =>
  fetch(`/grid?limit=${limit}${envQ("&")}`).then(json);

// Reset part of a model's state so the next run redoes it (POST). `body` is
// one of `{all: true}`, `{intervals: [{since, until}, …]}` or
// `{range: {since, until}}` → `{full_name, reset: n}`. Throws with the
// backend's message on a refusal (read-only deployment, bad request).
export const resetState = async (name, body) => {
  const r = await fetch(`/state/${encodeURIComponent(name)}/reset${envQ("?")}`, {
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
export const getGaps = () => fetch(`/gaps${envQ("?")}`).then(json);

// The model's stored property bag (library.metadata): write_mode, tags,
// description, bounds, batching, columns, … Fetched lazily on ⓘ hover.
export const getModelMeta = (name) =>
  fetch(`/model/${encodeURIComponent(name)}${envQ("?")}`).then(json);

// Models matching a bollhav tag expression (the `[group]` syntax). Returns
// `[{name, tags}]` (tags = the model's own tags that matched, for highlighting),
// or null on a malformed expression (backend 400).
export const getMatch = (expr) =>
  fetch(`/match?expr=${encodeURIComponent(expr)}${envQ("&")}`).then((r) =>
    r.ok ? r.json().then((d) => d.models) : null,
  );
