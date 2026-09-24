// Shared runes state for the lineage graph. The full graph is kept in memory
// so we can re-filter (focus a model + its upstreams) without refetching.
// Header writes `query`; Flow reads the laid-out `nodes`/`edges`.
import { getGraph, getMatch, getEnvironments, getConfig, setApiEnv } from "./api.js";
import {
  layout,
  toFlow,
  upstreamClosure,
  namesClosure,
  subgraphOf,
} from "./graph.js";

export const view = $state({
  title: "model explorer", // the site's name: the page title and the header's left
  writable: true, // false when the deployment is read-only (no state resets)
  tab: "gaps", // "gaps" (the default), "models", "runs", "grid" or "lineage" (the graph)
  full: null, // raw /graph response
  // time filters (shared by the runs + grid tabs, applied client-side):
  loadedMode: "exact", // "exact" (prefix match) | "range" (from/to)
  loadedExact: "", // prefix-match on the displayed load time ("2026-06-15 18:15")
  loadedFrom: "", // ISO-ish — when the run was applied/logged (range start)
  loadedTo: "",
  intervalMode: "any", // "any" | "whole" (whole-table only) | "range" (windowed in [from,to])
  intervalFrom: "", // date — the data window (since/until), used when mode = "range"
  intervalTo: "",
  query: "", // name search box text
  focused: null, // model we've narrowed to by name, or null for "show all"
  tagExpr: "", // tag-expression search box text
  tagApplied: "", // the expression currently narrowing the graph ("" = none)
  tagMatches: null, // model names matching the tag expression (null = inactive)
  tagHighlights: {}, // name -> the model's matched tags (server-computed)
  matFilter: "all", // site-wide materialization filter: "all" | "TABLE" | "VIEW"
  hideUpstreams: true, // when focusing/filtering, drop the upstream closure (the default)
  environments: [], // [{schema,label}] — bollhav library schemas in the DB
  env: null, // selected env schema (null = prod z_bollhav)
  nodes: [],
  edges: [],
  refreshAt: 0, // bumped on refresh() so open panels reload too
  gapScore: null, // gaps tab: mean backfill % across models (a project score),
  // computed + set by GapsView, read by the bottom Legend
  gapScored: 0, // how many models the score averaged over
  recenterAt: 0, // bumped by the sub-bar "recenter" button; Flow re-fits the view
  refreshing: false, // true while a refresh() fetch is in flight (spinner)
  canRefresh: true, // false during the fetch + 5s cooldown (button disabled)
  cooldown: 0, // seconds left on the post-refresh cooldown (5..1, then 0)
  // site-wide name presentation: "lengthen" (catalog.schema.table on one line)
  // or "thicken" (one dotted segment per line, the default). Read by
  // LineageNode; layout sizes nodes taller in thicken so names don't overlap.
  nameStyle: "thicken",
  // detail level — a bare tree vs a decorated one:
  //   "lappland"   → just boxes + names + arrows (bare)
  //   "stockholm"  → everything: pills + status lights + runs/errors buttons
  //                  + contract/freshness edge labels
  // LineageNode reads it for node decoration; toFlow uses it for edge labels.
  detail: "stockholm",
});

// The dependency arrows animate (marching dashes) when the shown graph has at
// most this many models; above it they are static, since the animation is
// costly on big graphs.
const ANIMATE_MAX_MODELS = 10;

// Is a model let through by the materialization filter? (non-models always are)
export function matOk(node) {
  return (
    view.matFilter === "all" ||
    node.type !== "model" ||
    (node.model_type || "").toUpperCase() === view.matFilter
  );
}

// The graph minus the models the materialization filter drops (and the edges
// that touched them).
function applyMat(graph) {
  if (view.matFilter === "all") return graph;
  const keep = new Set(graph.nodes.filter(matOk).map((n) => n.name));
  return {
    ...graph,
    nodes: graph.nodes.filter((n) => keep.has(n.name)),
    edges: graph.edges.filter((e) => keep.has(e.from) && keep.has(e.to)),
  };
}

// Switch the materialization filter and re-render; a focused model that the
// filter drops is swapped for a random one that passes.
export function setMatFilter(v) {
  view.matFilter = v;
  const focusedNode = view.full?.nodes.find((n) => n.name === view.focused);
  if (focusedNode && !matOk(focusedNode)) focusRandom();
  rerender();
}

function render(graph) {
  graph = applyMat(graph);
  const models = graph.nodes.filter((n) => n.type === "model").length;
  const animated = models <= ANIMATE_MAX_MODELS;
  const { nodes, edges } = toFlow(graph, view.detail, view.tagHighlights, animated);
  view.nodes = layout(nodes, edges, view.nameStyle);
  view.edges = edges;
}

// The current narrowing of the full graph: a name focus (model + its upstreams),
// a tag-expression match (matched models + their upstreams), or everything.
// Name focus and tag filter are mutually exclusive (setting one clears the other).
function selected() {
  if (view.focused) {
    return view.hideUpstreams
      ? subgraphOf(view.full, [view.focused])
      : upstreamClosure(view.full, view.focused);
  }
  if (view.tagMatches) {
    return view.hideUpstreams
      ? subgraphOf(view.full, view.tagMatches)
      : namesClosure(view.full, view.tagMatches);
  }
  return view.full;
}

function rerender() {
  if (view.full) render(selected());
}

// Toggle whether a focus / tag filter drags in the upstream closure.
export function setHideUpstreams(v) {
  view.hideUpstreams = v;
  rerender();
}

// Set the detail level and re-render (edge labels depend on it; node decoration
// is read live from `view.detail` by LineageNode). Keeps the current narrowing.
export function setDetail(level) {
  view.detail = level;
  rerender();
}

// Flip the site-wide name presentation and re-lay-out so node spacing tracks
// the new (taller, in thicken) node heights. Keeps the current narrowing.
export function setNameStyle(style) {
  view.nameStyle = style;
  rerender();
}

export async function loadGraph() {
  view.full = await getGraph();
  // re-narrow to the active focus / tag filter so refresh keeps the view;
  // with neither (e.g. right after an env switch) land on a random model
  if (!view.focused && !view.tagMatches) focusRandom();
  rerender();
}

// Fetch the list of environments (library schemas) in the connected DB.
export async function loadEnvironments() {
  try {
    view.environments = await getEnvironments();
  } catch {
    view.environments = [];
  }
}

// Initial load: discover environments, then draw the (prod) graph.
// `restored` is what restoreUrl() read from a shared link: the focused model
// and / or the tag filter, which need the graph before they can apply.
export async function init(restored = {}) {
  await loadEnvironments();
  let cfg = {};
  try {
    cfg = (await getConfig()) || {};
  } catch {
    cfg = {};
  }
  // the site's name — env-var configurable on the backend (LINEAGE_TITLE)
  if (cfg.title) view.title = cfg.title;
  view.writable = cfg.writable !== false;
  document.title = view.title;
  view.full = await getGraph();
  // Narrow the graph BEFORE the first render so slow clients never lay out the
  // whole DAG. A shared link's filter or model wins; else a deployment's
  // `default_tags` (env var); else one random model.
  let prefiltered = false;
  if (restored.filter) prefiltered = await applyTags(restored.filter);
  if (!prefiltered && restored.model) {
    const node = view.full.nodes.find((n) => n.name === restored.model);
    if (node && matOk(node)) {
      view.query = node.name;
      view.focused = node.name;
      prefiltered = true;
    }
  }
  if (!prefiltered && cfg.default_tags) prefiltered = await applyTags(cfg.default_tags);
  if (!prefiltered) focusRandom();
  rerender();
}

// Make a tag expression the narrowing (its matches + their highlights);
// false when it's malformed or matches nothing, so the caller can fall back.
async function applyTags(expr) {
  const models = await getMatch(expr);
  if (!models || !models.length) return false;
  view.tagExpr = expr;
  view.tagApplied = expr;
  view.tagMatches = models.map((m) => m.name);
  view.tagHighlights = Object.fromEntries(models.map((m) => [m.name, m.tags]));
  return true;
}

// Narrow to one random model (+ its upstreams). The whole DAG is slow to lay
// out, so this is where the graph lands whenever nothing else narrows it:
// on first load, and again when a filter is cleared.
function focusRandom() {
  const models = view.full
    ? view.full.nodes.filter((n) => n.type === "model" && matOk(n))
    : [];
  if (!models.length) {
    // nothing passes the materialization filter: show nothing rather than
    // keep a focus the filter would drop (a reload of the link agrees)
    view.query = "";
    view.focused = null;
    return;
  }
  const pick = models[Math.floor(Math.random() * models.length)];
  view.query = pick.name;
  view.focused = pick.name;
}

// Switch the active environment (a library schema). Clears any focus / tag
// filter (they're env-specific) and reloads the graph for the new env.
export async function setEnv(schema) {
  view.env = schema || null;
  setApiEnv(view.env);
  view.query = "";
  view.focused = null;
  view.tagExpr = "";
  view.tagApplied = "";
  view.tagMatches = null;
  view.tagHighlights = {};
  clearTime();
  await loadGraph();
}

// Re-fetch the graph (updated run / error badges) and signal open side panels
// to reload their runs + errors. Rate-limited to once per 5s; the spinner runs
// only while the fetch is in flight, the button stays disabled for the cooldown.
export async function refresh() {
  if (!view.canRefresh) return;
  view.canRefresh = false;
  view.refreshing = true;
  view.refreshAt++;
  try {
    // hold the spinner ~0.8s so the click clearly registers before the
    // success state, even though the localhost fetch is near-instant
    await Promise.all([loadGraph(), new Promise((r) => setTimeout(r, 800))]);
  } finally {
    view.refreshing = false;
    // green "done" + 5..1 countdown, re-enabling at 0
    view.cooldown = 5;
    const tick = setInterval(() => {
      view.cooldown -= 1;
      if (view.cooldown <= 0) {
        clearInterval(tick);
        view.cooldown = 0;
        view.canRefresh = true;
      }
    }, 1000);
  }
}

// Narrow to an exact model name + its upstream closure. Partial / unmatched
// typing leaves the current view untouched (only fires on an exact match,
// e.g. one picked from the datalist).
export function applyFilter() {
  if (!view.full) return;
  const t = view.query.trim();
  if (!t) {
    view.focused = null;
    rerender();
    return;
  }
  if (view.full.nodes.some((n) => n.name === t)) {
    // name focus and tag filter are mutually exclusive
    view.tagExpr = "";
    view.tagApplied = "";
    view.tagMatches = null;
    view.tagHighlights = {};
    view.focused = t;
    rerender();
  }
}

// Narrow to the models matching a bollhav tag expression (the `[group]` syntax,
// evaluated server-side via /match) plus their upstream closure. An empty box
// clears it; a malformed expression (backend 400 → null) leaves the view as-is.
export async function applyTagFilter() {
  if (!view.full) return;
  const expr = view.tagExpr.trim();
  if (!expr) {
    view.tagApplied = "";
    view.tagMatches = null;
    view.tagHighlights = {};
    focusRandom();
    rerender();
    return;
  }
  const models = await getMatch(expr); // [{name, tags}] or null
  if (models == null) return; // malformed expression — keep current view
  // tag filter and name focus are mutually exclusive
  view.query = "";
  view.focused = null;
  view.tagApplied = expr;
  view.tagMatches = models.map((m) => m.name);
  view.tagHighlights = Object.fromEntries(models.map((m) => [m.name, m.tags]));
  rerender();
}

// Switch between the lineage graph, runs log, and grid.
export function setTab(tab) {
  view.tab = tab;
}

// Re-frame the graph. LineageBar lives outside the SvelteFlow context, so it
// can't call fitView directly — it bumps this counter and a helper component
// inside <SvelteFlow> (Recenter.svelte) reacts by calling fitView.
export function recenter() {
  view.recenterAt++;
}

// True if a row (with a load time and an optional since/until window) passes
// the active time filters. Used by the runs + grid tabs.
// parse a user-typed bound — accepts a date or a date + time, with either a
// space or a "T" between them ("2026-06-14", "2026-06-14 13:45", "…T13:45:30").
// NaN for blank/partial input, so any comparison against it is false (no filter).
function bound(s) {
  return Date.parse((s || "").trim().replace(" ", "T"));
}

export function passesTime(loadedAt, since, until) {
  // when it was loaded (applied/logged): either an exact prefix match on the
  // displayed time ("2026-06-15" matches the whole day) or a from/to range.
  if (view.loadedMode === "exact") {
    if (view.loadedExact) {
      if (!loadedAt) return false;
      const shown = loadedAt.replace("T", " ").slice(0, 19);
      if (!shown.startsWith(view.loadedExact.trim())) return false;
    }
  } else if (view.loadedFrom || view.loadedTo) {
    if (!loadedAt) return false;
    const t = Date.parse(loadedAt);
    if (view.loadedFrom && t < bound(view.loadedFrom)) return false;
    if (view.loadedTo && t > bound(view.loadedTo)) return false;
  }
  // what was loaded — whole-table only, a windowed range, or anything
  const whole = !since;
  if (view.intervalMode === "whole") return whole;
  if (view.intervalMode === "range") {
    if (whole) return false;
    if (view.intervalFrom && Date.parse(until) < bound(view.intervalFrom))
      return false;
    if (view.intervalTo && Date.parse(since) > bound(view.intervalTo))
      return false;
  }
  return true;
}

// reset the time filters
export function clearTime() {
  view.loadedMode = "exact";
  view.loadedExact = "";
  view.loadedFrom = "";
  view.loadedTo = "";
  view.intervalMode = "any";
  view.intervalFrom = "";
  view.intervalTo = "";
}

export function clearFocus() {
  view.tagExpr = "";
  view.tagApplied = "";
  view.tagMatches = null;
  view.tagHighlights = {};
  focusRandom();
  rerender();
}
