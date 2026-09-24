// The address bar mirrors every choice the user makes, so a link reproduces
// the exact view for a colleague: tab, environment, the focused model or the
// tag filter, materialization, time window, the lineage's side panels, and
// each tab's sorting / display choices.
//
// `ui` holds the per-tab choices that used to be local component state (a
// component unmounts on a tab switch and would otherwise forget them). The
// rest lives in `view`, `selection` and `info`. urlQuery() serialises all of
// it, writing only what differs from the defaults so links stay short, and
// restoreUrl() reads it back on load.
//
// Keys:  tab · env · model · filter · mat · upstreams · detail
//        loaded | loaded_from loaded_to · interval interval_from interval_to
//        panel panel_tab panel_runs · info · theme
//        grid_sort grid_dir grid_cells grid_runs grid_models grid_row
//        runs_show runs_order runs_limit runs_name
//        gaps gaps_sort gaps_dir gaps_open
//        models_sort models_dir models_pane models_open
import { view } from "./view.svelte.js";
import { selection, info } from "./selection.svelte.js";
import { setApiEnv } from "./api.js";

export const ui = $state({
  ready: false, // set once restoreUrl() has run; until then nothing is written back
  dark: false,
  // grid tab
  gridSort: "catalog",
  gridDir: "asc",
  gridCells: "oldest", // "oldest" → newest on the right; "newest" → newest on the left
  gridRuns: "width", // "width" = as many runs as fit, 100, 365 or "all"
  gridModels: "height", // "height" = as many models as fit, or "all"
  gridRow: null, // the highlighted model row
  // runs tab
  runsShow: "errors", // "errors" | "runs" | "both"
  runsOrder: "desc",
  runsLimit: "height",
  runsName: "schema", // "fqn" | "schema" | "table" — how much of the model name to show
  // gaps tab
  gapsMode: "with", // "with" | "without" gaps
  gapsSort: "gap",
  gapsDir: "desc",
  gapsOpen: null, // the expanded model
  // models tab
  modelsSort: "table", // "full" | "schematable" | "table"
  modelsDir: "asc",
  modelsPane: "properties", // "properties" | "columns"
  modelsOpen: null, // the selected model
  // the lineage's side panel
  panelRuns: 100,
});

// internal value <-> what the URL says, where they differ
const RUNS_NAME = [
  ["fqn", "catalog"],
  ["schema", "schema"],
  ["table", "table"],
];
const MODELS_SORT = [
  ["full", "catalog"],
  ["schematable", "schema"],
  ["table", "table"],
];
const enc = (val, pairs) => (pairs.find(([v]) => v === val) || [val, val])[1];
const dec = (str, pairs, def) => (pairs.find(([, s]) => s === str) || [def])[0];

// one of the allowed strings, else the default
const oneOf = (v, allowed, def) => (allowed.includes(v) ? v : def);
// a count choice: a keyword ("all", "width", …) or one of the allowed numbers
const countOf = (v, allowed, def) => {
  if (v == null) return def;
  if (allowed.includes(v)) return v;
  const n = Number(v);
  return allowed.includes(n) ? n : def;
};

function put(p, key, val, def) {
  if (val != null && val !== "" && val !== def) p.set(key, String(val));
}

// The query string for the current state (no leading "?"; "" when everything
// is at its default).
export function urlQuery() {
  const p = new URLSearchParams();
  put(p, "tab", view.tab, "gaps");
  put(p, "env", view.env, null);
  // the narrowing: an applied tag expression, or the focused model
  put(p, "filter", view.tagApplied, "");
  put(p, "model", view.focused, null);
  put(p, "mat", view.matFilter.toLowerCase(), "all");
  put(p, "upstreams", view.hideUpstreams ? "hide" : "show", "hide");
  put(p, "detail", view.detail === "lappland" ? "simple" : "verbose", "verbose");
  // the time filter: when it ran (an exact prefix or a range) + what was loaded
  if (view.loadedMode === "range") {
    put(p, "loaded_from", view.loadedFrom, "");
    put(p, "loaded_to", view.loadedTo, "");
  } else {
    put(p, "loaded", view.loadedExact, "");
  }
  put(p, "interval", view.intervalMode, "any");
  if (view.intervalMode === "range") {
    put(p, "interval_from", view.intervalFrom, "");
    put(p, "interval_to", view.intervalTo, "");
  }
  // the lineage's panels
  put(p, "panel", selection.name, null);
  if (selection.name) {
    put(p, "panel_tab", selection.tab, "state");
    put(p, "panel_runs", ui.panelRuns, 100);
  }
  put(p, "info", info.name, null);
  // per tab
  put(p, "grid_sort", ui.gridSort, "catalog");
  put(p, "grid_dir", ui.gridDir, "asc");
  put(p, "grid_cells", ui.gridCells, "oldest");
  put(p, "grid_runs", ui.gridRuns, "width");
  put(p, "grid_models", ui.gridModels, "height");
  put(p, "grid_row", ui.gridRow, null);
  put(p, "runs_show", ui.runsShow, "errors");
  put(p, "runs_order", ui.runsOrder, "desc");
  put(p, "runs_limit", ui.runsLimit, "height");
  put(p, "runs_name", enc(ui.runsName, RUNS_NAME), "schema");
  put(p, "gaps", ui.gapsMode, "with");
  put(p, "gaps_sort", ui.gapsSort, "gap");
  put(p, "gaps_dir", ui.gapsDir, "desc");
  put(p, "gaps_open", ui.gapsOpen, null);
  put(p, "models_sort", enc(ui.modelsSort, MODELS_SORT), "table");
  put(p, "models_dir", ui.modelsDir, "asc");
  put(p, "models_pane", ui.modelsPane, "properties");
  put(p, "models_open", ui.modelsOpen, null);
  put(p, "theme", ui.dark ? "dark" : "light", "light");
  return p.toString();
}

// Read the address bar into the state, before anything is fetched (the env
// must be set first so every read hits the right schema). Returns the two
// choices that need the graph to be loaded before they can apply — the
// focused model and the tag filter — for init() to finish.
export function restoreUrl() {
  const p = new URLSearchParams(location.search);
  const g = (k) => p.get(k);
  view.tab = oneOf(g("tab"), ["gaps", "models", "runs", "grid", "lineage"], "gaps");
  view.env = g("env") || null;
  setApiEnv(view.env);
  const mat = oneOf(g("mat"), ["table", "view"], null);
  view.matFilter = mat ? mat.toUpperCase() : "all";
  view.hideUpstreams = g("upstreams") !== "show";
  view.detail = g("detail") === "simple" ? "lappland" : "stockholm";
  if (g("loaded_from") || g("loaded_to")) {
    view.loadedMode = "range";
    view.loadedFrom = g("loaded_from") || "";
    view.loadedTo = g("loaded_to") || "";
  } else if (g("loaded")) {
    view.loadedMode = "exact";
    view.loadedExact = g("loaded");
  }
  view.intervalMode = oneOf(g("interval"), ["any", "whole", "range"], "any");
  if (view.intervalMode === "range") {
    view.intervalFrom = g("interval_from") || "";
    view.intervalTo = g("interval_to") || "";
  }
  selection.name = g("panel") || null;
  selection.tab = oneOf(g("panel_tab"), ["state", "errors"], "state");
  ui.panelRuns = countOf(g("panel_runs"), [100, 365, "all"], 100);
  info.name = g("info") || null;
  ui.dark = g("theme") === "dark";
  ui.gridSort = oneOf(g("grid_sort"), ["catalog", "schema", "table"], "catalog");
  ui.gridDir = oneOf(g("grid_dir"), ["asc", "desc"], "asc");
  ui.gridCells = oneOf(g("grid_cells"), ["oldest", "newest"], "oldest");
  ui.gridRuns = countOf(g("grid_runs"), ["width", 100, 365, "all"], "width");
  ui.gridModels = oneOf(g("grid_models"), ["height", "all"], "height");
  ui.gridRow = g("grid_row") || null;
  ui.runsShow = oneOf(g("runs_show"), ["errors", "runs", "both"], "errors");
  ui.runsOrder = oneOf(g("runs_order"), ["desc", "asc"], "desc");
  ui.runsLimit = countOf(g("runs_limit"), ["height", 100, 365, "all"], "height");
  ui.runsName = dec(g("runs_name"), RUNS_NAME, "schema");
  ui.gapsMode = oneOf(g("gaps"), ["with", "without"], "with");
  ui.gapsSort = oneOf(g("gaps_sort"), ["gap", "catalog", "schema", "table"], "gap");
  ui.gapsDir = oneOf(g("gaps_dir"), ["asc", "desc"], "desc");
  ui.gapsOpen = g("gaps_open") || null;
  ui.modelsSort = dec(g("models_sort"), MODELS_SORT, "table");
  ui.modelsDir = oneOf(g("models_dir"), ["asc", "desc"], "asc");
  ui.modelsPane = oneOf(g("models_pane"), ["properties", "columns"], "properties");
  ui.modelsOpen = g("models_open") || null;
  ui.ready = true;
  return { model: g("model") || null, filter: g("filter") || "" };
}
