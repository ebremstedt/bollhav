// Shared, framework-free constants for the lineage GUI.

// Node sizing fed to dagre + Svelte Flow.
// sized for the 16px names on the cards (LineageNode .name)
export const NODE_W = 390;
export const NODE_H = 108;

// Managed models get a warm yellow outline — one colour, regardless of
// temporality. The `model` pill uses this; the temporality (temporal /
// timeless) is shown as a separate badge (see TEMPORALITY_COLOR).
export const MODEL_YELLOW = "#FFD23F";

// The `unmanaged` pill is neutral grey — a source isn't managed by bollhav, so
// it stays understated (vs the gold `model` pill on managed nodes).
export const UNMANAGED_GREY = "#9aa0a6";

// A model's temporality badge: does its work reference time?
export const TEMPORALITY_COLOR = {
  temporal: "#43A047", // green — windowed / time-aware
  timeless: "#1B5E20", // dark green — whole-table, no time axis
};

// A model's materialization badge: a stored table vs a SQL view. Keyed by the
// library `model_type` value; absent/unknown shows no pill.
export const MATERIALIZATION_COLOR = {
  TABLE: "#1B5E20", // dark green — a materialized table
  VIEW: "#66BB6A", // light green — a SQL view
};

// External (unmanaged) source kinds — the second pill on a source node.
// `model` deliberately shares the managed-model yellow: a "model" reads the
// same whether it's managed or an unmanaged relational source.
export const SRC_COLOR = {
  api: "#F58518", // orange
  file: "#9D755D", // brown
  model: MODEL_YELLOW, // yellow — same as a managed model
};

// Gaps tab — the backfill timeline colours. `covered` = applied / loaded
// (a temporal interval that ran, or a timeless whole-table model that loaded);
// `gap` = not filled yet (a temporal hole, or a timeless model not yet loaded).
// Timeless models share these colours and are marked with a `timeless` badge
// instead of a colour of their own. Shared by GapsView + the bottom Legend.
export const GAP_COLORS = {
  covered: "#2e7d32", // a calm dark green (the app's active green)
  gap: "#8b2332", // wine red — missing, still to be backfilled
};

// status dot colours in the side-panel runs table
export const STATUS_COLOR = {
  applied: "#54A24B",
  pending: "#888",
  running: "#4C78A8",
  blocked: "#F58518",
  error: "#E45756",
};

// "2025-01-01T12:34:56.789" -> "2025-01-01 12:34:56"
export const ts = (s) => (s ? s.replace("T", " ").slice(0, 19) : "—");

// legend (bottom bar) — each item has a hover tooltip explanation. Colours
// come from the constants above so the legend never drifts from the nodes.
export const LEGEND_KINDS = [
  {
    c: MODEL_YELLOW,
    label: "model",
    tip: "A bollhav-managed model — state-tracked, gated, yellow outline. The pill says `model`. (Temporal vs timeless is on the node's hover tooltip.)",
  },
  {
    c: MATERIALIZATION_COLOR.TABLE,
    label: "table",
    tip: "Materialization: the model is stored as a table.",
  },
  {
    c: MATERIALIZATION_COLOR.VIEW,
    label: "view",
    tip: "Materialization: the model is a SQL view (CREATE OR REPLACE VIEW).",
  },
];

export const LEGEND_SOURCES = [
  { c: SRC_COLOR.api, label: "api", tip: "Unmanaged external source read over a REST / HTTP API." },
  { c: SRC_COLOR.file, label: "file", tip: "Unmanaged external file input (CSV / Parquet / JSON, local or object storage)." },
  { c: SRC_COLOR.model, label: "model", tip: "Unmanaged external relational table / view read as a source (not managed by bollhav)." },
];
