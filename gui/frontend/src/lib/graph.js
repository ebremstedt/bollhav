// Pure graph transforms: backend /graph shape <-> Svelte Flow {nodes, edges},
// dagre layout, and the upstream-closure filter. No Svelte, no fetch — easy to
// reason about and unit-test in isolation.
import { MarkerType } from "@xyflow/svelte";
import dagre from "@dagrejs/dagre";
import { NODE_W, NODE_H } from "./constants.js";

// Extra pixels per stacked name line, used to grow node height in "thicken"
// mode so dagre reserves vertical room and stacked names don't overlap.
const THICKEN_LINE_H = 30; // one 24px name line, with its line-height

// Height a node occupies for layout — taller in "thicken", by one extra line
// per dotted segment beyond the first (a 3-part catalog.schema.table → +2).
function nodeHeight(node, nameStyle) {
  if (nameStyle !== "thicken") return NODE_H;
  const segments = String(node.data?.name ?? "").split(".").length;
  return NODE_H + Math.max(0, segments - 1) * THICKEN_LINE_H;
}

// Position every node with dagre (left -> right DAG layout). Mutates each
// node's `position` in place and returns the same array.
export function layout(nodes, edges, nameStyle = "lengthen") {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: "LR", nodesep: 68, ranksep: 290 });
  const heights = new Map(nodes.map((n) => [n.id, nodeHeight(n, nameStyle)]));
  nodes.forEach((n) =>
    g.setNode(n.id, { width: NODE_W, height: heights.get(n.id) }),
  );
  edges.forEach((e) => g.setEdge(e.source, e.target));
  dagre.layout(g);
  nodes.forEach((n) => {
    const p = g.node(n.id);
    n.position = { x: p.x - NODE_W / 2, y: p.y - heights.get(n.id) / 2 };
  });
  return nodes;
}

// Compact human duration for a freshness window (seconds -> "1d" / "6h" / "30m").
function fmtDur(secs) {
  if (secs == null) return "";
  const d = secs / 86400;
  if (d >= 1) return `${+d.toFixed(d % 1 ? 1 : 0)}d`;
  const h = secs / 3600;
  if (h >= 1) return `${+h.toFixed(h % 1 ? 1 : 0)}h`;
  const m = secs / 60;
  if (m >= 1) return `${+m.toFixed(0)}m`;
  return `${secs}s`;
}

// Split a model name into {text, hit} segments, marking the parts that match
// `tokens` (the model's server-confirmed matched tags). Matches hierarchically
// so a token can be a whole dot-segment ("curated_clean_entities"), a snake part
// ("clean"), or a PascalCase/number word ("Fact") — mirroring bollhav's tag
// derivation. Case-insensitive. In `thicken` mode a "\n" is emitted after each
// dot so the name stacks one segment per line.
export function nameSegments(name, tokens, thicken = false) {
  const q = new Set([...tokens].map((t) => String(t).toLowerCase()));
  const segs = [];
  const hot = (w) => q.has(w.toLowerCase());
  const push = (text, hit) => {
    if (!text) return;
    const last = segs[segs.length - 1];
    if (last && last.hit === hit) last.text += text;
    else segs.push({ text, hit });
  };
  name.split(".").forEach((seg, i) => {
    if (i > 0) {
      push(".", false);
      if (thicken) push("\n", false);
    }
    if (hot(seg)) {
      push(seg, true); // whole catalog / schema / table segment matched
      return;
    }
    seg.split("_").forEach((p, j) => {
      if (j > 0) push("_", false);
      if (!p) return;
      if (hot(p)) {
        push(p, true); // whole snake part matched
        return;
      }
      const toks = p.match(/[A-Z][a-z]*|[a-z]+|[0-9]+/g);
      if (!toks || toks.join("") !== p) {
        push(p, hot(p));
        return;
      }
      for (const t of toks) push(t, hot(t)); // PascalCase / number words
    });
  });
  return segs;
}

// The freshness bound as compact text (e.g. "1d" or "7d all"), or "" if none.
export function freshnessText(e) {
  if (!e.freshness) return "";
  const within = fmtDur(e.freshness.within_seconds);
  return `${within}${e.freshness.scope === "all" ? " all" : ""}`;
}

// Map the backend /graph payload to Svelte Flow nodes + edges (unlaid-out).
// `detail` gates edge decoration: contract/freshness labels only show on "stockholm".
// `highlights` maps a node name → the model's tags that matched the active tag
// filter (server-computed). Those become `data.matchTokens` so the node can
// green-highlight just the matching part of its name.
export function toFlow(graph, detail = "stockholm", highlights = {}, animated = false) {
  const nodes = graph.nodes.map((n) => ({
    id: n.name,
    type: n.type, // "model" | "external"
    data: {
      name: n.name,
      matchTokens: highlights[n.name] || [],
      // For a model `kind` is its temporality (temporal / timeless); for an
      // external source it's the source kind (model / api / file).
      kind: n.kind,
      nodeType: n.type,
      modelType: n.model_type || null,
      upstream: n.upstream || [],
      sources: n.sources || [],
      lastSeen: n.last_seen || null,
      hasError: n.has_error || false,
      running: n.has_running || false,
      blocked: n.has_blocked || false,
      stale: n.has_stale || false,
    },
    position: { x: 0, y: 0 },
  }));
  // upstream edges carry a two-tone contract label (custom ContractEdge:
  // contract level grey, freshness blue); source edges stay bare/default.
  const edges = graph.edges.map((e, i) => {
    const base = {
      id: "e" + i,
      source: e.from,
      target: e.to,
      animated,
      // --edge is black in light mode, white in dark (theme.css); a CSS var
      // works for the arrowhead too because xyflow sets its colour inline.
      style: "stroke:var(--edge);stroke-width:1.5",
      markerEnd: { type: MarkerType.ArrowClosed, color: "var(--edge)" },
    };
    if (
      detail === "stockholm" &&
      e.relation === "upstream" &&
      (e.contract || e.freshness)
    ) {
      return {
        ...base,
        type: "contract",
        data: { contract: e.contract || "", freshness: freshnessText(e) },
      };
    }
    return base;
  });
  return { nodes, edges };
}

// Just the named nodes and the edges among them — no upstream expansion.
// Used when "hide upstreams" is on.
export function subgraphOf(graph, names) {
  const present = new Set(graph.nodes.map((n) => n.name));
  const keep = new Set(names.filter((n) => present.has(n)));
  return {
    nodes: graph.nodes.filter((n) => keep.has(n.name)),
    edges: graph.edges.filter((e) => keep.has(e.from) && keep.has(e.to)),
  };
}

// Every node that feeds into any of `names` (transitively), plus those nodes —
// a multi-seed upstream closure, used for the tag-expression filter.
export function namesClosure(graph, names) {
  const present = new Set(graph.nodes.map((n) => n.name));
  const keep = new Set(names.filter((n) => present.has(n)));
  let changed = true;
  while (changed) {
    changed = false;
    for (const e of graph.edges) {
      if (keep.has(e.to) && !keep.has(e.from)) {
        keep.add(e.from);
        changed = true;
      }
    }
  }
  return {
    nodes: graph.nodes.filter((n) => keep.has(n.name)),
    edges: graph.edges.filter((e) => keep.has(e.from) && keep.has(e.to)),
  };
}

// Every node that feeds into `target` (transitively), plus target itself.
export function upstreamClosure(graph, target) {
  const keep = new Set([target]);
  let changed = true;
  while (changed) {
    changed = false;
    for (const e of graph.edges) {
      if (keep.has(e.to) && !keep.has(e.from)) {
        keep.add(e.from);
        changed = true;
      }
    }
  }
  return {
    nodes: graph.nodes.filter((n) => keep.has(n.name)),
    edges: graph.edges.filter((e) => keep.has(e.from) && keep.has(e.to)),
  };
}
