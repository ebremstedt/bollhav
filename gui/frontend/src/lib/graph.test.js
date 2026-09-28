import { describe, it, expect } from "vitest";
import {
  upstreamClosure,
  namesClosure,
  subgraphOf,
  nameSegments,
  freshnessText,
  toFlow,
  layout,
} from "./graph.js";

// fixture: src ──▶ a ──▶ b
//                  └────▶ c     (a feeds both b and c; src is an unmanaged source)
const graph = {
  nodes: [
    { name: "src", type: "external", kind: "model" },
    {
      name: "a",
      type: "model",
      kind: "temporal",
      model_type: "TABLE",
      upstream: ["src"],
      sources: [],
      tags: ["a"],
    },
    { name: "b", type: "model", kind: "timeless", model_type: "VIEW", upstream: ["a"] },
    { name: "c", type: "model", kind: "temporal", upstream: ["a"] },
  ],
  edges: [
    { from: "src", to: "a", relation: "source", kind: "model" },
    {
      from: "a",
      to: "b",
      relation: "upstream",
      contract: "whole",
      freshness: { within_seconds: 86400, scope: "latest" },
    },
    { from: "a", to: "c", relation: "upstream", contract: "window", freshness: null },
  ],
};

const names = (sub) => new Set(sub.nodes.map((n) => n.name));
const edgeKeys = (sub) => sub.edges.map((e) => `${e.from}->${e.to}`).sort();

describe("upstreamClosure", () => {
  it("keeps the target plus everything upstream of it", () => {
    const r = upstreamClosure(graph, "b");
    expect(names(r)).toEqual(new Set(["b", "a", "src"]));
    expect(edgeKeys(r)).toEqual(["a->b", "src->a"]);
  });

  it("excludes siblings that aren't upstream of the target", () => {
    const r = upstreamClosure(graph, "b");
    expect(names(r).has("c")).toBe(false); // c is downstream-sibling, not kept
  });
});

describe("namesClosure", () => {
  it("is a multi-seed upstream closure", () => {
    const r = namesClosure(graph, ["b", "c"]);
    expect(names(r)).toEqual(new Set(["b", "c", "a", "src"]));
    expect(edgeKeys(r)).toEqual(["a->b", "a->c", "src->a"]);
  });

  it("ignores names not in the graph", () => {
    expect(namesClosure(graph, ["ghost"]).nodes).toEqual([]);
  });
});

describe("subgraphOf", () => {
  it("keeps only the named nodes and edges among them (no upstream expansion)", () => {
    const r = subgraphOf(graph, ["a", "b"]);
    expect(names(r)).toEqual(new Set(["a", "b"]));
    expect(edgeKeys(r)).toEqual(["a->b"]); // src NOT pulled in, src->a dropped
  });
});

describe("freshnessText", () => {
  it("returns '' when there's no freshness", () => {
    expect(freshnessText({ freshness: null })).toBe("");
    expect(freshnessText({})).toBe("");
  });
  it("formats days / hours / minutes", () => {
    expect(freshnessText({ freshness: { within_seconds: 86400, scope: "latest" } })).toBe("1d");
    expect(freshnessText({ freshness: { within_seconds: 21600, scope: "latest" } })).toBe("6h");
    expect(freshnessText({ freshness: { within_seconds: 1800, scope: "latest" } })).toBe("30m");
  });
  it("appends ' all' only for the ALL scope", () => {
    expect(freshnessText({ freshness: { within_seconds: 604800, scope: "all" } })).toBe("7d all");
  });
});

describe("toFlow", () => {
  it("maps nodes and tags only matched names with matchTokens", () => {
    const { nodes } = toFlow(graph, "stockholm", { a: ["a"] });
    const a = nodes.find((n) => n.id === "a");
    const b = nodes.find((n) => n.id === "b");
    expect(a.type).toBe("model");
    expect(a.data.matchTokens).toEqual(["a"]);
    expect(b.data.matchTokens).toEqual([]); // not in the highlights map
  });

  it("stockholm: upstream edges become contract edges with a two-tone label", () => {
    const { edges } = toFlow(graph, "stockholm");
    const ab = edges.find((e) => e.source === "a" && e.target === "b");
    expect(ab.type).toBe("contract");
    expect(ab.data).toEqual({ contract: "whole", freshness: "1d" });
    const src = edges.find((e) => e.source === "src");
    expect(src.type).toBeUndefined(); // source edges stay bare/default
  });

  it("lappland: no contract labels on any edge", () => {
    const { edges } = toFlow(graph, "lappland");
    expect(edges.every((e) => e.type === undefined)).toBe(true);
  });
});

describe("nameSegments", () => {
  const NAME =
    "AnalyticsLakehouse.curated_clean_entities.CustomerInteractionEngagementEventFact";
  const hits = (segs) => segs.filter((s) => s.hit).map((s) => s.text);

  it("highlights a whole dot-segment (the schema)", () => {
    expect(hits(nameSegments(NAME, ["curated_clean_entities"]))).toEqual([
      "curated_clean_entities",
    ]);
  });

  it("highlights a single snake part", () => {
    expect(hits(nameSegments(NAME, ["clean"]))).toEqual(["clean"]);
  });

  it("highlights a single PascalCase word", () => {
    expect(hits(nameSegments(NAME, ["fact"]))).toEqual(["Fact"]);
  });

  it("is case-insensitive", () => {
    expect(hits(nameSegments("AnalyticsLakehouse", ["LAKEHOUSE"]))).toEqual(["Lakehouse"]);
  });

  it("highlights multiple matches across segments", () => {
    expect(hits(nameSegments(NAME, ["clean", "fact"]))).toEqual(["clean", "Fact"]);
  });

  it("preserves the full name exactly (no characters lost)", () => {
    const segs = nameSegments(NAME, ["clean", "fact"], false);
    expect(segs.map((s) => s.text).join("")).toBe(NAME);
  });

  it("stacks one dotted segment per line in thicken mode", () => {
    expect(nameSegments("a.b.c", [], true).map((s) => s.text).join("")).toBe("a.\nb.\nc");
  });

  it("marks nothing when no tokens match", () => {
    expect(hits(nameSegments(NAME, ["nope"]))).toEqual([]);
  });
});

describe("layout", () => {
  it("assigns finite numeric positions to every node", () => {
    const { nodes, edges } = toFlow(graph, "lappland");
    const laid = layout(nodes, edges, "lengthen");
    for (const n of laid) {
      expect(Number.isFinite(n.position.x)).toBe(true);
      expect(Number.isFinite(n.position.y)).toBe(true);
    }
  });
});
