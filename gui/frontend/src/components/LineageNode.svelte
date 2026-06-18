<script>
  import { Handle, Position } from "@xyflow/svelte";
  import { selection, info } from "../lib/selection.svelte.js";
  import { view } from "../lib/view.svelte.js";
  import {
    MODEL_YELLOW,
    UNMANAGED_GREY,
    MATERIALIZATION_COLOR,
    SRC_COLOR,
  } from "../lib/constants.js";
  import { nameSegments } from "../lib/graph.js";

  let { data } = $props();

  // Site-wide name presentation. "thicken" stacks a dotted `catalog.schema.table`
  // name one segment per line (each non-final segment keeps its trailing dot);
  // "lengthen" leaves it on one line. Rendered with `white-space: pre-line`.
  let displayName = $derived(
    view.nameStyle === "thicken"
      ? data.name
          .split(".")
          .map((seg, i, arr) => (i < arr.length - 1 ? seg + "." : seg))
          .join("\n")
      : data.name,
  );

  let segs = $derived(
    data.matchTokens && data.matchTokens.length
      ? nameSegments(data.name, data.matchTokens, view.nameStyle === "thicken")
      : null,
  );

  // readable text colour (dark/white) for a solid label background — keeps the
  // light-green and yellow labels legible.
  function textOn(hex) {
    const c = hex.replace("#", "");
    const r = parseInt(c.slice(0, 2), 16);
    const g = parseInt(c.slice(2, 4), 16);
    const b = parseInt(c.slice(4, 6), 16);
    const luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255;
    return luminance > 0.62 ? "#1b1b1f" : "#fff";
  }

  let isModel = $derived(data.nodeType === "model");
  // Detail level: lappland = bare (names only); stockholm = everything (pills +
  // status lights + runs/errors buttons, and the edge labels gated in graph.js).
  let showPills = $derived(view.detail !== "lappland");
  let showDots = $derived(view.detail !== "lappland");
  let showActions = $derived(view.detail === "stockholm");
  // An unmanaged source's second pill colour reflects its kind (model/api/file).
  let kindColor = $derived(SRC_COLOR[data.kind] || "#888");
  // Outline: a solid yellow frame for a managed model; for an unmanaged source
  // the (dashed) border colour reflects WHICH kind of source it is.
  let accent = $derived(isModel ? MODEL_YELLOW : kindColor);
  // Materialization pill (table / view) — only when the library knows it.
  let matColor = $derived(MATERIALIZATION_COLOR[data.modelType] || null);

  // Open the LEFT metadata panel for this model (the ⓘ badge). Stop the click
  // bubbling so it doesn't also trigger the card's open() / right panel.
  function openInfo(e) {
    e?.stopPropagation();
    info.name = data.name;
  }

  function open() {
    if (isModel) show("state");
  }

  // open the side panel on a specific tab; stop the click bubbling to the
  // card's own open() so the button choice wins.
  function show(tab, e) {
    e?.stopPropagation();
    selection.name = data.name;
    selection.tab = tab;
  }
</script>

<Handle type="target" position={Position.Left} />

<div
  class="card"
  class:model={isModel}
  class:ext={!isModel}
  style="border-color:{accent};cursor:{isModel ? 'pointer' : 'default'}"
  onclick={open}
  role="button"
  tabindex="0"
  onkeydown={(e) => e.key === "Enter" && open()}
>
  {#if showPills}
  <div class="tag-row">
    {#if isModel}
      <span
        class="kind-label"
        style="background:{MODEL_YELLOW};color:{textOn(MODEL_YELLOW)}"
        title="managed model · {data.kind}">model</span
      >
      {#if matColor}
        <span
          class="kind-label"
          style="background:{matColor};color:{textOn(matColor)}"
          title="materialization — a stored table or a SQL view"
          >{data.modelType.toLowerCase()}</span
        >
      {/if}
      <button
        class="info"
        aria-label="model details"
        title="model details"
        onclick={openInfo}
      >i</button>
    {:else}
      <span
        class="kind-label"
        style="background:{UNMANAGED_GREY};color:{textOn(UNMANAGED_GREY)}"
        title="not managed by bollhav — external input">unmanaged</span
      >
      <span
        class="kind-label"
        style="background:{kindColor};color:{textOn(kindColor)}"
        title="source kind">{data.kind}</span
      >
    {/if}
  </div>
  {/if}
  <!-- status lights: a fixed strip anchored top-right, filling right→left, so
       a single light always sits at the same spot. Order here is the fill
       order (rightmost first). -->
  {#if showDots}
  <div class="dots">
    {#if data.hasError}
      <span class="dot err" title="error on a recent run"></span>
    {/if}
    {#if data.running}
      <span class="dot run" title="a run is in progress"></span>
    {/if}
    {#if data.blocked}
      <span
        class="dot block"
        title="blocked: an upstream model hasn't produced the data yet"
      ></span>
    {/if}
    {#if data.stale}
      <span
        class="dot stale"
        title="blocked: an upstream is present but too old (freshness gate not met)"
      ></span>
    {/if}
  </div>
  {/if}
  <div class="name">
    {#if segs}{#each segs as s}<span class:hit={s.hit}>{s.text}</span>{/each}{:else}{displayName}{/if}
  </div>
  {#if isModel && showActions}
    <div class="actions">
      <button class="mini runs" onclick={(e) => show("state", e)}>runs</button>
      <button class="mini errors" onclick={(e) => show("errors", e)}>errors</button>
    </div>
  {/if}
</div>

<Handle type="source" position={Position.Right} />

<style>
  .card {
    position: relative;
    min-width: 170px;
    padding: 11px 10px 8px;
    border: 2px solid #888;
    border-radius: 8px;
    background: var(--node-bg);
    box-shadow: var(--node-shadow);
    font-family: system-ui, sans-serif;
  }
  /* kind tag + info badge, popped out of the top-left corner */
  .tag-row {
    position: absolute;
    top: -9px;
    left: -6px;
    display: flex;
    align-items: center;
    gap: 4px;
    z-index: 2;
  }
  .kind-label {
    font-size: 10px;
    font-weight: 600;
    padding: 1px 7px;
    border-radius: 10px;
    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.25);
    white-space: nowrap;
  }
  /* circled "i" sitting just to the right of the kind pill */
  .info {
    width: 15px;
    height: 15px;
    padding: 0;
    line-height: 13px;
    font-size: 10px;
    font-weight: 700;
    font-style: italic;
    font-family: Georgia, "Times New Roman", serif;
    color: var(--node-fg);
    background: var(--node-bg);
    border: 1px solid var(--control-border);
    border-radius: 50%;
    cursor: help;
    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.25);
  }
  .info:hover {
    border-color: #2f8fff;
    color: #2f8fff;
  }
  /* status-light strip: anchored at the top-right, filling right→left
     (row-reverse), so one light sits at the same anchor as four. */
  .dots {
    position: absolute;
    top: -6px;
    right: -6px;
    display: flex;
    flex-direction: row-reverse;
    gap: 4px;
    z-index: 3;
  }
  .dot {
    width: 11px;
    height: 11px;
    border-radius: 50%;
    border: 2px solid var(--node-dot-border);
    /* static glow, tinted per-dot via the --glow RGB custom property (no pulse) */
    box-shadow:
      0 0 6px 2px rgba(var(--glow), 0.9),
      0 0 14px 4px rgba(var(--glow), 0.55);
  }
  .dot.err {
    background: #ff2d3a;
    --glow: 229, 32, 46;
  }
  .dot.run {
    background: #4ade80;
    --glow: 74, 222, 128;
  }
  .dot.block {
    background: #f58518;
    --glow: 245, 133, 24;
  }
  .dot.stale {
    background: #2f8fff;
    --glow: 47, 143, 255;
  }
  .card.model {
    border-radius: 6px;
  }
  /* non-managed external sources: diagonal striped fill + a DASHED border
     whose colour (set inline) reflects the source kind (api / file / model). */
  .card.ext {
    background: repeating-linear-gradient(
      45deg,
      var(--node-bg),
      var(--node-bg) 7px,
      var(--node-stripe) 7px,
      var(--node-stripe) 14px
    );
    border-style: dashed;
  }
  /* managed models: a touch heavier golden frame so they read as the
     first-class thing on the canvas. */
  .card.model {
    border-width: 2.5px;
  }
  .name {
    font-size: 13px;
    font-weight: 600;
    color: var(--node-fg);
    margin-bottom: 5px;
    white-space: pre-line;
    line-height: 1.25;
  }
  /* the part of a matched model's name that matched the tag query — green,
     legible on both dark and light backgrounds */
  .name .hit {
    color: #16a34a;
  }
  .actions {
    display: flex;
    align-items: center;
    gap: 5px;
  }
  .mini {
    font-size: 10px;
    padding: 1px 7px;
    border-radius: 9px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--node-fg);
    cursor: pointer;
  }
  .mini.runs {
    border-color: #2f8fff;
  }
  .mini.errors {
    border-color: #ff8a8a;
  }
  .mini:hover {
    background: var(--node-bg);
  }
</style>
