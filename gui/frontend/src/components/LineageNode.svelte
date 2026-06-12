<script>
  import { Handle, Position } from "@xyflow/svelte";
  import { selection } from "../lib/selection.svelte.js";
  import { KIND_COLOR, SRC_COLOR } from "../lib/constants.js";

  let { data } = $props();

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
  let accent = $derived(
    isModel ? KIND_COLOR[data.kind] || "#888" : SRC_COLOR[data.kind] || "#888",
  );

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
  <span class="kind-label" style="background:{accent};color:{textOn(accent)}">
    {data.kind}
  </span>
  {#if data.hasError}
    <span class="err-dot" title="unresolved error on a recent run"></span>
  {/if}
  {#if data.running}
    <span class="run-dot" title="a run is in progress"></span>
  {/if}
  <div class="name">{data.name}</div>
  {#if isModel}
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
  /* kind tag, popped out of the top-left corner (mirrors the err-dot) */
  .kind-label {
    position: absolute;
    top: -9px;
    left: -6px;
    font-size: 10px;
    font-weight: 600;
    padding: 1px 7px;
    border-radius: 10px;
    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.25);
    white-space: nowrap;
  }
  .err-dot {
    position: absolute;
    top: -5px;
    right: -5px;
    width: 11px;
    height: 11px;
    border-radius: 50%;
    background: #ff2d3a;
    border: 2px solid var(--node-dot-border);
    box-shadow:
      0 0 6px 2px rgba(229, 32, 46, 0.9),
      0 0 14px 4px rgba(229, 32, 46, 0.55);
    animation: err-pulse 1.4s ease-in-out infinite;
  }
  @keyframes err-pulse {
    0%,
    100% {
      box-shadow:
        0 0 6px 2px rgba(229, 32, 46, 0.9),
        0 0 14px 4px rgba(229, 32, 46, 0.5);
    }
    50% {
      box-shadow:
        0 0 10px 3px rgba(229, 32, 46, 1),
        0 0 26px 8px rgba(229, 32, 46, 0.7);
    }
  }
  .run-dot {
    position: absolute;
    bottom: -5px;
    right: -5px;
    width: 11px;
    height: 11px;
    border-radius: 50%;
    background: #2f8fff;
    border: 2px solid var(--node-dot-border);
    box-shadow:
      0 0 6px 2px rgba(47, 143, 255, 0.9),
      0 0 14px 4px rgba(47, 143, 255, 0.55);
    animation: run-pulse 1.4s ease-in-out infinite;
  }
  @keyframes run-pulse {
    0%,
    100% {
      box-shadow:
        0 0 6px 2px rgba(47, 143, 255, 0.9),
        0 0 14px 4px rgba(47, 143, 255, 0.5);
    }
    50% {
      box-shadow:
        0 0 10px 3px rgba(47, 143, 255, 1),
        0 0 26px 8px rgba(47, 143, 255, 0.7);
    }
  }
  /* respect users who ask for less motion — keep the strong glow, drop the pulse */
  @media (prefers-reduced-motion: reduce) {
    .err-dot,
    .run-dot {
      animation: none;
    }
  }
  .card.model {
    border-radius: 6px;
  }
  /* non-managed external sources: diagonal striped fill + dashed border */
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
  .name {
    font-size: 13px;
    font-weight: 600;
    color: var(--node-fg);
    margin-bottom: 5px;
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
