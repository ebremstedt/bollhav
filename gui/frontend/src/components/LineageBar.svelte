<script>
  import {
    view,
    setDetail,
    setHideUpstreams,
    setAnimateEdges,
  } from "../lib/view.svelte.js";

  // detail-level radio: a bare tree (names only) vs a decorated one (everything).
  const DETAILS = [
    ["lappland", "🌲", "bare — just boxes, names, and arrows"],
    [
      "stockholm",
      "🎄",
      "everything — pills, status lights, runs/errors buttons, and contract labels",
    ],
  ];
</script>

<div class="subbar">
  <span
    class="tip-wrap"
    data-tip="Detail level (the christmas tree) — how much decoration to show on the graph."
  >
    <span class="seg">
      {#each DETAILS as [val, emoji, tip]}
        <button
          class="seg-btn"
          class:active={view.detail === val}
          data-tip={tip}
          title={val}
          onclick={() => setDetail(val)}>{emoji}</button
        >
      {/each}
    </span>
  </span>
  <span
    class="tip-wrap"
    data-tip="When focusing a model or tag-filtering, include the upstream chain or show only the matched models."
  >
    <button class="toggle" onclick={() => setHideUpstreams(!view.hideUpstreams)}>
      {view.hideUpstreams ? "upstreams: off" : "upstreams: on"}
    </button>
  </span>
  <span
    class="tip-wrap"
    data-tip="Animate the dependency arrows (marching dashes). Off by default — it's heavy on slow machines."
  >
    <button
      class="toggle"
      class:on={view.animateEdges}
      onclick={() => setAnimateEdges(!view.animateEdges)}
    >
      {view.animateEdges ? "arrows: animated" : "arrows: static"}
    </button>
  </span>
</div>

<style>
  .subbar {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 8px 16px;
    border-bottom: 1px solid var(--border);
    background: var(--bg);
    color: var(--fg);
  }
  .toggle {
    font-size: 12px;
    padding: 4px 10px;
    border-radius: 6px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--control-fg);
    cursor: pointer;
  }
  .toggle.on {
    background: #2e7d32;
    border-color: #2e7d32;
    color: #fff;
  }
  .seg {
    display: inline-flex;
    align-items: center;
    gap: 2px;
    border: 1px solid var(--control-border);
    border-radius: 6px;
    background: var(--control-bg);
    padding: 2px;
  }
  .seg-btn {
    font-size: 12px;
    padding: 3px 9px;
    border: none;
    border-radius: 4px;
    background: transparent;
    color: var(--control-fg);
    cursor: pointer;
  }
  .seg-btn.active {
    background: #2e7d32;
    color: #fff;
  }
  .tip-wrap {
    position: relative;
    display: inline-flex;
  }
  .tip-wrap:hover::after {
    content: attr(data-tip);
    position: absolute;
    top: calc(100% + 9px);
    left: 0;
    width: 230px;
    white-space: normal;
    background: #222;
    color: #fff;
    font-size: 12px;
    line-height: 1.35;
    padding: 7px 9px;
    border-radius: 6px;
    box-shadow: 0 4px 14px rgba(0, 0, 0, 0.3);
    z-index: 50;
    pointer-events: none;
  }
  .tip-wrap:hover::before {
    content: "";
    position: absolute;
    top: calc(100% + 3px);
    left: 14px;
    border: 6px solid transparent;
    border-bottom-color: #222;
    z-index: 50;
    pointer-events: none;
  }
</style>
