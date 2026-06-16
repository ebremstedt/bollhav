<script>
  import {
    view,
    applyFilter,
    applyTagFilter,
    clearFocus,
    refresh,
    setDetail,
    setHideUpstreams,
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

  let { dark = $bindable() } = $props();

  let modelNames = $derived(
    view.full
      ? view.full.nodes
          .filter((n) => n.type === "model")
          .map((n) => n.name)
          .sort()
      : [],
  );

  let allTags = $derived(
    view.full
      ? [...new Set(view.full.nodes.flatMap((n) => n.tags || []))].sort()
      : [],
  );
</script>

<header>
  <div class="left">
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
            onclick={() => setDetail(val)}>{emoji} {val}</button
          >
        {/each}
      </span>
    </span>
  </div>

  <strong class="brand">Lineage</strong>

  <div class="right">
    <input
      class="search"
      list="model-list"
      placeholder="find a model by name…"
      value={view.query}
      oninput={(e) => {
        view.query = e.currentTarget.value;
        applyFilter();
      }}
    />
    <datalist id="model-list">
      {#each modelNames as m}
        <option value={m}></option>
      {/each}
    </datalist>
    <input
      class="search tagsearch"
      list="tag-list"
      placeholder="tag or tagexpression — clean  ·  [(raw|clean)&amp;orbit]"
      value={view.tagExpr}
      oninput={(e) => (view.tagExpr = e.currentTarget.value)}
      onkeydown={(e) => e.key === "Enter" && applyTagFilter()}
    />
    <datalist id="tag-list">
      {#each allTags as t}
        <option value={t}></option>
      {/each}
    </datalist>
    <button class="toggle" onclick={applyTagFilter}>filter</button>
    <button class="clear" onclick={clearFocus}>✕ clear all</button>
    <span
      class="tip-wrap"
      data-tip="When focusing a model or tag-filtering, include the upstream chain or show only the matched models."
    >
      <button
        class="toggle"
        onclick={() => setHideUpstreams(!view.hideUpstreams)}
      >
        {view.hideUpstreams ? "⬆ upstreams: off" : "⬆ upstreams: on"}
      </button>
    </span>
    <span
      class="tip-wrap"
      data-tip="Reload the graph. Limited to once every 5 seconds."
    >
      <button class="toggle" onclick={refresh} disabled={!view.canRefresh}>
        {#if view.refreshing}
          <span class="ico spin">⟳</span> refreshing…
        {:else}
          <span class="ico">⟳</span> refresh{view.cooldown > 0
            ? ` (${view.cooldown})`
            : ""}
        {/if}
      </button>
    </span>
    <button class="toggle" onclick={() => (dark = !dark)}>
      {dark ? "☀ light" : "☾ dark"}
    </button>
  </div>
</header>

<style>
  header {
    padding: 10px 14px;
    border-bottom: 1px solid var(--border);
    font-size: 14px;
    display: flex;
    align-items: center;
    gap: 10px;
    background: var(--bg);
    color: var(--fg);
  }
  /* three header zones: christmas radio (left) · Lineage (centred) · controls
     (right). left/right flex equally so the brand sits dead centre. */
  .left,
  .right {
    flex: 1;
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .right {
    justify-content: flex-end;
  }
  .brand {
    flex: 0 0 auto;
    white-space: nowrap;
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
  /* christmas-tree detail radio: a segmented control */
  .seg {
    display: inline-flex;
    align-items: center;
    gap: 2px;
    border: 1px solid var(--control-border);
    border-radius: 6px;
    background: var(--control-bg);
    padding: 2px;
  }
  .seg .tree {
    font-size: 13px;
    padding: 0 3px 0 4px;
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
  .toggle:disabled {
    opacity: 0.55;
    cursor: not-allowed;
  }
  /* custom hover tooltip, dropped below the button (native title is flaky and
     never shows while the button is disabled during the cooldown) */
  .tip-wrap {
    position: relative;
    display: inline-flex;
  }
  .tip-wrap:hover::after {
    content: attr(data-tip);
    position: absolute;
    top: calc(100% + 9px);
    right: 0;
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
    right: 14px;
    border: 6px solid transparent;
    border-bottom-color: #222;
    z-index: 50;
    pointer-events: none;
  }
  .ico {
    display: inline-block;
  }
  .ico.spin {
    animation: spin 0.7s linear infinite;
  }
  @keyframes spin {
    to {
      transform: rotate(360deg);
    }
  }
  .search {
    font-size: 12px;
    padding: 4px 9px;
    border-radius: 6px;
    border: 1px solid var(--control-border);
    background: var(--input-bg);
    color: var(--control-fg);
    width: 230px;
  }
  .search::placeholder {
    color: var(--placeholder);
  }
  .tagsearch {
    width: 200px;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    color: #16a34a;
  }
  .tagsearch::placeholder {
    color: var(--placeholder);
  }
  .clear {
    font-size: 12px;
    padding: 4px 9px;
    border-radius: 6px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--control-fg);
    cursor: pointer;
  }
</style>
