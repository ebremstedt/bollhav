<script>
  import {
    view,
    applyFilter,
    applyTagFilter,
    clearFocus,
    refresh,
    setEnv,
    setTab,
  } from "../lib/view.svelte.js";

  // top-level view tabs
  const TABS = [
    ["lineage", "Lineage"],
    ["runs", "Runs"],
    ["grid", "Grid"],
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
    <span class="seg">
      {#each TABS as [val, label]}
        <button
          class="seg-btn"
          class:active={view.tab === val}
          onclick={() => setTab(val)}
          >{label}{#if val === "grid"}<sup class="beta">(beta)</sup>{/if}</button
        >
      {/each}
    </span>
    {#if view.environments.length}
      <span
        class="tip-wrap tipleft"
        data-tip="Environment — which bollhav library schema to read: prod, or a suffixed dev / PR env in the same database."
      >
        <select
          class="envsel"
          value={view.env ??
            view.environments.find((e) => e.label === "prod")?.schema}
          onchange={(e) => setEnv(e.currentTarget.value)}
        >
          {#each view.environments as ev}
            <option value={ev.schema}>{ev.label}</option>
          {/each}
        </select>
      </span>
    {/if}
  </div>

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
      data-tip="Reload data. Limited to once every 5 seconds."
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
  .toggle {
    font-size: 12px;
    padding: 4px 10px;
    border-radius: 6px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--control-fg);
    cursor: pointer;
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
  .beta {
    font-size: 8px;
    margin-left: 1px;
    color: #ffd23f;
  }
  .toggle:disabled {
    opacity: 0.55;
    cursor: not-allowed;
  }
  /* custom hover tooltip, dropped below the button */
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
  .tip-wrap.tipleft:hover::after {
    right: auto;
    left: 0;
  }
  .tip-wrap.tipleft:hover::before {
    right: auto;
    left: 14px;
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
  .envsel {
    font-size: 12px;
    padding: 4px 8px;
    border-radius: 6px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--control-fg);
    cursor: pointer;
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
