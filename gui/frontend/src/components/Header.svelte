<script>
  import {
    view,
    applyFilter,
    clearFocus,
    refresh,
    setNameStyle,
  } from "../lib/view.svelte.js";

  let { dark = $bindable() } = $props();

  let modelNames = $derived(
    view.full
      ? view.full.nodes
          .filter((n) => n.type === "model")
          .map((n) => n.name)
          .sort()
      : [],
  );
</script>

<header>
  <strong>Lineage</strong>
  <span class="hint">click a model for runs &amp; errors</span>
  <input
    class="search"
    list="model-list"
    placeholder="focus a model + its upstreams…"
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
  {#if view.focused}
    <button class="clear" onclick={clearFocus}>✕ show all</button>
  {/if}
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
  <span
    class="tip-wrap"
    data-tip="How model & source names are shown: Lengthen (one line) or Thicken (one dotted segment per line)."
  >
    <button
      class="toggle"
      onclick={() =>
        setNameStyle(view.nameStyle === "thicken" ? "lengthen" : "thicken")}
    >
      {view.nameStyle === "thicken" ? "⤢ Lengthen" : "≣ Thicken"}
    </button>
  </span>
  <button class="toggle" onclick={() => (dark = !dark)}>
    {dark ? "☀ light" : "☾ dark"}
  </button>
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
  .hint {
    color: var(--muted);
    font-weight: 400;
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
    margin-left: auto;
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
