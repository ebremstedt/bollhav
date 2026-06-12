<script>
  import { view, applyFilter, clearFocus, refresh } from "../lib/view.svelte.js";

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
  <span class="hint">colour = kind · click a model for runs &amp; errors</span>
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
  <button class="toggle" onclick={refresh} title="reload the graph">⟳ refresh</button>
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
