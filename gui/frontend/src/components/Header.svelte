<script>
  import { view, refresh, setEnv, setTab } from "../lib/view.svelte.js";

  // top-level view tabs
  const TABS = [
    ["models", "Models"],
    ["gaps", "Gaps"],
    ["runs", "Runs"],
    ["grid", "Grid"],
    ["lineage", "Lineage"],
  ];

  let { dark = $bindable() } = $props();

</script>

<header>
  <div class="brand">{view.title}</div>
  <div class="left">
    <span class="seg">
      {#each TABS as [val, label]}
        <button
          class="seg-btn"
          class:active={view.tab === val}
          onclick={() => setTab(val)}>{label}</button
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
    <!-- filtering by name / tag lives in each tab's sub-bar (ModelFilter) -->
    <span
      class="tip-wrap"
      data-tip="Reload data. Limited to once every 5 seconds."
    >
      <button
        class="toggle"
        onclick={refresh}
        disabled={!view.canRefresh}
        aria-label="refresh"
        title={view.cooldown > 0 ? `refresh (${view.cooldown}s)` : "refresh"}
      >
        <span class="ico" class:spin={view.refreshing}>⟳</span>
      </button>
    </span>
    <button
      class="toggle"
      onclick={() => (dark = !dark)}
      aria-label={dark ? "switch to light mode" : "switch to dark mode"}
      title={dark ? "light mode" : "dark mode"}
    >
      {dark ? "☀" : "☾"}
    </button>
  </div>
</header>

<style>
  /* three columns: the site's name, the menu (centred), the buttons (right) */
  header {
    padding: 10px 14px;
    border-bottom: 1px solid var(--border);
    font-size: 14px;
    display: grid;
    grid-template-columns: 1fr auto 1fr;
    align-items: center;
    gap: 10px;
    background: var(--bg);
    color: var(--fg);
  }
  .left,
  .right {
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .brand {
    font-family: var(--brand-font);
    font-size: var(--brand-size);
    font-weight: var(--brand-weight);
    grid-column: 1;
    white-space: nowrap;
  }
  .left {
    grid-column: 2;
    justify-content: center;
  }
  .right {
    grid-column: 3;
    justify-content: flex-end;
  }
  .toggle {
    font-family: var(--menu-font);
    font-size: var(--menu-size);
    font-weight: var(--menu-weight);
    padding: 6px 15px;
    border-radius: 8px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--control-fg);
    cursor: pointer;
    white-space: nowrap;
  }
  .seg {
    display: inline-flex;
    align-items: center;
    gap: 3px;
    border: 1px solid var(--control-border);
    border-radius: 8px;
    background: var(--control-bg);
    padding: 3px;
  }
  .seg-btn {
    font-family: var(--menu-font);
    font-size: var(--menu-size);
    font-weight: var(--menu-weight);
    padding: 5px 14px;
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
  /* the ⟳ glyph is drawn small in the system font: scaled up to read the
     same size as the ☾ / ☀ next to it, without making its button taller */
  .ico {
    display: inline-block;
    font-size: 1.75em;
    font-weight: 700;
    line-height: 0.69;
    vertical-align: -2px;
  }
  .ico.spin {
    animation: spin 0.7s linear infinite;
  }
  @keyframes spin {
    to {
      transform: rotate(360deg);
    }
  }
  .envsel {
    font-family: var(--menu-font);
    font-size: var(--menu-size);
    font-weight: var(--menu-weight);
    padding: 6px 12px;
    border-radius: 8px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--control-fg);
    cursor: pointer;
  }
</style>
