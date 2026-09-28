<script>
  // Shared time filter for the runs, grid and gaps tabs, in two halves with a
  // button each: WHEN (when it ran) and WHAT (what was loaded, the interval).
  // Reads/writes the shared time-filter state in the view store.
  import { view } from "../lib/view.svelte.js";

  // `intervalOnly` hides the WHEN half — for a tab whose rows are intervals
  // rather than runs (gaps)
  let { intervalOnly = false } = $props();

  let open = $state(null); // "when" | "what" | null — which popover is open
  const toggle = (which) => (open = open === which ? null : which);

  const LOADED_MODES = [
    ["exact", "exact"],
    ["range", "range"],
  ];
  const INTERVAL_MODES = [
    ["any", "any"],
    ["whole", "whole table"],
    ["range", "date range"],
  ];
  let loadedActive = $derived(
    view.loadedMode === "exact"
      ? !!view.loadedExact
      : !!(view.loadedFrom || view.loadedTo),
  );
  let intervalActive = $derived(view.intervalMode !== "any");

  function clearLoaded() {
    view.loadedMode = "exact";
    view.loadedExact = "";
    view.loadedFrom = "";
    view.loadedTo = "";
  }
  function clearInterval() {
    view.intervalMode = "any";
    view.intervalFrom = "";
    view.intervalTo = "";
  }
</script>

<span class="time-wrap">
  {#if !intervalOnly}
    <span class="one">
      <button class="toggle" class:on={loadedActive} onclick={() => toggle("when")}>when</button>
      {#if open === "when"}
        <div class="time-pop">
          <div class="tp-head">
            <strong>Filter by when it ran</strong>
            <button class="tp-x" onclick={() => (open = null)}>✕</button>
          </div>
          <div class="tp-sec">loaded (when it ran)</div>
          <span class="seg tp-seg">
            {#each LOADED_MODES as [val, label]}
              <button
                class="seg-btn"
                class:active={view.loadedMode === val}
                onclick={() => (view.loadedMode = val)}>{label}</button
              >
            {/each}
          </span>
          {#if view.loadedMode === "exact"}
            <label>at <input type="text" placeholder="2026-06-15 18:15:00" bind:value={view.loadedExact} /></label>
          {:else}
            <label>from <input type="text" placeholder="2026-06-14 09:30" bind:value={view.loadedFrom} /></label>
            <label>to <input type="text" placeholder="2026-06-14 17:00:00" bind:value={view.loadedTo} /></label>
          {/if}
          <div class="tp-hint">date, or date + time (HH:MM[:SS])</div>
          <button class="tp-clear" onclick={clearLoaded}>clear</button>
        </div>
      {/if}
    </span>
  {/if}
  <span class="one">
    <button class="toggle" class:on={intervalActive} onclick={() => toggle("what")}>what</button>
    {#if open === "what"}
      <div class="time-pop">
        <div class="tp-head">
          <strong>Filter by what was loaded</strong>
          <button class="tp-x" onclick={() => (open = null)}>✕</button>
        </div>
        <div class="tp-sec">interval (what was loaded)</div>
        <span class="seg tp-seg">
          {#each INTERVAL_MODES as [val, label]}
            <button
              class="seg-btn"
              class:active={view.intervalMode === val}
              onclick={() => (view.intervalMode = val)}>{label}</button
            >
          {/each}
        </span>
        {#if view.intervalMode === "range"}
          <label>from <input type="text" placeholder="2026-06-14" bind:value={view.intervalFrom} /></label>
          <label>to <input type="text" placeholder="2026-06-18 23:59" bind:value={view.intervalTo} /></label>
        {/if}
        <button class="tp-clear" onclick={clearInterval}>clear</button>
      </div>
    {/if}
  </span>
</span>

<style>
  .time-wrap {
    display: inline-flex;
    gap: 8px;
  }
  .one {
    position: relative;
    display: inline-flex;
  }
  .toggle {
    font-family: var(--box-option-font);
    font-size: var(--box-option-size);
    font-weight: var(--box-option-weight);
    padding: 6px 15px;
    border-radius: 8px;
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
  .time-pop {
    position: absolute;
    top: calc(100% + 8px);
    left: 0;
    width: 270px;
    background: var(--bg);
    border: 1px solid var(--control-border);
    border-radius: 8px;
    box-shadow: 0 6px 20px rgba(0, 0, 0, 0.3);
    padding: 12px;
    z-index: 60;
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .tp-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 2px;
  }
  .tp-x {
    border: none;
    background: transparent;
    color: var(--muted);
    cursor: pointer;
    font-size: 13px;
  }
  .tp-sec {
    font-size: 11px;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.04em;
    margin-top: 4px;
  }
  .time-pop label {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    font-size: 12px;
  }
  .time-pop input[type="text"] {
    font-family: var(--font-mono);
    font-size: 11px;
    padding: 3px 6px;
    width: 150px;
    border-radius: 5px;
    border: 1px solid var(--control-border);
    background: var(--input-bg);
    color: var(--control-fg);
  }
  .time-pop input::placeholder {
    color: var(--placeholder);
  }
  .tp-hint {
    font-size: 10px;
    color: var(--muted);
    font-style: italic;
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
    font-family: var(--box-option-font);
    font-size: var(--box-option-size);
    font-weight: var(--box-option-weight);
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
  .tp-seg {
    display: flex;
  }
  .tp-seg .seg-btn {
    flex: 1;
    text-align: center;
  }
  .tp-clear {
    margin-top: 6px;
    font-size: 12px;
    padding: 4px 9px;
    border-radius: 6px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--control-fg);
    cursor: pointer;
  }
</style>
