<script>
  // Shared ⏱ time filter for the runs + grid tabs (lives in their second bar).
  // Reads/writes the shared time-filter state in the view store.
  import { view, clearTime } from "../lib/view.svelte.js";

  let showTime = $state(false);
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
  let timeActive = $derived(loadedActive || view.intervalMode !== "any");
</script>

<span class="time-wrap">
  <button
    class="toggle"
    class:on={timeActive}
    onclick={() => (showTime = !showTime)}
  >
    ⏱ time
  </button>
  {#if showTime}
    <div class="time-pop">
      <div class="tp-head">
        <strong>Filter by time</strong>
        <button class="tp-x" onclick={() => (showTime = false)}>✕</button>
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
      <button class="tp-clear" onclick={clearTime}>clear time filter</button>
    </div>
  {/if}
</span>

<style>
  .time-wrap {
    position: relative;
    display: inline-flex;
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
    font-size: 11px;
    padding: 3px 6px;
    width: 150px;
    border-radius: 5px;
    border: 1px solid var(--control-border);
    background: var(--input-bg);
    color: var(--control-fg);
    font-family: ui-monospace, monospace;
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
