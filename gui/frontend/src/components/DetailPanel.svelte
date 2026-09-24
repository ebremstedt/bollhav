<script>
  import { selection } from "../lib/selection.svelte.js";
  import { view } from "../lib/view.svelte.js";
  import { ui } from "../lib/url.svelte.js";
  import { getState, getErrors } from "../lib/api.js";
  import RunsTable from "./RunsTable.svelte";
  import ErrorList from "./ErrorList.svelte";
  import ResetBox from "./ResetBox.svelte";

  let runs = $state([]);
  let errs = $state([]);
  let showHistoric = $state(false);
  // how many runs / errors to list — the site-wide choice
  const LIMITS = [100, 365, "all"];
  let effectiveLimit = $derived(ui.panelRuns === "all" ? 100000 : ui.panelRuns);

  // the runs picked in the table, for a reset (a new model or a reload
  // drops them)
  let picked = $state([]);
  const key = (r) => `${r.since}|${r.until}`;
  let pickedKeys = $derived(new Set(picked.map(key)));
  function togglePick(r) {
    picked = pickedKeys.has(key(r)) ? picked.filter((p) => key(p) !== key(r)) : [...picked, r];
  }
  $effect(() => {
    void selection.name;
    void view.refreshAt;
    picked = [];
  });

  // (re)load runs + errors whenever the selected model or the count changes
  $effect(() => {
    const name = selection.name;
    const n = effectiveLimit;
    void view.refreshAt; // re-run when the user hits refresh
    showHistoric = false;
    if (!name) {
      runs = [];
      errs = [];
      return;
    }
    getState(name, n).then((d) => (runs = d));
    getErrors(name, n).then((d) => (errs = d));
  });

  // An error is "unresolved" only while that same interval window is still in
  // 'error' state. A later successful run clears the state row, so the error
  // becomes "historic" (resolved) — kept for the record, shown muted.
  let activeWindows = $derived(
    new Set(
      runs
        .filter((r) => r.status === "error")
        .map((r) => `${r.since}|${r.until}`),
    ),
  );
  let activeErrs = $derived(
    errs.filter((e) => activeWindows.has(`${e.since}|${e.until}`)),
  );
  let historicErrs = $derived(
    errs.filter((e) => !activeWindows.has(`${e.since}|${e.until}`)),
  );
</script>

<aside class="panel">
  <div class="panel-head">
    <span class="panel-title">{selection.name}</span>
    <button class="x" onclick={() => (selection.name = null)}>✕</button>
  </div>

  <div class="tabs">
    <button
      class="tab"
      class:active={selection.tab === "state"}
      onclick={() => (selection.tab = "state")}
    >
      Latest runs
    </button>
    <button
      class="tab"
      class:active={selection.tab === "errors"}
      onclick={() => (selection.tab = "errors")}
    >
      Errors {activeErrs.length ? `(${activeErrs.length})` : ""}
    </button>
  </div>
  <div class="count">
    <span class="count-label">number of runs</span>
    <span class="seg">
      {#each LIMITS as n}
        <button class="seg-btn" class:active={ui.panelRuns === n} onclick={() => (ui.panelRuns = n)}>{n}</button>
      {/each}
    </span>
  </div>

  {#if selection.tab === "state"}
    <RunsTable {runs} selected={view.writable ? pickedKeys : null} onpick={view.writable ? togglePick : null} />
    {#if view.writable}
      <p class="pick-hint">click rows to pick intervals</p>
      <ResetBox
        name={selection.name}
        intervals={picked}
        sample={runs.find((r) => r.since)?.since || ""}
        ondone={() => view.refreshAt++}
      />
    {/if}
  {:else}
    {#if activeErrs.length === 0 && historicErrs.length === 0}
      <p class="empty">No errors logged.</p>
    {/if}
    {#if activeErrs.length}
      <ErrorList errs={activeErrs} />
    {/if}
    {#if historicErrs.length}
      <button class="hist-toggle" onclick={() => (showHistoric = !showHistoric)}>
        {showHistoric ? "▾" : "▸"} Historic errors (resolved) · {historicErrs.length}
      </button>
      {#if showHistoric}
        <ErrorList errs={historicErrs} muted />
      {/if}
    {/if}
  {/if}
</aside>

<style>
  .panel {
    width: 460px;
    border-left: 1px solid var(--border);
    background: var(--bg);
    color: var(--fg);
    overflow-y: auto;
    padding: 12px 14px;
    box-sizing: border-box;
  }
  .panel-head {
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .panel-title {
    font-family: var(--name-font);
    font-weight: var(--name-weight);
    font-size: 14px;
    word-break: break-all;
  }
  .x {
    margin-left: auto;
    border: none;
    background: transparent;
    font-size: 15px;
    cursor: pointer;
    color: inherit;
  }
  .tabs {
    display: flex;
    gap: 6px;
    margin: 14px 0 12px;
  }
  .tab {
    font-family: var(--btn-font);
    flex: 1;
    font-size: 15px;
    padding: 6px 10px;
    border-radius: 8px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--fg);
    cursor: pointer;
  }
  /* the run / error count under the tabs */
  .count {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 3px;
    margin: 0 0 12px;
  }
  .count-label {
    font-size: 11px;
    color: var(--muted);
  }
  .seg {
    display: inline-flex;
    align-items: stretch;
    gap: 3px;
    border: 1px solid var(--control-border);
    border-radius: 8px;
    background: var(--control-bg);
    padding: 3px;
  }
  .seg-btn {
    font-family: var(--box-option-font);
    font-size: 14px;
    padding: 4px 12px;
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
  /* the active tab: the site's green, like every other active switch */
  .tab.active {
    background: #2e7d32;
    color: #fff;
    border-color: #2e7d32;
  }
  .empty {
    color: #999;
    font-size: 12px;
    font-style: italic;
  }
  .pick-hint {
    margin: 8px 0 0;
    font-size: 12px;
    color: var(--muted);
    font-style: italic;
  }
  .hist-toggle {
    width: 100%;
    text-align: left;
    margin-top: 12px;
    padding: 6px 4px;
    font-size: 11px;
    color: var(--muted);
    background: transparent;
    border: none;
    border-top: 1px solid var(--border);
    cursor: pointer;
  }
  .hist-toggle:hover {
    color: var(--fg);
  }
</style>
