<script>
  import { selection } from "../lib/selection.svelte.js";
  import { view } from "../lib/view.svelte.js";
  import { getState, getErrors } from "../lib/api.js";
  import RunsTable from "./RunsTable.svelte";
  import ErrorList from "./ErrorList.svelte";

  let runs = $state([]);
  let errs = $state([]);
  let showHistoric = $state(false);

  // (re)load runs + errors whenever the selected model changes
  $effect(() => {
    const name = selection.name;
    void view.refreshAt; // re-run when the user hits refresh
    showHistoric = false;
    if (!name) {
      runs = [];
      errs = [];
      return;
    }
    getState(name).then((d) => (runs = d));
    getErrors(name).then((d) => (errs = d));
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

  {#if selection.tab === "state"}
    <RunsTable {runs} />
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
    width: 340px;
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
    font-weight: 700;
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
    flex: 1;
    font-size: 12px;
    padding: 5px 8px;
    border-radius: 6px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--fg);
    cursor: pointer;
  }
  .tab.active {
    background: var(--fg);
    color: var(--bg);
    border-color: var(--fg);
  }
  .empty {
    color: #999;
    font-size: 12px;
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
