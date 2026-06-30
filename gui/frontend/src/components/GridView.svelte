<script>
  import { view, passesTime } from "../lib/view.svelte.js";
  import { getGrid, getErrors } from "../lib/api.js";
  import { ts, STATUS_COLOR } from "../lib/constants.js";
  import { nameSegments } from "../lib/graph.js";
  import TimeFilter from "./TimeFilter.svelte";

  const LIMITS = [20, 40, 100, 300];
  const SORT_KEYS = [
    ["catalog", "catalog"],
    ["schema", "schema"],
    ["table", "table"],
  ];

  let limit = $state(40);
  let cellOrder = $state("oldest"); // "oldest" → newest on the right; "newest" → newest on the left
  let sortKey = $state("catalog"); // sort the model rows by catalog / schema / table
  let sortDir = $state("asc");
  let groups = $state([]); // [{full_name, runs:[…]}]
  let loading = $state(false);
  let loaded = $state(false);
  let selected = $state(null); // {full_name, run}
  let selErr = $state(null); // fetched error detail for a selected error cell

  let tagMatchSet = $derived(view.tagMatches ? new Set(view.tagMatches) : null);

  // (re)load on env / refresh / limit change
  $effect(() => {
    const n = limit;
    void view.env;
    void view.refreshAt;
    loading = true;
    selected = null;
    getGrid(n)
      .then((d) => {
        groups = d;
        loaded = true;
      })
      .catch(() => (groups = []))
      .finally(() => (loading = false));
  });

  // filter by model name + tag match, and each model's cells by the time
  // filter; drop models left with no visible cells.
  let rows = $derived.by(() => {
    const out = groups
      .filter((g) => !tagMatchSet || tagMatchSet.has(g.full_name))
      .map((g) => {
        // backend gives newest-first; "oldest" puts newest on the right
        const ordered = cellOrder === "newest" ? g.runs : [...g.runs].reverse();
        return {
          full_name: g.full_name,
          cells: ordered.filter((r) => passesTime(r.applied_at, r.since, r.until)),
        };
      })
      .filter((g) => g.cells.length);
    // sort the model rows by the chosen identity part
    const sign = sortDir === "asc" ? 1 : -1;
    out.sort((a, b) => {
      const c = part(a.full_name, sortKey).localeCompare(part(b.full_name, sortKey));
      return (c || a.full_name.localeCompare(b.full_name)) * sign;
    });
    return out;
  });

  // is any filter (tag / time) narrowing the view right now? (the model-name
  // filter is lineage-only)
  let hasFilter = $derived(
    !!tagMatchSet ||
      (view.loadedMode === "exact"
        ? !!view.loadedExact
        : !!(view.loadedFrom || view.loadedTo)) ||
      view.intervalMode !== "any",
  );

  // a model's catalog / schema / table segment, from the end of the dotted name
  function part(full, key) {
    const p = (full || "").split(".");
    if (key === "table") return (p[p.length - 1] || "").toLowerCase();
    if (key === "schema") return (p[p.length - 2] || "").toLowerCase();
    return (p[p.length - 3] || "").toLowerCase(); // catalog
  }

  // when an error cell is selected, fetch its error message/traceback
  $effect(() => {
    selErr = null;
    const s = selected;
    if (s && s.run.status === "error") {
      getErrors(s.full_name).then((list) => {
        selErr =
          list.find((e) => e.run_id === s.run.run_id) ||
          list.find((e) => e.since === s.run.since && e.until === s.run.until) ||
          null;
      });
    }
  });

  function shortName(full) {
    return (full || "").split(".").slice(-2).join(".");
  }
  function interval(r) {
    return r.since ? `${ts(r.since)} → ${ts(r.until)}` : "whole table";
  }
  function cellTip(full, r) {
    return `${r.status} · ${interval(r)}${r.applied_at ? " · " + ts(r.applied_at) : ""}${r.blocked_reason ? "\n" + r.blocked_reason : ""}`;
  }
  function copyTb(text, ev) {
    const btn = ev.currentTarget;
    navigator.clipboard.writeText(text).then(() => {
      btn.textContent = "copied!";
      setTimeout(() => (btn.textContent = "copy"), 1200);
    });
  }
</script>

<section class="grid">
  <div class="bar">
    <span class="count">
      {#if loading}loading…{:else}{rows.length} models{/if}
    </span>
    <TimeFilter />
    <span class="spacer"></span>
    <span class="seg">
      {#each SORT_KEYS as [val, label]}
        <button
          class="seg-btn"
          class:active={sortKey === val}
          title="click to toggle asc / desc"
          onclick={() => {
            if (sortKey === val) sortDir = sortDir === "asc" ? "desc" : "asc";
            else {
              sortKey = val;
              sortDir = "asc";
            }
          }}
          >{label}{#if sortKey === val}<sup class="dir">({sortDir})</sup>{/if}</button
        >
      {/each}
    </span>
    <button
      class="toggle"
      onclick={() => (cellOrder = cellOrder === "oldest" ? "newest" : "oldest")}
    >
      {cellOrder === "oldest" ? "↑ oldest first" : "↓ newest first"}
    </button>
    <span class="seg">
      {#each LIMITS as n}
        <button class="seg-btn" class:active={limit === n} onclick={() => (limit = n)}>
          {n}
        </button>
      {/each}
    </span>
  </div>

  {#if loaded && rows.length === 0}
    <p class="empty">
      {#if hasFilter}
        No models match the current filters.
      {:else}
        Nothing recorded in this environment yet. 🎉
      {/if}
    </p>
  {:else}
    <div class="body">
      <div class="scroll">
        <table>
          <tbody>
            {#each rows as g (g.full_name)}
              <tr>
                <td class="model" title={g.full_name}
                  >{#each nameSegments(shortName(g.full_name), view.tagHighlights[g.full_name] || []) as s}<span
                      class:hit={s.hit}>{s.text}</span
                    >{/each}</td
                >
                <td class="cells">
                  {#each g.cells as r, i (i)}
                    <button
                      class="cell"
                      class:sel={selected &&
                        selected.full_name === g.full_name &&
                        selected.run === r}
                      style="background:{STATUS_COLOR[r.status] || '#888'}"
                      data-tip={cellTip(g.full_name, r)}
                      onclick={() => (selected = { full_name: g.full_name, run: r })}
                      aria-label={r.status}
                    ></button>
                  {/each}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>

      {#if selected}
        <aside class="detail">
          <div class="det-head">
            <span class="det-title" title={selected.full_name}>{shortName(selected.full_name)}</span>
            <button class="x" onclick={() => (selected = null)}>✕</button>
          </div>
          <div class="det-row">
            <span class="dot" style="background:{STATUS_COLOR[selected.run.status] || '#888'}"></span>
            <b>{selected.run.status}</b>
          </div>
          <div class="det-kv"><span>interval</span><span class="mono">{interval(selected.run)}</span></div>
          <div class="det-kv"><span>applied</span><span class="mono">{ts(selected.run.applied_at)}</span></div>
          {#if selected.run.blocked_reason}
            <div class="det-kv"><span>blocked</span><span>{selected.run.blocked_reason}</span></div>
          {/if}
          <div class="det-kv"><span>run id</span><span class="mono small">{selected.run.run_id || "—"}</span></div>
          {#if selected.run.status === "error"}
            {#if selErr}
              <div class="det-msg">{selErr.error_type}: {selErr.error_message}</div>
              {#if selErr.traceback}
                <div class="tb-wrap">
                  <button class="copy-tb" onclick={(ev) => copyTb(selErr.traceback, ev)}>copy</button>
                  <pre class="tb">{selErr.traceback}</pre>
                </div>
              {/if}
            {:else}
              <div class="det-msg muted">loading error details…</div>
            {/if}
          {/if}
        </aside>
      {/if}
    </div>
  {/if}
</section>

<style>
  .grid {
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
    background: var(--bg);
    color: var(--fg);
  }
  .bar {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 16px;
    border-bottom: 1px solid var(--border);
  }
  .count {
    font-size: 12px;
    color: var(--muted);
  }
  .spacer {
    flex: 1;
  }
  .dir {
    font-size: 8px;
    margin-left: 1px;
    color: #ffd23f;
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
    padding: 3px 10px;
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
  .body {
    flex: 1;
    min-height: 0;
    display: flex;
  }
  .scroll {
    flex: 1;
    min-height: 0;
    overflow: auto;
    padding: 8px 16px 16px;
  }
  table {
    /* `separate` (not collapse) is required for a sticky <td> to paint its
       background OVER the cells that scroll under it (Chrome render bug). */
    border-collapse: separate;
    border-spacing: 0;
    font-size: 12px;
  }
  td {
    padding: 3px 8px 3px 0;
    vertical-align: middle;
  }
  .model {
    white-space: nowrap;
    font-weight: 600;
    position: sticky;
    left: 0;
    /* paint above the cells (which are position:relative) so they slide UNDER */
    z-index: 3;
    background: var(--bg);
    padding-right: 12px;
    /* shadow on the right edge so cells visibly slide UNDER the pinned column */
    box-shadow: 4px 0 6px -2px rgba(0, 0, 0, 0.25);
  }
  .model .hit {
    color: #16a34a;
  }
  .cells {
    display: flex;
    flex-wrap: nowrap;
    gap: 3px;
  }
  .cell {
    width: 15px;
    height: 15px;
    border-radius: 3px;
    border: 1px solid rgba(0, 0, 0, 0.2);
    padding: 0;
    cursor: pointer;
  }
  .cell:hover {
    outline: 2px solid var(--fg);
    outline-offset: 1px;
  }
  .cell.sel {
    outline: 2px solid var(--fg);
    outline-offset: 1px;
  }
  /* custom hover tooltip on a cell */
  .cell {
    position: relative;
  }
  .cell:hover::after {
    content: attr(data-tip);
    position: absolute;
    left: 50%;
    transform: translateX(-50%);
    top: calc(100% + 6px);
    width: max-content;
    max-width: 280px;
    white-space: pre-line;
    background: #222;
    color: #fff;
    font-size: 11px;
    line-height: 1.4;
    padding: 6px 8px;
    border-radius: 6px;
    box-shadow: 0 4px 14px rgba(0, 0, 0, 0.35);
    z-index: 40;
    pointer-events: none;
  }
  .detail {
    flex: 0 0 320px;
    border-left: 1px solid var(--border);
    padding: 12px 14px;
    overflow: auto;
  }
  .det-head {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
  }
  .det-title {
    font-weight: 700;
    font-size: 13px;
    word-break: break-all;
  }
  .x {
    margin-left: auto;
    border: none;
    background: transparent;
    color: var(--muted);
    font-size: 15px;
    cursor: pointer;
  }
  .det-row {
    display: flex;
    align-items: center;
    gap: 6px;
    margin-bottom: 8px;
    font-size: 13px;
  }
  .dot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    display: inline-block;
  }
  .det-kv {
    display: flex;
    justify-content: space-between;
    gap: 10px;
    font-size: 12px;
    padding: 3px 0;
    border-top: 1px solid var(--table-border);
  }
  .det-kv span:first-child {
    color: var(--muted);
  }
  .mono {
    font-family: ui-monospace, monospace;
    font-size: 11px;
  }
  .mono.small {
    font-size: 10px;
    word-break: break-all;
  }
  .det-msg {
    margin-top: 10px;
    font-size: 12px;
    color: var(--err-msg);
    white-space: pre-wrap;
    word-break: break-word;
  }
  .det-msg.muted {
    color: var(--muted);
    font-style: italic;
  }
  .tb-wrap {
    position: relative;
    margin-top: 8px;
  }
  .copy-tb {
    position: absolute;
    top: 6px;
    right: 6px;
    font-size: 10px;
    padding: 2px 8px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--control-fg);
    border-radius: 4px;
    cursor: pointer;
  }
  .tb {
    margin: 0;
    padding: 8px;
    background: var(--err-bg);
    border-radius: 4px;
    font-family: ui-monospace, monospace;
    font-size: 10px;
    white-space: pre-wrap;
    word-break: break-word;
    max-height: 360px;
    overflow: auto;
  }
  .empty {
    padding: 32px 16px;
    color: var(--muted);
    font-style: italic;
  }
</style>
