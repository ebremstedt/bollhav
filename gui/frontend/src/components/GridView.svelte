<script>
  import { view, passesTime, matOk } from "../lib/view.svelte.js";
  import { ui } from "../lib/url.svelte.js";
  import ModelFilter from "./ModelFilter.svelte";
  import { getGrid, getErrors } from "../lib/api.js";
  import ResetBox from "./ResetBox.svelte";
  import { ts, STATUS_COLOR } from "../lib/constants.js";
  import { nameSegments } from "../lib/graph.js";
  import TimeFilter from "./TimeFilter.svelte";

  // runs per model: "fit" = as many as the width beside the model column
  // holds (the default), or a fixed count
  const LIMITS = ["width", 100, 365, "all"];
  // model rows: "height" = as many as the scrolling area holds (the
  // default), or a fixed count
  const MODEL_LIMITS = ["height", "all"];
  const SORT_KEYS = [
    ["catalog", "catalog"],
    ["schema", "schema"],
    ["table", "table"],
  ];

  // width: before any row is on screen, an estimate from the scroll
  // area's width (a cell ≈ 17px, the model column ≈ 340px, padding 32px);
  // once rows exist, the real cell width and the real room beside the model
  // column are measured from the DOM. Rounded down to fives so a resize by a
  // few pixels doesn't refetch.
  let scrollW = $state(0); // bound from the scroll area; changing it re-measures
  let measured = $state(0);
  let estimate = $derived(scrollW ? Math.max(10, Math.floor((scrollW - 372) / 17)) : 0);
  let fit = $derived(measured || estimate);
  let effectiveLimit = $derived(ui.gridRuns === "width" ? fit : ui.gridRuns === "all" ? 100000 : ui.gridRuns);
  // height for the model rows: an estimate from the scroll area's
  // height (a row ≈ 19px) until real rows can be measured
  let scrollH = $state(0);
  let rowMeasured = $state(0);
  let rowEstimate = $derived(scrollH ? Math.max(5, Math.floor((scrollH - 24) / 19)) : 0);
  let modelFit = $derived(rowMeasured || rowEstimate);
  // 0 = no cap (see the slice in `rows`)
  let effectiveModels = $derived(ui.gridModels === "height" ? modelFit : 0);
  $effect(() => {
    void scrollH;
    void rows;
    const tr = document.querySelector(".grid tbody tr");
    if (!tr || !tr.offsetHeight) return;
    const n = Math.max(5, Math.floor((scrollH - 24) / tr.offsetHeight / 5) * 5);
    if (n !== rowMeasured) rowMeasured = n;
  });
  $effect(() => {
    void scrollW;
    void rows;
    const td = document.querySelector(".grid td.cells");
    const cell = td?.querySelector(".cell");
    if (!td || !cell || !cell.offsetWidth) return;
    const room = td.clientWidth - 8; // minus the cell's right padding
    const n = Math.max(5, Math.floor(room / cell.offsetWidth / 5) * 5);
    if (n !== measured) measured = n;
  });
  let groups = $state([]); // [{full_name, runs:[…]}]
  let loading = $state(false);
  let loaded = $state(false);
  // the picked cells (runs) of one model — the right-hand panel. `anchor` is
  // the cell a shift-click ranges from.
  let selected = $state(null); // {full_name, runs: [run, …], anchor}
  let selErr = $state(null); // fetched error detail for a selected error cell
  // the model whose name was clicked — the left-hand panel
  let modelPanel = $state(null); // full_name
  // any shown interval's `since`, for the offset a typed range should carry
  let sample = $derived(groups.flatMap((g) => g.runs).find((r) => r.since)?.since || "");

  let tagMatchSet = $derived(view.tagMatches ? new Set(view.tagMatches) : null);
  // the site-wide materialization filter (null = inactive)
  let matSet = $derived(
    view.matFilter === "all" || !view.full
      ? null
      : new Set(view.full.nodes.filter((n) => n.type === "model" && matOk(n)).map((n) => n.name)),
  );

  async function load(n) {
    loading = true;
    try {
      groups = await getGrid(n);
      loaded = true;
    } catch {
      groups = [];
    } finally {
      loading = false;
    }
  }

  // (re)load on env / refresh / limit change
  $effect(() => {
    const n = effectiveLimit;
    if (!n) return; // the scroll area isn't measured yet
    void view.env;
    void view.refreshAt;
    selected = null;
    load(n);
  });

  // filter by model name + tag match, and each model's cells by the time
  // filter; drop models left with no visible cells.
  let rows = $derived.by(() => {
    const out = groups
      .filter((g) => (!tagMatchSet || tagMatchSet.has(g.full_name)) && (!matSet || matSet.has(g.full_name)))
      .map((g) => {
        // backend gives newest-first; "oldest" puts newest on the right
        const ordered = ui.gridCells === "newest" ? g.runs : [...g.runs].reverse();
        return {
          full_name: g.full_name,
          cells: ordered.filter((r) => passesTime(r.applied_at, r.since, r.until)),
        };
      })
      .filter((g) => g.cells.length);
    // sort the model rows by the chosen identity part
    const sign = ui.gridDir === "asc" ? 1 : -1;
    out.sort((a, b) => {
      const c = part(a.full_name, ui.gridSort).localeCompare(part(b.full_name, ui.gridSort));
      return (c || a.full_name.localeCompare(b.full_name)) * sign;
    });
    return out.slice(0, effectiveModels || out.length);
  });

  // is any filter (tag / time) narrowing the view right now?
  let hasFilter = $derived(
    !!tagMatchSet ||
      !!matSet ||
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

  // the one selected cell, when exactly one is
  let one = $derived(selected && selected.runs.length === 1 ? selected.runs[0] : null);

  // when an error cell is selected, fetch its error message/traceback
  $effect(() => {
    selErr = null;
    const s = selected;
    const r = one;
    if (r && r.status === "error") {
      getErrors(s.full_name).then((list) => {
        selErr =
          list.find((e) => e.run_id === r.run_id) ||
          list.find((e) => e.since === r.since && e.until === r.until) ||
          null;
      });
    }
  });

  // cell selection: click = this cell; shift-click = every cell from the
  // anchor to this one (same row); ⌘ / ctrl-click = add or remove this cell
  function pick(g, r, i, e) {
    const same = selected && selected.full_name === g.full_name && selected.runs.length;
    if (same && e.shiftKey && selected.anchor >= 0) {
      const [lo, hi] = selected.anchor <= i ? [selected.anchor, i] : [i, selected.anchor];
      selected = { full_name: g.full_name, runs: g.cells.slice(lo, hi + 1), anchor: selected.anchor };
    } else if (same && (e.metaKey || e.ctrlKey)) {
      const runs = selected.runs.includes(r)
        ? selected.runs.filter((c) => c !== r)
        : [...selected.runs, r];
      selected = runs.length ? { full_name: g.full_name, runs, anchor: i } : null;
    } else {
      selected = { full_name: g.full_name, runs: [r], anchor: i };
    }
    ui.gridRow = g.full_name;
  }

  // clicking the model name: highlight the row and open the model's panel on
  // the left (a click on the same, already-open name closes both)
  function pickModel(g) {
    const off = modelPanel === g.full_name;
    modelPanel = off ? null : g.full_name;
    ui.gridRow = off ? null : g.full_name;
  }

  // after a reset: reload the cells, and re-point the picked runs at the
  // reloaded rows (same windows) so the panel shows their new status
  async function afterReset() {
    await load(effectiveLimit);
    if (selected) {
      const g = groups.find((x) => x.full_name === selected.full_name);
      const keys = new Set(selected.runs.map((r) => `${r.since}|${r.until}`));
      const runs = g ? g.runs.filter((r) => keys.has(`${r.since}|${r.until}`)) : [];
      selected = runs.length ? { ...selected, runs } : null;
    }
  }

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
  <ModelFilter />
  <div class="bar">
    <!-- one "sorting" box holding both sorts: the model rows (by a part of
         the name) and the run cells (oldest or newest first) -->
    <span class="group">
      <span class="group-label">sorting</span>
      <span class="group-body">
        <span class="sub">
          <span class="sub-label">models</span>
          <span class="seg">
            {#each SORT_KEYS as [val, label]}
              <!-- two lines: the key, and the direction under the active one
                   (the others keep an invisible second line so all three
                   buttons stay the same height) -->
              <button
                class="seg-btn two-line"
                class:active={ui.gridSort === val}
                title="click to toggle ascending / descending"
                onclick={() => {
                  if (ui.gridSort === val) ui.gridDir = ui.gridDir === "asc" ? "desc" : "asc";
                  else {
                    ui.gridSort = val;
                    ui.gridDir = "asc";
                  }
                }}
              >
                <span>{label}</span>
                <span class="dir" class:hidden={ui.gridSort !== val}
                  >{ui.gridSort === val && ui.gridDir === "desc" ? "descending" : "ascending"}</span
                >
              </button>
            {/each}
          </span>
        </span>
        <span class="sub">
          <span class="sub-label">runs</span>
          <button
            class="toggle order"
            onclick={() => (ui.gridCells = ui.gridCells === "oldest" ? "newest" : "oldest")}
          >
            {ui.gridCells === "oldest" ? "ascending" : "descending"}
          </button>
        </span>
      </span>
    </span>
    <!-- how the grid is shown: which runs (by time) and how many per model -->
    <span class="group">
      <span class="group-label">display</span>
      <span class="group-body">
        <span class="sub">
          <span class="sub-label">time</span>
          <TimeFilter />
        </span>
        <span class="sub">
          <span class="sub-label">number of models</span>
          <span class="seg">
            {#each MODEL_LIMITS as n}
              <button
                class="seg-btn"
                class:active={ui.gridModels === n}
                onclick={() => (ui.gridModels = n)}>{n}</button
              >
            {/each}
          </span>
        </span>
        <span class="sub">
          <span class="sub-label">number of runs</span>
          <span class="seg">
            {#each LIMITS as n}
              <button class="seg-btn" class:active={ui.gridRuns === n} onclick={() => (ui.gridRuns = n)}>
                {n}
              </button>
            {/each}
          </span>
        </span>
      </span>
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
      {#if modelPanel}
        <!-- the model's panel (its name was clicked): model-level resets -->
        <aside class="detail left">
          <div class="det-head">
            <span class="det-title" title={modelPanel}>{shortName(modelPanel)}</span>
            <button class="x" onclick={() => (modelPanel = null)}>✕</button>
          </div>
          <div class="det-kv"><span>full name</span><span class="mono small">{modelPanel}</span></div>
          <div class="det-kv">
            <span>runs shown</span><span>{rows.find((r) => r.full_name === modelPanel)?.cells.length ?? 0}</span>
          </div>
          {#if view.writable}
            <ResetBox name={modelPanel} model {sample} ondone={afterReset} />
          {/if}
        </aside>
      {/if}
      <div class="scroll" bind:clientWidth={scrollW} bind:clientHeight={scrollH}>
        <table>
          <tbody>
            {#each rows as g (g.full_name)}
              <!-- clicking the model name toggles the row highlight; clicking
                   a cell selects it and highlights its row too -->
              <tr class:hi={ui.gridRow === g.full_name}>
                <td
                  class="model"
                  title={g.full_name}
                  onclick={() => pickModel(g)}
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
                        selected.runs.includes(r)}
                      style="background:{STATUS_COLOR[r.status] || '#888'}"
                      data-tip={cellTip(g.full_name, r)}
                      onclick={(e) => pick(g, r, i, e)}
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
          {#if one}
            <div class="det-row">
              <span class="dot" style="background:{STATUS_COLOR[one.status] || '#888'}"></span>
              <b>{one.status}</b>
            </div>
            <div class="det-kv"><span>interval</span><span class="mono">{interval(one)}</span></div>
            <div class="det-kv"><span>applied</span><span class="mono">{ts(one.applied_at)}</span></div>
            {#if one.blocked_reason}
              <div class="det-kv"><span>blocked</span><span>{one.blocked_reason}</span></div>
            {/if}
            <div class="det-kv"><span>run id</span><span class="mono small">{one.run_id || "—"}</span></div>
            {#if one.status === "error"}
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
          {:else}
            <div class="det-row"><b>{selected.runs.length} intervals selected</b></div>
            <ul class="sel-list">
              {#each selected.runs as r}
                <li class="mono">{r.status} · {interval(r)}</li>
              {/each}
            </ul>
          {/if}

          {#if view.writable}
            <ResetBox name={selected.full_name} intervals={selected.runs} {sample} ondone={afterReset} />
          {/if}
        </aside>
      {/if}
    </div>
    <!-- the cells are runs, not dates: rows can hold different time resolutions -->
    <div class="foot">
      <span class="hint"
        >each cell is one run and each row is one model's latest runs in order of their window, so pipelines with
        different cadences (hourly, daily, monthly) sit side by side and columns don't share a time
        axis · click a model or a cell to highlight its row; shift-click a range of cells, ⌘ / ctrl-click
        to add cells</span
      >
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
    justify-content: center; /* the boxes sit centred in the bar */
    gap: 14px;
    padding: 10px 16px;
    border-bottom: 1px solid var(--border);
  }
  .foot {
    padding: 6px 16px;
    border-top: 1px solid var(--border);
    text-align: center;
  }
  .seg-btn.two-line {
    display: inline-flex;
    flex-direction: column;
    align-items: center;
    line-height: 1.15;
    padding: 4px 12px;
  }
  /* fixed widths so the box doesn't grow and shrink as the wording flips
     between "ascending" and "descending" */
  .dir {
    font-size: var(--box-option-sub-size);
    opacity: 0.85;
    display: inline-block;
    min-width: 66px;
    text-align: center;
  }
  .dir.hidden {
    visibility: hidden;
  }
  .toggle.order {
    min-width: 136px;
    text-align: center;
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
  .seg {
    display: inline-flex;
    align-items: stretch; /* buttons fill the segment's height (see .sub) */
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
    display: inline-flex;
    align-items: center;
    justify-content: center;
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
  .body {
    flex: 1;
    min-height: 0;
    display: flex;
    /* the table and its detail read a quarter larger, like the models tab */
  }
  .scroll {
    flex: 1;
    min-height: 0;
    overflow: auto;
    padding: 10px 20px 20px;
  }
  table {
    font-family: var(--table-cell-font);
    font-size: var(--table-cell-size);
    font-weight: var(--table-cell-weight);
    /* `separate` (not collapse) is required for a sticky <td> to paint its
       background OVER the cells that scroll under it (Chrome render bug). */
    border-collapse: separate;
    border-spacing: 0;
  }
  td {
    padding: 0 10px 0 0; /* rows touch */
    vertical-align: middle;
  }
  /* the highlighted row (click a model or a cell) — same tint as the gaps tab */
  tr.hi td {
    background: var(--row-hi);
  }
  tr.hi .model {
    box-shadow: inset 3px 0 0 #2e7d32;
    padding-left: 8px;
  }
  /* the highlighted row's cells brighten and get a green edge; every other
     row's cells fade a little so the highlighted one stands out */
  tr.hi .cell {
    filter: brightness(1.18) saturate(1.25);
    border-color: #2e7d32;
  }
  tbody:has(tr.hi) tr:not(.hi) .cell {
    opacity: 0.65;
  }
  .hint {
    font-size: 12px;
    font-style: italic;
    color: var(--muted);
  }
  .model {
    white-space: nowrap;
    font-family: var(--name-font);
    font-weight: var(--name-weight);
    font-size: var(--name-size);
    cursor: pointer;
    position: sticky;
    left: 0;
    /* paint above the cells (which are position:relative) so they slide UNDER */
    z-index: 3;
    background: var(--bg);
    padding-right: 15px;
    /* shadow on the right edge so cells visibly slide UNDER the pinned column */
    box-shadow: 5px 0 8px -2px rgba(0, 0, 0, 0.25);
  }
  .model .hit {
    background: rgba(22, 163, 74, 0.3);
    border-radius: 3px;
  }
  .cells {
    display: flex;
    flex-wrap: nowrap;
    gap: 0; /* cells touch; their 1px border keeps them apart */
  }
  .cell {
    width: 19px;
    height: 19px;
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
    top: calc(100% + 8px);
    width: max-content;
    max-width: 475px;
    white-space: pre-line;
    background: #222;
    color: #fff;
    font-size: 19px;
    line-height: 1.4;
    padding: 10px 14px;
    border-radius: 10px;
    box-shadow: 0 5px 18px rgba(0, 0, 0, 0.35);
    z-index: 40;
    pointer-events: none;
  }
  .detail {
    flex: 0 0 400px;
    border-left: 1px solid var(--border);
    padding: 15px 18px;
    overflow: auto;
  }
  .det-head {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 10px;
  }
  .det-title {
    font-family: var(--name-font);
    font-weight: var(--name-weight);
    font-size: 16px;
    word-break: break-all;
  }
  .x {
    margin-left: auto;
    border: none;
    background: transparent;
    color: var(--muted);
    font-size: 19px;
    cursor: pointer;
  }
  .det-row {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 10px;
    font-size: 16px;
  }
  .dot {
    width: 12px;
    height: 12px;
    border-radius: 50%;
    display: inline-block;
  }
  .det-kv {
    display: flex;
    justify-content: space-between;
    gap: 12px;
    font-size: 15px;
    padding: 3px 0;
    border-top: 1px solid var(--table-border);
  }
  .det-kv span:first-child {
    color: var(--muted);
  }
  .mono {
    font-family: var(--table-value-font);
    font-size: var(--table-value-size);
    font-weight: var(--table-value-weight);
  }
  .mono.small {
    font-size: 12px;
    word-break: break-all;
  }
  .det-msg {
    margin-top: 12px;
    font-size: 15px;
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
    margin-top: 10px;
  }
  .copy-tb {
    position: absolute;
    top: 8px;
    right: 8px;
    font-size: 12px;
    padding: 2px 10px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--control-fg);
    border-radius: 5px;
    cursor: pointer;
  }
  .tb {
    font-family: var(--font-mono);
    margin: 0;
    padding: 10px;
    background: var(--err-bg);
    border-radius: 5px;
    font-size: 12px;
    white-space: pre-wrap;
    word-break: break-word;
    max-height: 450px;
    overflow: auto;
  }
  .sel-list {
    margin: 4px 0 0;
    padding-left: 18px;
    max-height: 200px;
    overflow: auto;
  }
  .detail.left {
    border-left: none;
    border-right: 1px solid var(--border);
  }
  .empty {
    padding: 40px 20px;
    color: var(--muted);
    font-style: italic;
  }
</style>
