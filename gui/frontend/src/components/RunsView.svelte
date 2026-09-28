<script>
  import { view, passesTime, matOk } from "../lib/view.svelte.js";
  import { ui } from "../lib/url.svelte.js";
  import ModelFilter from "./ModelFilter.svelte";
  import { getAllErrors, getAllRuns } from "../lib/api.js";
  import { ts, STATUS_COLOR } from "../lib/constants.js";
  import TimeFilter from "./TimeFilter.svelte";

  // rows: "height" = as many as the scrolling area holds (the
  // default), or a fixed count
  const LIMITS = ["height", 100, 365, "all"];
  // what to show: failures only (default), the run ledger, or both interleaved
  const MODES = [
    ["errors", "errors"],
    ["runs", "runs"],
    ["both", "all"],
  ];
  // how to render the model name in the `model` column
  // [key, label]: the top level the model name is shown from
  // (catalog → catalog.schema.table, schema → schema.table, table → table)
  const NAME_MODES = [
    ["fqn", "catalog"],
    ["schema", "schema"],
    ["table", "table"],
  ];

  // height: before any row is on screen, an estimate from the scroll
  // area's height (a row ≈ 31px, the header ≈ 40px); once rows exist, the
  // shortest of the first rows is measured for real. Rounded down to fives so
  // a resize by a few pixels doesn't refetch.
  let scrollH = $state(0); // bound from the scroll area; changing it re-measures
  let measured = $state(0);
  let estimate = $derived(scrollH ? Math.max(10, Math.floor((scrollH - 40) / 31)) : 0);
  let fit = $derived(measured || estimate);
  let effectiveLimit = $derived(ui.runsLimit === "height" ? fit : ui.runsLimit === "all" ? 100000 : ui.runsLimit);
  $effect(() => {
    void scrollH;
    void items;
    const rows = [...document.querySelectorAll(".runs tbody tr")].slice(0, 5);
    const head = document.querySelector(".runs thead");
    if (!rows.length || !head) return;
    const rowH = Math.min(...rows.map((r) => r.offsetHeight).filter(Boolean));
    if (!rowH) return;
    const n = Math.max(5, Math.floor((scrollH - head.offsetHeight) / rowH / 5) * 5);
    if (n !== measured) measured = n;
  });
  let errs = $state([]);
  let runs = $state([]);
  let loading = $state(false);
  let loaded = $state(false);
  // which rows have their details dropdown open (by index) — so one button
  // can collapse them all.
  let openSet = $state(new Set());
  let highlighted = $state(null); // index of the highlighted row (click a row)

  // the shared tag-expression matches (set of full names, or null = inactive)
  let tagMatchSet = $derived(view.tagMatches ? new Set(view.tagMatches) : null);
  // the site-wide materialization filter (null = inactive)
  let matSet = $derived(
    view.matFilter === "all" || !view.full
      ? null
      : new Set(view.full.nodes.filter((n) => n.type === "model" && matOk(n)).map((n) => n.name)),
  );

  function toggleRow(i, isOpen) {
    const next = new Set(openSet);
    if (isOpen) next.add(i);
    else next.delete(i);
    openSet = next;
  }
  function collapseAll() {
    openSet = new Set();
  }

  // each model's tags, keyed by full name — joined from the already-loaded
  // graph so a row can show which model failed and what tags it carried.
  let tagsByName = $derived(
    view.full
      ? Object.fromEntries(
          view.full.nodes
            .filter((n) => n.type === "model")
            .map((n) => [n.name, n.tags || []]),
        )
      : {},
  );

  // (re)load whenever the mode, limit, environment, or a refresh changes.
  $effect(() => {
    const n = effectiveLimit;
    if (!n) return; // the scroll area isn't measured yet
    const m = ui.runsShow;
    void view.env; // reload on env switch
    void view.refreshAt; // reload when the user hits refresh
    loading = true;
    openSet = new Set(); // rows change → drop stale open state
    const tasks = [];
    if (m === "errors" || m === "both")
      tasks.push(getAllErrors(n).then((d) => (errs = d)));
    else errs = [];
    if (m === "runs" || m === "both")
      tasks.push(getAllRuns(n).then((d) => (runs = d)));
    else runs = [];
    Promise.all(tasks)
      .catch(() => {})
      .finally(() => {
        loading = false;
        loaded = true;
      });
  });

  // unified timeline of the selected sources, newest first by default
  let items = $derived.by(() => {
    const out = [];
    if (ui.runsShow !== "runs")
      for (const e of errs)
        out.push({
          src: "error",
          when: e.created_at,
          full_name: e.full_name,
          since: e.since,
          until: e.until,
          label: e.error_type,
          message: e.error_message,
          traceback: e.traceback,
        });
    if (ui.runsShow !== "errors")
      for (const r of runs)
        out.push({
          src: "run",
          when: r.applied_at,
          full_name: r.full_name,
          since: r.since,
          until: r.until,
          status: r.status,
          blocked_reason: r.blocked_reason,
        });
    // filter by tag-expression match, then cap
    let filtered = out;
    if (tagMatchSet) filtered = filtered.filter((o) => tagMatchSet.has(o.full_name));
    if (matSet) filtered = filtered.filter((o) => matSet.has(o.full_name));
    filtered = filtered.filter((o) => passesTime(o.when, o.since, o.until));
    const sign = ui.runsOrder === "desc" ? 1 : -1;
    filtered.sort((a, b) => (b.when || "").localeCompare(a.when || "") * sign);
    return filtered.slice(0, effectiveLimit || filtered.length);
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

  // render the model name per the chosen mode
  function displayName(full) {
    const parts = (full || "").split(".");
    if (ui.runsName === "table") return parts[parts.length - 1] || full;
    if (ui.runsName === "schema") return parts.slice(-2).join(".");
    return full;
  }

  // the model's meaningful tags — drop the dotted fully-qualified-name tags
  function modelTags(name) {
    return (tagsByName[name] || []).filter((t) => !t.includes(".")).sort();
  }

  // copy the traceback; briefly flip the button label to confirm
  function copyTb(text, ev) {
    const btn = ev.currentTarget;
    navigator.clipboard.writeText(text).then(() => {
      btn.textContent = "copied!";
      setTimeout(() => (btn.textContent = "copy"), 1200);
    });
  }

</script>

<section class="runs">
  <ModelFilter />
  <div class="bar">
    <!-- captioned boxes: what the list is narrowed to, how it is sorted, how it shows -->
    <!-- what the list is narrowed to: failures / runs / both, and a time window -->
    <span class="group">
      <span class="group-label">filtering</span>
      <span class="group-body">
        <span class="sub">
          <span class="sub-label">show</span>
          <span class="seg">
            {#each MODES as [val, label]}
              <button class="seg-btn" class:active={ui.runsShow === val} onclick={() => (ui.runsShow = val)}>
                {label}
              </button>
            {/each}
          </span>
        </span>
        <span class="sub">
          <span class="sub-label">time</span>
          <TimeFilter />
        </span>
      </span>
    </span>
    <span class="group">
      <span class="group-label">sorting</span>
      <span class="group-body">
        <span class="sub">
          <span class="sub-label">time</span>
          <span class="seg">
            <button class="seg-btn" class:active={ui.runsOrder === "desc"} onclick={() => (ui.runsOrder = "desc")}
              >descending</button
            >
            <button class="seg-btn" class:active={ui.runsOrder === "asc"} onclick={() => (ui.runsOrder = "asc")}
              >ascending</button
            >
          </span>
        </span>
      </span>
    </span>
    <span class="group">
      <span class="group-label">display</span>
      <span class="group-body">
        <span class="sub">
          <span class="sub-label">number of runs</span>
          <span class="seg">
            {#each LIMITS as n}
              <button class="seg-btn" class:active={ui.runsLimit === n} onclick={() => (ui.runsLimit = n)}>
                {n}
              </button>
            {/each}
          </span>
        </span>
        <span class="sub">
          <span class="sub-label">model top level</span>
          <span class="seg">
            {#each NAME_MODES as [val, label]}
              <button
                class="seg-btn"
                class:active={ui.runsName === val}
                onclick={() => (ui.runsName = val)}>{label}</button
              >
            {/each}
          </span>
        </span>
        <span class="sub">
          <span class="sub-label">details</span>
          <button class="collapse" onclick={collapseAll} disabled={openSet.size === 0}>
            ▾ collapse{openSet.size ? ` (${openSet.size})` : ""}
          </button>
        </span>
      </span>
    </span>
  </div>

  {#if loaded && items.length === 0}
    <p class="empty">
      {#if hasFilter}
        No runs match the current filters.
      {:else}
        Nothing recorded in this environment yet. 🎉
      {/if}
    </p>
  {:else}
    <div class="scroll" bind:clientHeight={scrollH}>
      <table>
        <thead>
          <tr>
            <th>when</th>
            <th>model</th>
            <th>interval</th>
            <th>status</th>
            <th>details</th>
          </tr>
        </thead>
        <tbody>
          {#each items as it, i (i)}
            <!-- click a row to highlight it; clicks inside the details
                 toggle are the toggle's own -->
            <tr
              class:hi={highlighted === i}
              onclick={(e) => {
                if (e.target.closest("details")) return;
                highlighted = highlighted === i ? null : i;
              }}
            >
              <td class="mono when">{ts(it.when)}</td>
              <td class="model">
                <span class="model-name" title={it.full_name}>{displayName(it.full_name)}</span>
              </td>
              <td class="mono interval">
                {#if it.since}{ts(it.since)} → {ts(it.until)}{:else}<span class="whole">whole table</span>{/if}
              </td>
              <td
                class="status"
                class:has-msg={it.message || it.blocked_reason}
                data-tip={it.message || it.blocked_reason || ""}
              >
                {#if it.src === "run"}
                  <span class="dot" style="background:{STATUS_COLOR[it.status] || '#888'}"></span>{it.status}
                {:else}
                  <span class="dot err-dot"></span><span class="type">{it.label}</span>
                {/if}
              </td>
              <td class="detailcol">
                <details
                  open={openSet.has(i)}
                  ontoggle={(e) => toggleRow(i, e.currentTarget.open)}
                >
                  <summary>show</summary>
                  {#if it.message || it.blocked_reason}
                    <div class="msg-text">{it.message || it.blocked_reason}</div>
                  {/if}
                  <div class="det-meta">
                    <div class="det-row">
                      <span class="det-k">model</span>
                      <span class="mono det-fqn">{it.full_name}</span>
                    </div>
                    {#if modelTags(it.full_name).length}
                      <!-- same look as the gaps tab: a green "show tags" pill
                           that reveals green chips -->
                      <details class="tags-det">
                        <summary>show tags ({modelTags(it.full_name).length})</summary>
                        <span class="tags">
                          {#each modelTags(it.full_name) as t}
                            <span class="tag">{t}</span>
                          {/each}
                        </span>
                      </details>
                    {/if}
                  </div>
                  {#if it.traceback}
                    <div class="tb-wrap">
                      <button class="copy-tb" onclick={(ev) => copyTb(it.traceback, ev)}>
                        copy
                      </button>
                      <pre class="tb">{it.traceback}</pre>
                    </div>
                  {/if}
                </details>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
    <div class="foot">
      <span class="foot-hint">click a row to highlight it</span>
    </div>
  {/if}
</section>

<style>
  .runs {
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
    justify-content: center;
    flex-wrap: wrap;
    gap: 14px;
    padding: 10px 16px;
    border-bottom: 1px solid var(--border);
  }
  .foot {
    padding: 6px 16px;
    border-top: 1px solid var(--border);
    text-align: center;
  }
  .foot-hint {
    font-size: 12px;
    font-style: italic;
    color: var(--muted);
  }
  /* the highlighted row — same tint as the grid and gaps tabs */
  tr.hi td {
    background: var(--row-hi);
  }
  tr.hi td:first-child {
    box-shadow: inset 3px 0 0 #2e7d32;
  }
  tbody tr {
    cursor: pointer;
  }
  .collapse {
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
  .collapse:disabled {
    opacity: 0.5;
    cursor: not-allowed;
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
  .scroll {
    flex: 1;
    min-height: 0;
    overflow: auto;
    padding: 0 16px 16px;
  }
  table {
    font-family: var(--table-cell-font);
    font-size: var(--table-cell-size);
    font-weight: var(--table-cell-weight);
    width: 100%;
    border-collapse: collapse;
  }
  thead th {
    font-family: var(--table-head-font);
    font-size: var(--table-head-size);
    font-weight: var(--table-head-weight);
    position: sticky;
    top: 0;
    background: var(--bg);
    text-align: left;
    color: var(--muted);
    padding: 8px 8px 6px;
    border-bottom: 1px solid var(--border);
    z-index: 1;
  }
  td {
    padding: 7px 8px;
    border-top: 1px solid var(--table-border);
    vertical-align: top;
  }
  .mono {
    font-family: var(--table-value-font);
    font-size: var(--table-value-size);
    font-weight: var(--table-value-weight);
  }
  .when {
    color: var(--muted);
    white-space: nowrap;
  }
  .model {
    white-space: nowrap;
  }
  .model-name {
    font-family: var(--name-font);
    font-weight: var(--name-weight);
    font-size: var(--name-size);
  }
  .interval {
    color: var(--muted);
    white-space: nowrap;
  }
  .interval .whole {
    font-style: italic;
  }
  .status {
    white-space: nowrap;
  }
  .status.has-msg {
    position: relative;
    cursor: help;
  }
  .status.has-msg:hover::after {
    content: attr(data-tip);
    position: absolute;
    left: 0;
    top: calc(100% + 4px);
    width: 340px;
    max-width: 60vw;
    white-space: pre-wrap;
    word-break: break-word;
    background: #222;
    color: #fff;
    font-size: 15px;
    line-height: 1.45;
    padding: 9px 12px;
    border-radius: 8px;
    box-shadow: 0 6px 18px rgba(0, 0, 0, 0.35);
    z-index: 30;
    pointer-events: none;
  }
  .dot {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    margin-right: 5px;
    vertical-align: middle;
  }
  .err-dot {
    background: #e5202e;
  }
  .type {
    color: var(--err-accent);
    font-weight: 600;
  }
  .detailcol {
    width: 100%;
  }
  .msg-text {
    color: var(--err-msg);
    white-space: pre-wrap;
    word-break: break-word;
    margin-bottom: 4px;
  }
  details {
    margin-top: 4px;
  }
  summary {
    cursor: pointer;
    color: var(--muted);
    font-size: 11px;
  }
  .det-meta {
    margin: 6px 0 2px;
  }
  .det-row {
    display: flex;
    gap: 8px;
    align-items: baseline;
    margin-bottom: 4px;
    font-size: 11px;
  }
  .det-k {
    color: var(--muted);
    min-width: 42px;
    flex: 0 0 auto;
  }
  .det-fqn {
    color: var(--fg);
  }
  /* the tags disclosure: a plain summary with the browser's side arrow,
     like the row's own "show" */
  .tags-det summary {
    cursor: pointer;
    color: var(--muted);
    font-size: 11px;
    margin-top: 4px;
  }
  .tags {
    display: flex;
    flex-wrap: wrap;
    gap: 5px;
    margin-top: 5px;
  }
  .tag {
    font-size: 10px;
    color: var(--fg);
    background: rgba(22, 163, 74, 0.12);
    border-radius: 4px;
    padding: 1px 6px;
  }
  .tb-wrap {
    position: relative;
  }
  .copy-tb {
    position: absolute;
    top: 10px;
    right: 8px;
    font-size: 10px;
    padding: 2px 8px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--control-fg);
    border-radius: 4px;
    cursor: pointer;
    z-index: 1;
  }
  .copy-tb:hover {
    opacity: 0.85;
  }
  .tb {
    font-family: var(--font-mono);
    margin: 4px 0 0;
    padding: 8px;
    background: var(--err-bg);
    border-radius: 4px;
    font-size: 11px;
    white-space: pre-wrap;
    word-break: break-word;
    max-height: 320px;
    overflow: auto;
  }
  .empty {
    padding: 32px 16px;
    color: var(--muted);
    font-style: italic;
  }
</style>
