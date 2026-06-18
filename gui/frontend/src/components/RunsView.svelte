<script>
  import { view, passesTime } from "../lib/view.svelte.js";
  import { getAllErrors, getAllRuns } from "../lib/api.js";
  import { ts, STATUS_COLOR } from "../lib/constants.js";
  import TimeFilter from "./TimeFilter.svelte";

  const LIMITS = [50, 100, 200, 1000];
  // what to show: failures only (default), the run ledger, or both interleaved
  const MODES = [
    ["errors", "errors"],
    ["runs", "runs"],
    ["both", "errors + runs"],
  ];
  // how to render the model name in the `model` column
  const NAME_MODES = [
    ["fqn", "catalog.schema.table"],
    ["schema", "schema.table"],
    ["table", "table"],
  ];

  let mode = $state("errors");
  let limit = $state(50);
  let nameMode = $state("schema");
  let errs = $state([]);
  let runs = $state([]);
  let loading = $state(false);
  let loaded = $state(false);
  // which rows have their details dropdown open (by index) — so one button
  // can collapse them all.
  let openSet = $state(new Set());

  // the shared search (driven by the global top bar): model-name substring and
  // the tag-expression matches (set of full names, or null = inactive).
  let nameQuery = $derived(view.query);
  let tagMatchSet = $derived(view.tagMatches ? new Set(view.tagMatches) : null);

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
    const n = limit;
    const m = mode;
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

  // unified, newest-first timeline of the selected sources
  let items = $derived.by(() => {
    const out = [];
    if (mode !== "runs")
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
    if (mode !== "errors")
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
    // filter by model name (substring) and/or tag-expression match, then cap
    let filtered = out;
    const nq = nameQuery.trim().toLowerCase();
    if (nq) filtered = filtered.filter((o) => o.full_name.toLowerCase().includes(nq));
    if (tagMatchSet) filtered = filtered.filter((o) => tagMatchSet.has(o.full_name));
    filtered = filtered.filter((o) => passesTime(o.when, o.since, o.until));
    filtered.sort((a, b) => (b.when || "").localeCompare(a.when || ""));
    return filtered.slice(0, limit);
  });

  // is any filter (name / tag / time) narrowing the view right now?
  let hasFilter = $derived(
    !!nameQuery.trim() ||
      !!tagMatchSet ||
      (view.loadedMode === "exact"
        ? !!view.loadedExact
        : !!(view.loadedFrom || view.loadedTo)) ||
      view.intervalMode !== "any",
  );

  // render the model name per the chosen mode
  function displayName(full) {
    const parts = (full || "").split(".");
    if (nameMode === "table") return parts[parts.length - 1] || full;
    if (nameMode === "schema") return parts.slice(-2).join(".");
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
  <div class="bar">
    <span class="seg">
      {#each MODES as [val, label]}
        <button class="seg-btn" class:active={mode === val} onclick={() => (mode = val)}>
          {label}
        </button>
      {/each}
    </span>
    <button class="collapse" onclick={collapseAll} disabled={openSet.size === 0}>
      ▾ collapse all{openSet.size ? ` (${openSet.size})` : ""}
    </button>
    <TimeFilter />
    <span class="spacer"></span>
    <span class="seg">
      {#each NAME_MODES as [val, label]}
        <button
          class="seg-btn"
          class:active={nameMode === val}
          onclick={() => (nameMode = val)}>{label}</button
        >
      {/each}
    </span>
    <span class="seg">
      {#each LIMITS as n}
        <button class="seg-btn" class:active={limit === n} onclick={() => (limit = n)}>
          {n}
        </button>
      {/each}
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
    <div class="scroll">
      <table>
        <thead>
          <tr>
            <th>when</th>
            <th>model</th>
            <th>interval</th>
            <th>status<sup class="hint">(hover)</sup></th>
            <th>details</th>
          </tr>
        </thead>
        <tbody>
          {#each items as it, i (i)}
            <tr>
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
                      <details class="tags-det">
                        <summary>tags</summary>
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
    flex-wrap: wrap;
    gap: 8px;
    padding: 10px 16px;
    border-bottom: 1px solid var(--border);
  }
  .spacer {
    flex: 1;
  }
  .collapse {
    font-size: 12px;
    padding: 4px 10px;
    border-radius: 6px;
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
  .scroll {
    flex: 1;
    min-height: 0;
    overflow: auto;
    padding: 0 16px 16px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
  }
  thead th {
    position: sticky;
    top: 0;
    background: var(--bg);
    text-align: left;
    color: var(--muted);
    font-weight: 500;
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
    font-family: ui-monospace, monospace;
    font-size: 11px;
  }
  .when {
    color: var(--muted);
    white-space: nowrap;
  }
  .model {
    white-space: nowrap;
  }
  .model-name {
    font-weight: 600;
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
    font-size: 11px;
    line-height: 1.45;
    padding: 8px 10px;
    border-radius: 6px;
    box-shadow: 0 6px 18px rgba(0, 0, 0, 0.35);
    z-index: 30;
    pointer-events: none;
  }
  .hint {
    font-weight: 400;
    font-size: 9px;
    color: #ffd23f;
    margin-left: 1px;
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
  .tags-det summary {
    cursor: pointer;
    color: var(--muted);
    font-size: 11px;
  }
  .tags {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
    margin-top: 5px;
  }
  /* always-black chip so the yellow tags read the same in light & dark mode */
  .tag {
    font-size: 10px;
    font-weight: 700;
    padding: 1px 7px;
    border-radius: 10px;
    background: #000;
    border: 1px solid #000;
    color: #ffd23f;
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
    margin: 4px 0 0;
    padding: 8px;
    background: var(--err-bg);
    border-radius: 4px;
    font-family: ui-monospace, monospace;
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
