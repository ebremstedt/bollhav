<script>
  import { view, matOk } from "../lib/view.svelte.js";
  import { ui } from "../lib/url.svelte.js";
  import ModelFilter from "./ModelFilter.svelte";
  import { getGaps } from "../lib/api.js";
  import { ts, GAP_COLORS } from "../lib/constants.js";
  import { nameSegments } from "../lib/graph.js";
  import TimeFilter from "./TimeFilter.svelte";
  import ResetBox from "./ResetBox.svelte";

  const GAP = GAP_COLORS.gap; // red — not filled yet (bar holes; covered green
  // lives in the .bar-wrap CSS gradient)

  // A model still needs backfilling if a temporal model has gaps, or a timeless
  // (whole-table) model hasn't loaded its single row yet.
  const needsBackfill = (g) =>
    (g.has_contract && g.gaps.length > 0) || (g.timeless && g.applied === false);

  let groups = $state([]); // [{full_name, has_contract, timeless, applied, gaps, …}]
  let loading = $state(false);
  let loaded = $state(false);
  // "with": the models that still have gaps (worst first); "without": the fully
  // covered / loaded / no-contract ones, by name
  // sort: by gap size (largest first by default) or by a part of the name
  const SORT_KEYS = [
    ["gap", "gap"],
    ["catalog", "catalog"],
    ["schema", "schema"],
    ["table", "table"],
  ];
  const part = (full, key) => {
    const p = (full || "").split(".");
    if (key === "table") return (p[p.length - 1] || "").toLowerCase();
    if (key === "schema") return (p[p.length - 2] || "").toLowerCase();
    return (p[p.length - 3] || "").toLowerCase();
  };
  function sorted(list) {
    const sign = ui.gapsDir === "asc" ? 1 : -1;
    return [...list].sort((a, b) => {
      const c =
        ui.gapsSort === "gap"
          ? (a.gap_seconds || 0) - (b.gap_seconds || 0)
          : part(a.full_name, ui.gapsSort).localeCompare(part(b.full_name, ui.gapsSort));
      return (c || a.full_name.localeCompare(b.full_name)) * sign;
    });
  }

  // the metric table's columns for one model: [heading, value] pairs
  function metricCols(g) {
    const cols = [];
    if (g.has_contract) {
      cols.push(["backfilled", `${g.pct_covered ?? "—"}%`]);
      if (g.gaps.length) {
        cols.push(["missing", humDur(g.gap_seconds)]);
        cols.push(["gaps", String(g.gaps.length)]);
      } else {
        cols.push(["missing", "none"]);
      }
      cols.push(["contract", `${dateOf(g.begin)} → ${dateOf(g.end)}`]);
    } else if (g.timeless && g.applied !== null) {
      cols.push(["backfilled", `${g.pct_covered ?? "—"}%`]);
      cols.push(["whole table", g.applied ? "loaded" : "not loaded yet"]);
    } else {
      cols.push(["contract", "no declared bounds"]);
    }
    for (const [status, n] of Object.entries(g.status_counts || {})) cols.push([status, String(n)]);
    return cols;
  }

  let tagMatchSet = $derived(view.tagMatches ? new Set(view.tagMatches) : null);
  // the site-wide materialization filter (null = inactive)
  let matSet = $derived(
    view.matFilter === "all" || !view.full
      ? null
      : new Set(view.full.nodes.filter((n) => n.type === "model" && matOk(n)).map((n) => n.name)),
  );

  // (re)load on env / refresh change — same trigger pattern as the grid tab
  $effect(() => {
    void view.env;
    void view.refreshAt;
    loading = true;
    getGaps()
      .then((d) => {
        groups = d;
        loaded = true;
        // project backfill score: the mean coverage % across every model that
        // has one (temporal w/ contract + timeless w/ state). Stashed in shared
        // view state so the bottom Legend can show it.
        const scored = d.filter((g) => g.pct_covered != null);
        view.gapScored = scored.length;
        view.gapScore = scored.length
          ? Math.round(scored.reduce((s, g) => s + g.pct_covered, 0) / scored.length)
          : null;
      })
      .catch(() => {
        groups = [];
        view.gapScore = null;
        view.gapScored = 0;
      })
      .finally(() => (loading = false));
  });

  // the time filter's interval range (from / to), applied to each model's
  // gaps: only the gaps overlapping the range count, and the missing total is
  // recomputed from them. Its "loaded" half is about runs, so it's hidden here.
  const bound = (v) => Date.parse((v || "").trim().replace(" ", "T"));
  const inRange = (since, until) => {
    if (view.intervalMode !== "range") return true;
    if (view.intervalFrom && Date.parse(until) < bound(view.intervalFrom)) return false;
    if (view.intervalTo && Date.parse(since) > bound(view.intervalTo)) return false;
    return true;
  };
  const withTime = (g) => {
    if (view.intervalMode !== "range" || !g.has_contract) return g;
    const gaps = g.gaps.filter((gp) => inRange(gp.since, gp.until));
    return { ...g, gaps, gap_seconds: gaps.reduce((t, gp) => t + (gp.seconds || 0), 0) };
  };

  // tag + time filter, then either the models needing backfill or the ones
  // without gaps (per `gapMode`), in the chosen sort order.
  // the right-hand panel: the clicked model, and the period clicked on its
  // bar (a covered island or a gap), or null for the model as a whole
  let panel = $state(null); // {full_name, begin, span: {since, until} | null, kind}

  function openPanel(g, span = null, kind = "model") {
    panel = { full_name: g.full_name, begin: g.begin, span, kind };
  }

  // a click on the bar: the point under the pointer, then the gap holding it
  // or else the covered island around it (between the neighbouring gaps)
  function pickBar(e, g) {
    e.stopPropagation();
    if (!g.has_contract || !g.begin || !g.end) return openPanel(g);
    const rect = e.currentTarget.getBoundingClientRect();
    const x = Math.min(1, Math.max(0, (e.clientX - rect.left) / rect.width));
    const b = Date.parse(g.begin);
    const t = b + x * (Date.parse(g.end) - b);
    const gaps = [...g.gaps].sort((p, q) => Date.parse(p.since) - Date.parse(q.since));
    const hit = gaps.find((gp) => Date.parse(gp.since) <= t && t < Date.parse(gp.until));
    if (hit) return openPanel(g, { since: hit.since, until: hit.until }, "gap");
    const before = gaps.filter((gp) => Date.parse(gp.until) <= t).at(-1);
    const after = gaps.find((gp) => Date.parse(gp.since) > t);
    openPanel(g, { since: before ? before.until : g.begin, until: after ? after.since : g.end }, "covered");
  }

  let rows = $derived.by(() => {
    const visible = groups
      .filter((g) => (!tagMatchSet || tagMatchSet.has(g.full_name)) && (!matSet || matSet.has(g.full_name)))
      .map(withTime);
    return sorted(visible.filter(ui.gapsMode === "with" ? needsBackfill : (g) => !needsBackfill(g)));
  });

  let gapCount = $derived(groups.filter(needsBackfill).length);

  let hasFilter = $derived(!!tagMatchSet || !!matSet || view.intervalMode === "range");

  function shortName(full) {
    return (full || "").split(".").slice(-2).join(".");
  }

  // seconds -> compact "54d 6h" / "3h 12m" / "45m"
  function humDur(sec) {
    if (sec == null) return "—";
    const d = Math.floor(sec / 86400);
    const h = Math.floor((sec % 86400) / 3600);
    const m = Math.floor((sec % 3600) / 60);
    if (d) return `${d}d ${h}h`;
    if (h) return `${h}h ${m}m`;
    if (m) return `${m}m`;
    return `${Math.round(sec)}s`;
  }

  // ISO date part for the bar's start / end labels, e.g. "2026-05-01"
  const dateOf = (s) => (s ? s.slice(0, 10) : "");

  // a gap's position within the contract window, as left% / width% of the bar
  function span(g, begin, end) {
    const b = Date.parse(begin);
    const total = Date.parse(end) - b || 1;
    const left = ((Date.parse(g.since) - b) / total) * 100;
    const width = ((Date.parse(g.until) - Date.parse(g.since)) / total) * 100;
    return `left:${Math.max(0, left)}%;width:${Math.max(0.4, width)}%`;
  }

  function copyRange(g, ev) {
    const btn = ev.currentTarget;
    navigator.clipboard
      .writeText(`RUN_SINCE=${g.since} RUN_UNTIL=${g.until}`)
      .then(() => {
        const prev = btn.textContent;
        btn.textContent = "copied!";
        setTimeout(() => (btn.textContent = prev), 1200);
      });
  }

</script>

<section class="gaps">
  <ModelFilter />
  <div class="bar">
    <!-- captioned boxes like the other tabs: the sort order, then which
         models to display -->
    <span class="group">
      <span class="group-label">sorting</span>
      <span class="group-body">
        <span class="sub">
          <span class="sub-label">models</span>
          <span class="seg">
            {#each SORT_KEYS as [val, label]}
              <button
                class="seg-btn two-line"
                class:active={ui.gapsSort === val}
                title="click to toggle ascending / descending"
                onclick={() => {
                  if (ui.gapsSort === val) ui.gapsDir = ui.gapsDir === "asc" ? "desc" : "asc";
                  else {
                    ui.gapsSort = val;
                    ui.gapsDir = val === "gap" ? "desc" : "asc";
                  }
                }}
              >
                <span>{label}</span>
                <span class="dir" class:hidden={ui.gapsSort !== val}
                  >{ui.gapsSort === val && ui.gapsDir === "desc" ? "descending" : "ascending"}</span
                >
              </button>
            {/each}
          </span>
        </span>
      </span>
    </span>
    <span class="group">
      <span class="group-label">display</span>
      <span class="group-body">
        <span class="sub">
          <span class="sub-label">time</span>
          <TimeFilter intervalOnly />
        </span>
        <span class="sub">
          <span class="sub-label">gaps</span>
          <span class="seg">
            <button
              class="seg-btn"
              class:active={ui.gapsMode === "with"}
              onclick={() => (ui.gapsMode = "with")}>with</button
            >
            <button
              class="seg-btn"
              class:active={ui.gapsMode === "without"}
              onclick={() => (ui.gapsMode = "without")}>without</button
            >
          </span>
        </span>
      </span>
    </span>
  </div>

  {#if loaded && rows.length === 0}
    <p class="empty">
      {#if hasFilter}
        No models match the current filters.
      {:else if gapCount === 0}
        Every model's contract is fully backfilled. 🎉
      {:else}
        Nothing to show.
      {/if}
    </p>
  {:else}
    <div class="body">
    <div class="scroll">
      <table>
        <tbody>
          {#each rows as g (g.full_name)}
            {@const open = ui.gapsOpen === g.full_name}
            {@const tags = (g.tags || []).filter((t) => !t.includes("."))}
            <tr
              class="row"
              class:open
              onclick={() => {
                ui.gapsOpen = open ? null : g.full_name;
                openPanel(g);
              }}
            >
              <td class="model" title={g.full_name}
                >{#each nameSegments(shortName(g.full_name), view.tagHighlights[g.full_name] || []) as s}<span
                    class:hit={s.hit}>{s.text}</span
                  >{/each}{#if g.timeless}<span
                    class="tl-badge"
                    title="Timeless — a whole-table model with no time axis. It's all-or-nothing (loaded or not), never a partial gap."
                    >timeless</span
                  >{/if}</td
              >
              <td class="track">
                {#if g.has_contract}
                  <div
                    class="bar-wrap pick"
                    title="click a period to pick it for a reset"
                    onclick={(e) => pickBar(e, g)}
                    role="button"
                    tabindex="0"
                    onkeydown={(e) => e.key === "Enter" && openPanel(g)}
                  >
                    {#each g.gaps as gp}
                      <div
                        class="hole"
                        style="{span(gp, g.begin, g.end)};background:{GAP}"
                      ></div>
                    {/each}
                    <span class="edge edge-l">{dateOf(g.begin)}</span>
                    <span class="edge edge-r">{dateOf(g.end)}</span>
                  </div>
                {:else if g.timeless && g.applied !== null}
                  <div
                    class="bar-wrap pick"
                    title="timeless — no time bounds"
                    onclick={(e) => pickBar(e, g)}
                    role="button"
                    tabindex="0"
                    onkeydown={(e) => e.key === "Enter" && openPanel(g)}
                  >
                    {#if !g.applied}
                      <div class="hole" style="left:0;width:100%;background:{GAP}"></div>
                    {/if}
                    <span class="edge edge-l">∞</span>
                    <span class="edge edge-r">∞</span>
                  </div>
                {:else}
                  <span class="nobounds">no declared contract bounds</span>
                {/if}
              </td>
            </tr>
            {#if open}
              {@const mcols = metricCols(g)}
              <tr class="detail-row">
                <!-- under the model name: the tags, behind a disclosure arrow
                     (same as runs → details → show) -->
                <td class="detail-left">
                  {#if tags.length}
                    <details class="tags-det">
                      <summary>show tags ({tags.length})</summary>
                      <div class="tags">
                        {#each tags as t}<span class="tag">{t}</span>{/each}
                      </div>
                    </details>
                  {/if}
                </td>
                <td>
                  <div class="detail">
                    <table class="mtable">
                      <thead>
                        <tr>{#each mcols as [label]}<th>{label}</th>{/each}</tr>
                      </thead>
                      <tbody>
                        <tr>{#each mcols as [, value]}<td>{value}</td>{/each}</tr>
                      </tbody>
                    </table>
                    {#if g.has_contract && g.gaps.length}
                      <div class="gaplist">
                        {#each g.gaps as gp}
                          <div class="gapline">
                            <span class="dot" style="background:{GAP}"></span>
                            <span class="mono">{ts(gp.since)}</span>
                            <span class="arrow">→</span>
                            <span class="mono">{ts(gp.until)}</span>
                            <span class="dur">{humDur(gp.seconds)}</span>
                            <button
                              class="copy"
                              title="copy as RUN_SINCE / RUN_UNTIL for a backfill run"
                              onclick={(ev) => {
                                ev.stopPropagation();
                                copyRange(gp, ev);
                              }}>copy</button
                            >
                          </div>
                        {/each}
                      </div>
                    {/if}
                  </div>
                </td>
              </tr>
            {/if}
          {/each}
        </tbody>
      </table>
    </div>
      {#if panel}
        <!-- the clicked model (and period): the same resets as the grid's panels -->
        <aside class="side">
          <div class="det-head">
            <span class="det-title" title={panel.full_name}>{shortName(panel.full_name)}</span>
            <button class="x" onclick={() => (panel = null)}>✕</button>
          </div>
          {#if panel.span}
            <div class="det-kv">
              <span>{panel.kind === "gap" ? "gap" : "covered period"}</span>
              <span class="mono">{ts(panel.span.since)} → {ts(panel.span.until)}</span>
            </div>
            <div class="det-note">
              {panel.kind === "gap"
                ? "a gap holds nothing applied, so there is nothing to reset in it — the range below is prefilled with it anyway"
                : "the range below is prefilled with this period"}
            </div>
          {:else}
            <div class="det-note">the whole model · click a bar to pick a period</div>
          {/if}
          {#if view.writable}
            <ResetBox
              name={panel.full_name}
              model
              span={panel.span}
              sample={panel.begin || ""}
              ondone={() => view.refreshAt++}
            />
          {/if}
        </aside>
      {/if}
    </div>
    <div class="foot">
      <span class="count">
        {#if loading}
          loading…
        {:else}
          {gapCount} model{gapCount === 1 ? "" : "s"} needing backfill
        {/if}
      </span>
      <span class="hint">· click a row to highlight it, a bar to pick a period</span>
    </div>
  {/if}
</section>

<style>
  .gaps {
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
    background: var(--bg);
    color: var(--fg);
  }
  /* the table's and footer's secondary text is black in light mode
     (theme.css) — the app-wide grey reads too faint next to the coverage
     bars. Scoped here so the bars above keep the normal grey labels. */
  .scroll,
  .foot {
    --muted: var(--gaps-muted);
  }
  .bar {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 14px;
    padding: 10px 16px;
    border-bottom: 1px solid var(--border);
  }
  .foot {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 6px;
    padding: 6px 16px;
    border-top: 1px solid var(--border);
  }
  .count {
    font-size: 12px;
    color: var(--muted);
  }
  .tl-badge {
    margin-left: 7px;
    font-size: 9px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--muted);
    border: 1px solid var(--control-border);
    border-radius: 4px;
    padding: 0 4px;
    vertical-align: middle;
    cursor: help;
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
  .seg-btn.two-line {
    display: inline-flex;
    flex-direction: column;
    align-items: center;
    line-height: 1.15;
    padding: 4px 12px;
  }
  .dir {
    font-size: var(--box-option-sub-size);
    opacity: 0.85;
    display: inline-block;
    min-width: 66px; /* "descending" — so the button never resizes */
    text-align: center;
  }
  .dir.hidden {
    visibility: hidden;
  }
  .body {
    flex: 1;
    min-height: 0;
    display: flex;
  }
  .bar-wrap.pick {
    cursor: pointer;
  }
  /* the right-hand panel — the grid's detail aside, in short */
  .side {
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
  .det-note {
    margin-top: 8px;
    font-size: 12px;
    color: var(--muted);
    font-style: italic;
  }
  .scroll {
    flex: 1;
    min-width: 0;
    flex: 1;
    min-height: 0;
    overflow: auto;
    padding: 8px 16px 16px;
  }
  table {
    font-family: var(--table-cell-font);
    font-size: var(--table-cell-size);
    font-weight: var(--table-cell-weight);
    border-collapse: separate;
    border-spacing: 0;
    width: 100%;
  }
  td {
    padding: 1px 10px 1px 0;
    vertical-align: middle;
  }
  .row {
    cursor: pointer;
  }
  .row:hover .model,
  .row.open .model {
    color: var(--fg);
  }
  .row:hover td {
    background: var(--control-bg);
  }
  /* the clicked row and its detail share one highlight, so it's obvious which
     model the detail below belongs to */
  .row.open td,
  .row.open:hover td,
  .detail-row td {
    background: var(--row-hi);
  }
  .row.open .model {
    box-shadow: inset 3px 0 0 #2e7d32;
    padding-left: 6px;
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
    /* room for a full catalog-less name at this size; the bars take the rest */
    max-width: 520px;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .model .hit {
    background: rgba(22, 163, 74, 0.3);
    border-radius: 3px;
  }
  .track {
    width: 100%;
  }
  .bar-wrap {
    position: relative;
    width: 100%;
    height: 24px;
    border-radius: 4px;
    /* dark green with a subtle top-down sheen (matches GAP_COLORS.covered) */
    background: linear-gradient(180deg, #3a934a 0%, #2e7d32 100%);
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.18);
    overflow: hidden;
    min-width: 160px;
  }
  /* contract start / end (and the timeless label) overlaid on the bar ends,
     painted over the holes (they come later in the DOM) */
  .edge {
    position: absolute;
    top: 0;
    bottom: 0;
    display: flex;
    align-items: center;
    padding: 0 8px;
    font-size: 13px;
    color: #fff;
    text-shadow: 0 1px 2px rgba(0, 0, 0, 0.55);
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
    pointer-events: none;
  }
  .edge-l {
    left: 0;
  }
  .edge-r {
    right: 0;
  }
  .hole {
    position: absolute;
    top: 0;
    bottom: 0;
    /* color set inline from the GAP constant (red — not yet filled) */
  }
  .nobounds {
    font-size: 11px;
    color: var(--muted);
    font-style: italic;
  }
  .detail-row td {
    padding-top: 0;
    padding-bottom: 12px;
  }
  .detail-left {
    vertical-align: top;
    padding-top: 4px;
  }
  /* summary · status counts · tags pill sit on one wrapping row; the gap
     list (if any) drops to its own row underneath */
  .detail {
    padding: 2px 0 0;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: center; /* the metrics sit centred under the bar */
    gap: 6px 14px;
  }
  /* the metrics, headings on top, values below */
  .mtable {
    font-family: var(--table-cell-font);
    font-size: var(--table-cell-size);
    font-weight: var(--table-cell-weight);
    border-collapse: collapse;
  }
  .mtable th {
    font-family: var(--table-head-font);
    font-size: var(--table-head-size);
    font-weight: var(--table-head-weight);
    color: var(--muted);
    text-align: left;
    padding: 1px 14px 1px 0;
    white-space: nowrap;
  }
  .mtable td {
    font-family: var(--table-cell-font);
    font-size: var(--table-cell-size);
    font-weight: var(--table-cell-weight);
    padding: 1px 14px 1px 0;
    white-space: nowrap;
  }
  /* the tags disclosure: a plain summary with the browser's side arrow */
  .tags-det summary {
    cursor: pointer;
    color: var(--muted);
    font-size: 11px;
  }
  .tags {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 5px;
  }
  .tag {
    font-size: 10px;
    color: var(--fg);
    background: rgba(22, 163, 74, 0.12);
    border-radius: 4px;
    padding: 1px 6px;
  }
  .gaplist {
    flex-basis: 100%;
    border-left: 2px solid var(--border);
    padding: 4px 0 4px 12px;
    margin: 2px 0 0;
  }
  .gapline {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 3px 0;
    font-size: 12px;
  }
  .dot {
    width: 9px;
    height: 9px;
    border-radius: 2px;
    display: inline-block;
    flex: 0 0 auto;
  }
  .mono {
    font-family: var(--table-value-font);
    font-size: var(--table-value-size);
    font-weight: var(--table-value-weight);
  }
  .arrow {
    color: var(--muted);
  }
  .dur {
    color: var(--muted);
    font-size: 11px;
    min-width: 70px;
  }
  .copy {
    font-size: 10px;
    padding: 2px 8px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--control-fg);
    border-radius: 4px;
    cursor: pointer;
  }
  .empty {
    padding: 32px 16px;
    color: var(--muted);
    font-style: italic;
  }
</style>
