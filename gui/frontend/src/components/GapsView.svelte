<script>
  import { view } from "../lib/view.svelte.js";
  import { getGaps } from "../lib/api.js";
  import { ts, GAP_COLORS } from "../lib/constants.js";
  import { nameSegments } from "../lib/graph.js";

  const GAP = GAP_COLORS.gap; // gray — not filled yet (bar holes; covered green
  // lives in the .bar-wrap CSS gradient)

  // A model still needs backfilling if a temporal model has gaps, or a timeless
  // (whole-table) model hasn't loaded its single row yet.
  const needsBackfill = (g) =>
    (g.has_contract && g.gaps.length > 0) || (g.timeless && g.applied === false);

  let groups = $state([]); // [{full_name, has_contract, timeless, applied, gaps, …}]
  let loading = $state(false);
  let loaded = $state(false);
  let showAll = $state(false); // also list fully-covered / no-contract models
  let selected = $state(null); // full_name of the expanded model

  let tagMatchSet = $derived(view.tagMatches ? new Set(view.tagMatches) : null);

  // (re)load on env / refresh change — same trigger pattern as the grid tab
  $effect(() => {
    void view.env;
    void view.refreshAt;
    loading = true;
    selected = null;
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

  // name + tag filter, then the models needing backfill first (worst gap first);
  // `showAll` then appends the fully-covered / loaded / no-contract models.
  let rows = $derived.by(() => {
    const nq = view.query.trim().toLowerCase();
    const visible = groups
      .filter((g) => !nq || g.full_name.toLowerCase().includes(nq))
      .filter((g) => !tagMatchSet || tagMatchSet.has(g.full_name));
    const primary = visible.filter(needsBackfill);
    primary.sort(
      (a, b) => b.gap_seconds - a.gap_seconds || a.full_name.localeCompare(b.full_name),
    );
    if (!showAll) return primary;
    const rest = visible
      .filter((g) => !needsBackfill(g))
      .sort((a, b) => a.full_name.localeCompare(b.full_name));
    return [...primary, ...rest];
  });

  let gapCount = $derived(groups.filter(needsBackfill).length);

  let hasFilter = $derived(!!view.query.trim() || !!tagMatchSet);

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
  <div class="bar">
    <span class="count">
      {#if loading}
        loading…
      {:else}
        {gapCount} model{gapCount === 1 ? "" : "s"} needing backfill
      {/if}
    </span>
    <span class="spacer"></span>
    <button class="toggle" class:on={showAll} onclick={() => (showAll = !showAll)}>
      {showAll ? "▾ all models" : "▸ only models with gaps"}
    </button>
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
    <div class="scroll">
      <table>
        <tbody>
          {#each rows as g (g.full_name)}
            {@const open = selected === g.full_name}
            <tr
              class="row"
              class:open
              class:dim={!needsBackfill(g)}
              onclick={() => (selected = open ? null : g.full_name)}
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
                  <div class="bar-wrap" title="{ts(g.begin)} → {ts(g.end)}">
                    {#each g.gaps as gp}
                      <div
                        class="hole"
                        style="{span(gp, g.begin, g.end)};background:{GAP}"
                      ></div>
                    {/each}
                  </div>
                {:else if g.timeless && g.applied !== null}
                  <div
                    class="bar-wrap"
                    title="whole-table model — {g.applied
                      ? 'loaded'
                      : 'not loaded yet'}"
                  >
                    {#if !g.applied}
                      <div class="hole" style="left:0;width:100%;background:{GAP}"></div>
                    {/if}
                  </div>
                {:else}
                  <span class="nobounds">no declared contract bounds</span>
                {/if}
              </td>
              <td class="stat pct">
                {#if g.has_contract || (g.timeless && g.applied !== null)}{g.pct_covered ??
                    "—"}%{/if}
              </td>
              <td class="stat gapdur">
                {#if g.has_contract && g.gaps.length}
                  <b>{humDur(g.gap_seconds)}</b> · {g.gaps.length} gap{g.gaps
                    .length === 1
                    ? ""
                    : "s"}
                {:else if g.has_contract}
                  <span class="ok">fully backfilled</span>
                {:else if g.timeless && g.applied === true}
                  <span class="ok">whole table loaded</span>
                {:else if g.timeless && g.applied === false}
                  whole table — not loaded
                {/if}
              </td>
            </tr>
            {#if open && g.has_contract && g.gaps.length}
              <tr class="detail-row">
                <td></td>
                <td colspan="3">
                  <div class="gaplist">
                    <div class="window">
                      contract window <span class="mono">{ts(g.begin)}</span> →
                      <span class="mono">{ts(g.end)}</span>
                    </div>
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
                </td>
              </tr>
            {/if}
          {/each}
        </tbody>
      </table>
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
  .bar {
    display: flex;
    align-items: center;
    gap: 14px;
    padding: 10px 16px;
    border-bottom: 1px solid var(--border);
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
  .spacer {
    flex: 1;
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
    color: #fff;
  }
  .scroll {
    flex: 1;
    min-height: 0;
    overflow: auto;
    padding: 8px 16px 16px;
  }
  table {
    border-collapse: separate;
    border-spacing: 0;
    font-size: 12px;
    width: 100%;
  }
  td {
    padding: 5px 10px 5px 0;
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
  .row.dim {
    opacity: 0.6;
  }
  .model {
    white-space: nowrap;
    font-weight: 600;
    max-width: 320px;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .model .hit {
    color: #16a34a;
  }
  .track {
    width: 100%;
  }
  .bar-wrap {
    position: relative;
    height: 16px;
    border-radius: 4px;
    /* fresh emerald with a subtle top-down sheen (matches COVERED) */
    background: linear-gradient(180deg, #4ed47a 0%, #2da44e 100%);
    border: 1px solid rgba(0, 0, 0, 0.18);
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.18);
    overflow: hidden;
    min-width: 180px;
  }
  .hole {
    position: absolute;
    top: 0;
    bottom: 0;
    /* color set inline from the GAP constant (gray — not yet filled) */
  }
  .nobounds {
    font-size: 11px;
    color: var(--muted);
    font-style: italic;
  }
  .stat {
    white-space: nowrap;
    text-align: right;
    font-size: 11px;
    color: var(--muted);
  }
  .pct {
    width: 48px;
    font-variant-numeric: tabular-nums;
  }
  .gapdur {
    width: 150px;
  }
  .gapdur .ok {
    color: var(--muted);
    font-style: italic;
  }
  .detail-row td {
    padding-top: 0;
    padding-bottom: 10px;
  }
  .gaplist {
    border-left: 2px solid var(--border);
    padding: 4px 0 4px 12px;
    margin: 2px 0 6px;
  }
  .window {
    font-size: 11px;
    color: var(--muted);
    margin-bottom: 6px;
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
    font-family: ui-monospace, monospace;
    font-size: 11px;
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
