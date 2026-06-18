<script>
  import { LEGEND_KINDS, LEGEND_SOURCES, STATUS_COLOR } from "../lib/constants.js";
  import { view } from "../lib/view.svelte.js";

  let showHelp = $state(false);

  // run-status colours shown on the runs + grid tabs
  const RUN_STATUSES = [
    ["applied", "applied (ok)"],
    ["running", "running"],
    ["blocked", "blocked"],
    ["error", "error"],
    ["pending", "pending"],
  ];
</script>

<footer class="legend">
  {#if view.tab === "lineage"}
  <span
    class="grp"
    data-tip="Sources are external inputs bollhav does NOT manage — no state, never gated, fixed across environments. Drawn as a striped, dashed card."
    >Sources (unmanaged)</span
  >
  {#each LEGEND_SOURCES as s}
    <span class="item" data-tip={s.tip}>
      <span class="sw striped" style="border-color:{s.c}"></span>{s.label}
    </span>
  {/each}

  <span class="sep"></span>

  <span
    class="grp"
    data-tip="Managed models are state-tracked and gated. Drawn with a yellow outline; the pill reads `model`, with a second pill for its temporality."
    >Models (managed)</span
  >
  {#each LEGEND_KINDS.slice(0, 1) as k}
    <span class="item" data-tip={k.tip}>
      <span class="sw" style="background:{k.c}"></span>{k.label}
    </span>
  {/each}
  <span class="pipe">|</span>
  {#each LEGEND_KINDS.slice(1) as k}
    <span class="item" data-tip={k.tip}>
      <span class="sw" style="background:{k.c}"></span>{k.label}
    </span>
  {/each}

  <span class="sep"></span>

  <span
    class="item"
    data-tip="Arrow from an input to the model that consumes it (an upstream model or an external source)."
  >
    <span class="edge"></span>depends on
  </span>
  <span
    class="item"
    data-tip="Each upstream edge is labelled with the contract it enforces: the completeness level (exists / window / through / whole) and, when set, a freshness bound (❄ ≤age, latest or all)."
  >
    <span class="edge"></span>contract · <span class="snow">❄︎</span> freshness
  </span>
  <span
    class="item"
    data-tip="A recent run failed and has not yet succeeded on a rerun. Clears once that interval reruns without error."
  >
    <span class="sw dot"></span>error
  </span>
  <span class="item" data-tip="A run is currently in progress for this model.">
    <span class="sw dot running"></span>running
  </span>
  <span
    class="item"
    data-tip="Blocked on completeness: an upstream model this depends on hasn't produced the data yet."
  >
    <span class="sw dot blocked"></span>blocked
  </span>
  <span
    class="item"
    data-tip="Blocked on freshness: an upstream is present but too old for this model's freshness contract (present-but-stale)."
  >
    <span class="sw dot stale"></span>stale
  </span>
  {:else}
    {#each RUN_STATUSES as [k, label]}
      <span class="item">
        <span class="sw dot" style="background:{STATUS_COLOR[k]}"></span>{label}
      </span>
    {/each}
  {/if}

  <span class="help-wrap">
    <button
      class="help-btn"
      onclick={() => (showHelp = !showHelp)}
      aria-expanded={showHelp}>🏷 tag syntax</button
    >
    {#if showHelp}
      <div class="help-pop">
        <div class="help-head">
          <strong>Filter by tag / tag expression</strong>
          <button class="help-x" onclick={() => (showHelp = false)}>✕</button>
        </div>
        <p>
          Type a tag or an expression; matching models stay (with their
          upstreams) and the matched part of each name turns green.
        </p>
        <ul>
          <li><code>clean</code> — has tag <code>clean</code> (bare = <code>[clean]</code>)</li>
          <li><code>[a &amp; b]</code> — a <em>and</em> b</li>
          <li><code>[a | b]</code> — a <em>or</em> b</li>
          <li><code>[a][b]</code> — a <em>or</em> b (separate groups)</li>
          <li><code>[not:a]</code> — <em>exclude</em> a</li>
          <li><code>[(a|b) &amp; c]</code> — parentheses to group</li>
        </ul>
        <div class="help-eg">Examples</div>
        <ul>
          <li>
            <code>[(customer|order) &amp; fact]</code><br />customer or order facts
          </li>
          <li><code>[clean &amp; not:fact]</code><br />clean, but not facts</li>
          <li><code>[view][dimension]</code><br />views or dimensions</li>
          <li>
            <code>[consumption &amp; not:view]</code><br />consume layer, excluding views
          </li>
        </ul>
        <p class="help-foot">
          Case-insensitive. Tags are auto-derived from each model's
          catalog / schema / name (plus any explicit ones).
        </p>
      </div>
    {/if}
  </span>
</footer>

<style>
  .legend {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 14px;
    flex-wrap: wrap;
    padding: 8px 14px;
    border-top: 1px solid var(--border);
    background: var(--bg);
    color: var(--legend-fg);
    font-size: 12px;
  }
  .grp {
    font-weight: 600;
    color: var(--legend-grp);
    cursor: help;
  }
  .pipe {
    color: var(--sep);
  }
  .item {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    cursor: help;
  }
  /* custom hover tooltip (native title is too flaky/slow) */
  [data-tip] {
    position: relative;
  }
  [data-tip]:hover::after {
    content: attr(data-tip);
    position: absolute;
    left: 0;
    bottom: calc(100% + 9px);
    width: 240px;
    white-space: normal;
    background: #222;
    color: #fff;
    font-weight: 400;
    line-height: 1.35;
    padding: 7px 9px;
    border-radius: 6px;
    box-shadow: 0 4px 14px rgba(0, 0, 0, 0.3);
    z-index: 50;
    pointer-events: none;
  }
  [data-tip]:hover::before {
    content: "";
    position: absolute;
    left: 14px;
    bottom: calc(100% + 3px);
    border: 6px solid transparent;
    border-top-color: #222;
    z-index: 50;
    pointer-events: none;
  }
  .sw {
    width: 13px;
    height: 13px;
    border-radius: 3px;
    display: inline-block;
  }
  .sw.striped {
    background: repeating-linear-gradient(
      45deg,
      var(--stripe-base),
      var(--stripe-base) 3px,
      var(--stripe-line) 3px,
      var(--stripe-line) 6px
    );
    border: 1px dashed #888;
    border-radius: 2px;
  }
  .sw.dot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    background: #e5202e;
  }
  .sw.dot.running {
    background: #4ade80;
  }
  .sw.dot.blocked {
    background: #f58518;
  }
  .sw.dot.stale {
    background: #2f8fff;
  }
  .snow {
    color: #4aa3ff;
    font-weight: 700;
  }
  .edge {
    width: 22px;
    border-top: 2px dashed #9aa0a6;
    display: inline-block;
  }
  .sep {
    width: 1px;
    height: 16px;
    background: var(--sep);
  }
  .help-wrap {
    position: relative;
    display: inline-flex;
  }
  .help-btn {
    font-size: 12px;
    padding: 3px 9px;
    border-radius: 6px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--control-fg);
    cursor: pointer;
  }
  .help-pop {
    position: absolute;
    bottom: calc(100% + 8px);
    right: 0;
    width: 300px;
    background: #222;
    color: #fff;
    border-radius: 8px;
    padding: 11px 13px;
    box-shadow: 0 6px 20px rgba(0, 0, 0, 0.4);
    z-index: 60;
    font-style: normal;
    line-height: 1.4;
  }
  .help-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    margin-bottom: 6px;
  }
  .help-x {
    border: none;
    background: transparent;
    color: #aaa;
    cursor: pointer;
    font-size: 13px;
    line-height: 1;
  }
  .help-x:hover {
    color: #fff;
  }
  .help-pop p {
    margin: 6px 0;
    color: #d6d9de;
  }
  .help-pop ul {
    margin: 6px 0;
    padding-left: 4px;
    list-style: none;
  }
  .help-pop li {
    padding: 2px 0;
  }
  .help-eg {
    margin-top: 8px;
    border-top: 1px solid #3a3a3a;
    padding-top: 6px;
    font-weight: 700;
  }
  .help-eg + ul li {
    padding: 4px 0;
    color: #d6d9de;
  }
  .help-pop em {
    color: #fff;
    font-style: normal;
    font-weight: 600;
  }
  .help-pop code {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 11px;
    background: #15803d;
    color: #fff;
    padding: 1px 5px;
    border-radius: 4px;
  }
  .help-foot {
    border-top: 1px solid #3a3a3a;
    padding-top: 6px;
    font-size: 11px;
  }
</style>
