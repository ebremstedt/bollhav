<script>
  import { LEGEND_KINDS, LEGEND_SOURCES } from "../lib/constants.js";
</script>

<footer class="legend">
  <span
    class="grp"
    data-tip="Managed models are state-tracked and gated. Drawn with a yellow outline; the pill reads `model`, with a second pill for its temporality."
    >Models (managed)</span
  >
  {#each LEGEND_KINDS as k}
    <span class="item" data-tip={k.tip}>
      <span class="sw" style="background:{k.c}"></span>{k.label}
    </span>
  {/each}

  <span class="sep"></span>

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

  <span class="legend-hint">hover any item for an explanation</span>
</footer>

<style>
  .legend {
    display: flex;
    align-items: center;
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
  .legend-hint {
    margin-left: auto;
    color: var(--legend-hint);
    font-style: italic;
  }
</style>
