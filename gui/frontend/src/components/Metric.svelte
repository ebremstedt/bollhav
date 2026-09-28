<script>
  // A metric: the figure inside a circle, its caption underneath.
  //
  //   <Metric value="33%" label="backfilled" />
  //   <Metric value="50d" label="missing, 1 gap" tone="gap" title="50d 3h" />
  //
  // `tone` colours it: "ok" green, "gap" red, anything else neutral. Keep
  // `value` short — a percentage, a count, a duration's leading unit — so it
  // fits the circle; put the long form in `title`.
  let { value, label, tone = null, title = undefined } = $props();
</script>

<span class="metric" class:ok={tone === "ok"} class:gap={tone === "gap"} {title}>
  <span class="circle">{value}</span>
  <span class="cap">{label}</span>
</span>

<style>
  .metric {
    display: inline-flex;
    flex-direction: column;
    align-items: center;
    gap: 3px;
    min-width: 56px;
  }
  .circle {
    width: 46px;
    height: 46px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 13px;
    font-weight: 700;
    font-variant-numeric: tabular-nums;
    border: 2px solid var(--control-border);
    background: var(--control-bg);
    color: var(--fg);
  }
  .cap {
    font-size: 11px;
    color: var(--fg);
    white-space: nowrap;
    text-align: center;
  }
  .ok .circle {
    color: var(--fg);
    border-color: #16a34a;
    background: rgba(22, 163, 74, 0.12);
  }
  .gap .circle {
    color: var(--fg);
    border-color: #8b2332;
    background: rgba(139, 35, 50, 0.12);
  }
</style>
