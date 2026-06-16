<script>
  // Upstream edge with a two-tone label: the contract level in neutral grey and
  // the freshness bound (❄) in blue, to match the blue 'stale' status light.
  import { BaseEdge, EdgeLabel, getBezierPath } from "@xyflow/svelte";

  let {
    id,
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    markerEnd,
    data,
  } = $props();

  let p = $derived(
    getBezierPath({
      sourceX,
      sourceY,
      sourcePosition,
      targetX,
      targetY,
      targetPosition,
    }),
  );

  // U+2744 + U+FE0E (text variation selector) renders the snowflake as a glyph,
  // not a colour emoji — so it inherits the blue `.fresh` text colour.
  const SNOW = "❄︎";
</script>

<BaseEdge {id} path={p[0]} {markerEnd} style="stroke:#9aa0a6;stroke-width:1.5" />
{#if data?.contract || data?.freshness}
  <EdgeLabel x={p[1]} y={p[2]} transparent>
    <div class="lbl">
      {#if data?.contract}<span class="contract">{data.contract}</span>{/if}
      {#if data?.freshness}<span class="fresh">{SNOW}{data.freshness}</span>{/if}
    </div>
  </EdgeLabel>
{/if}

<style>
  .lbl {
    display: flex;
    flex-direction: column;
    gap: 1px;
    align-items: center;
    font-size: 10px;
    font-weight: 600;
    line-height: 1.25;
    background: #1f2227;
    padding: 2px 6px;
    border-radius: 5px;
    white-space: nowrap;
  }
  .contract {
    color: #cfd3d8;
    text-transform: uppercase;
    letter-spacing: 0.3px;
  }
  .fresh {
    color: #4aa3ff;
  }
</style>
