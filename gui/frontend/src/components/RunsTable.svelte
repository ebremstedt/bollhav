<script>
  import { STATUS_COLOR, ts } from "../lib/constants.js";

  // `selected` is a Set of "since|until" keys and `onpick(run)` a callback:
  // with both, rows are pickable (the side panel's interval resets)
  let { runs, selected = null, onpick = null } = $props();
  const key = (r) => `${r.since}|${r.until}`;
</script>

{#if runs.length === 0}
  <p class="empty">No state rows (not bootstrapped, or never run).</p>
{:else}
  <table>
    <thead>
      <tr><th>status</th><th>window</th><th>applied</th></tr>
    </thead>
    <tbody>
      {#each runs as r}
        <tr class:pickable={!!onpick} class:sel={selected?.has(key(r))} onclick={() => onpick?.(r)}>
          <td class="status">
            <span class="dot" style="background:{STATUS_COLOR[r.status] || '#888'}"></span>
            {r.status}
          </td>
          <td class="mono">
            {#if r.since}
              <span class="nw">{ts(r.since)} →</span> <span class="nw">{ts(r.until)}</span>
            {:else}
              whole table
            {/if}
          </td>
          <td class="mono">{ts(r.applied_at)}</td>
        </tr>
      {/each}
    </tbody>
  </table>
{/if}

<style>
  table {
    font-family: var(--table-cell-font);
    font-size: var(--table-cell-size);
    font-weight: var(--table-cell-weight);
    width: 100%;
    border-collapse: collapse;
  }
  th {
    font-family: var(--table-head-font);
    font-size: var(--table-head-size);
    font-weight: var(--table-head-weight);
    text-align: left;
    color: #999;
    padding: 2px 4px;
  }
  td {
    padding: 3px 4px;
    border-top: 1px solid var(--table-border);
    vertical-align: top;
  }
  .status,
  .nw {
    white-space: nowrap;
  }
  tr.pickable {
    cursor: pointer;
  }
  tr.sel td {
    background: var(--row-hi);
  }
  .mono {
    font-family: var(--table-value-font);
    font-size: var(--table-value-size);
    font-weight: var(--table-value-weight);
  }
  .dot {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    margin-right: 4px;
  }
  .empty {
    color: #999;
    font-size: 12px;
    font-style: italic;
  }
</style>
