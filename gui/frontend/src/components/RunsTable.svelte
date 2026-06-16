<script>
  import { STATUS_COLOR, ts } from "../lib/constants.js";

  let { runs } = $props();
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
        <tr>
          <td>
            <span class="dot" style="background:{STATUS_COLOR[r.status] || '#888'}"></span>
            {r.status}
          </td>
          <td class="mono">
            {r.since ? ts(r.since) + " → " + ts(r.until) : "whole table"}
          </td>
          <td class="mono">{ts(r.applied_at)}</td>
        </tr>
      {/each}
    </tbody>
  </table>
{/if}

<style>
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
  }
  th {
    text-align: left;
    color: #999;
    font-weight: 500;
    padding: 2px 4px;
  }
  td {
    padding: 3px 4px;
    border-top: 1px solid var(--table-border);
    vertical-align: top;
  }
  .mono {
    font-family: ui-monospace, monospace;
    font-size: 11px;
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
