<script>
  import { ts } from "../lib/constants.js";

  // `muted` renders resolved/historic errors de-emphasised. Empty state is
  // handled by the parent (DetailPanel splits active vs historic).
  let { errs, muted = false } = $props();
</script>

{#each errs as e}
  <div class="err" class:muted>
    <div class="err-top">
      <span class="err-type">{e.error_type}</span>
      <span class="mono err-time">{ts(e.created_at)}</span>
    </div>
    <div class="err-msg">{e.error_message}</div>
  </div>
{/each}

<style>
  .mono {
    font-family: ui-monospace, monospace;
    font-size: 11px;
  }
  .err {
    border-left: 3px solid #e45756;
    padding: 4px 8px;
    margin-bottom: 8px;
    background: var(--err-bg);
    border-radius: 0 4px 4px 0;
  }
  .err.muted {
    border-left-color: #9a9a9a;
    background: transparent;
    opacity: 0.65;
  }
  .err-top {
    display: flex;
    justify-content: space-between;
    gap: 8px;
  }
  .err-type {
    font-weight: 600;
    font-size: 12px;
    color: var(--err-accent);
  }
  .err.muted .err-type {
    color: var(--muted);
  }
  .err-time {
    color: #999;
  }
  .err-msg {
    font-size: 12px;
    color: var(--err-msg);
    margin-top: 2px;
  }
</style>
