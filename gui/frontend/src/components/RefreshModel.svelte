<script>
  // A "refresh" pill for one model: recompute just that model in the backend's
  // cache (its status lights, gaps, grid row, recent runs and errors), then
  // reload what's on screen. Only shown when the selection is served from the
  // cache; a dev environment is read live anyway.
  import { view, refreshModel } from "../lib/view.svelte.js";

  let { name } = $props();
  let busy = $derived(view.modelRefreshing === name);
</script>

{#if view.freshness.cached && name}
  <button
    class="pill"
    class:busy
    disabled={view.modelRefreshing != null}
    title="recompute this model now: its status, gaps, runs and errors"
    onclick={() => refreshModel(name)}>refresh</button
  >
{/if}

<style>
  .pill {
    font-family: var(--btn-font);
    font-size: 11px;
    line-height: 1.4;
    padding: 1px 9px;
    border-radius: 999px;
    border: 1px solid var(--control-border);
    background: var(--control-bg);
    color: var(--control-fg);
    cursor: pointer;
    white-space: nowrap;
  }
  .pill:hover {
    border-color: var(--fg);
    color: var(--fg);
  }
  .pill:disabled {
    cursor: default;
    opacity: 0.6;
  }
  /* while this model recomputes: the pill keeps its text and pulses green */
  .pill.busy {
    opacity: 1;
    color: #1b1b1f;
    border-color: transparent;
    animation: pulse-green 2.4s ease-in-out infinite;
  }
  @keyframes pulse-green {
    0%,
    100% {
      background: #7fc8a0;
      box-shadow: 0 0 0 0 rgba(127, 200, 160, 0.55);
    }
    50% {
      background: #a8dcbf;
      box-shadow: 0 0 0 5px rgba(127, 200, 160, 0);
    }
  }
  @media (prefers-reduced-motion: reduce) {
    .pill.busy {
      animation: none;
      background: #7fc8a0;
    }
  }
</style>
