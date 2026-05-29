<script lang="ts">
  import ModelRow from "./lib/ModelRow.svelte";
  import ErrorsPanel from "./lib/ErrorsPanel.svelte";
  import { api } from "./lib/api";
  import type { SummaryEntry } from "./lib/types";
  import { onDestroy, onMount } from "svelte";

  let summary: SummaryEntry[] = [];
  let err: string | null = null;
  let lastUpdate: Date | null = null;
  const pollMs = 2000;
  let timer: number | undefined;

  async function load() {
    try {
      summary = await api.summary();
      err = null;
      lastUpdate = new Date();
    } catch (e) {
      err = (e as Error).message;
    }
  }

  onMount(() => {
    void load();
    timer = window.setInterval(load, pollMs);
  });
  onDestroy(() => clearInterval(timer));

  function fmtAgo(d: Date | null): string {
    if (!d) return "—";
    const sec = Math.round((Date.now() - d.getTime()) / 1000);
    if (sec < 2) return "just now";
    return `${sec}s ago`;
  }
</script>

<header>
  <div class="brand">bollhav</div>
  <div class="status">
    {#if err}
      <span class="err">backend: {err}</span>
    {:else}
      <span class="dim">polling every {pollMs / 1000}s · updated {fmtAgo(lastUpdate)}</span>
    {/if}
  </div>
</header>

<main>
  <section class="legend">
    <span class="lg pending"></span> pending
    <span class="lg running"></span> running
    <span class="lg applied"></span> applied
    <span class="lg blocked"></span> blocked
    <span class="lg error"></span> error
  </section>

  {#if summary.length === 0 && !err}
    <p class="empty">No models in library yet. Run a bollhav pipeline first.</p>
  {:else}
    {#each summary as entry (entry.full_name)}
      <ModelRow {entry} {pollMs} />
    {/each}
  {/if}

  <ErrorsPanel />
</main>

<style>
  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 14px 18px;
    border-bottom: 1px solid var(--border);
    background: var(--bg-2);
    position: sticky;
    top: 0;
    z-index: 10;
  }
  .brand {
    font-weight: 700;
    letter-spacing: 0.04em;
  }
  .status {
    font-size: 12px;
  }
  .dim {
    color: var(--fg-dim);
  }
  .err {
    color: var(--error);
  }

  main {
    max-width: 1400px;
    margin: 0 auto;
  }

  .legend {
    padding: 12px 18px;
    border-bottom: 1px solid var(--border);
    font-size: 12px;
    color: var(--fg-dim);
    display: flex;
    align-items: center;
    gap: 14px;
  }
  .lg {
    display: inline-block;
    width: 12px;
    height: 12px;
    border-radius: 2px;
    margin-right: 2px;
    vertical-align: middle;
  }
  .lg.pending {
    background: var(--pending);
  }
  .lg.running {
    background: var(--running);
  }
  .lg.applied {
    background: var(--applied);
  }
  .lg.blocked {
    background: var(--blocked);
  }
  .lg.error {
    background: var(--error);
  }

  .empty {
    padding: 24px 18px;
    color: var(--fg-dim);
  }
</style>
