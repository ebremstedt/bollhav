<script>
  import { onMount } from "svelte";
  import { selection, info } from "./lib/selection.svelte.js";
  import { init, view } from "./lib/view.svelte.js";
  import { ui, restoreUrl, urlQuery } from "./lib/url.svelte.js";
  import Header from "./components/Header.svelte";
  import Flow from "./components/Flow.svelte";
  import DetailPanel from "./components/DetailPanel.svelte";
  import MetaPanel from "./components/MetaPanel.svelte";
  import Legend from "./components/Legend.svelte";
  import LineageBar from "./components/LineageBar.svelte";
  import RunsView from "./components/RunsView.svelte";
  import GridView from "./components/GridView.svelte";
  import GapsView from "./components/GapsView.svelte";
  import ModelsView from "./components/ModelsView.svelte";

  // the address bar is read first (env, tab, every choice), then the graph
  // loads and the link's model / filter is applied
  onMount(() => init(restoreUrl()));

  // mirror every choice into the address bar, so the link can be shared.
  // Replaced, not pushed: the back button shouldn't step through each click.
  $effect(() => {
    if (!ui.ready) return;
    const q = urlQuery();
    if (location.search !== (q ? `?${q}` : "")) {
      history.replaceState(null, "", q ? `?${q}` : location.pathname);
    }
  });
</script>

<div class="wrap" class:dark={ui.dark}>
  <Header bind:dark={ui.dark} />
  {#if view.tab === "runs"}
    <RunsView />
  {:else if view.tab === "grid"}
    <GridView />
  {:else if view.tab === "gaps"}
    <GapsView />
  {:else if view.tab === "models"}
    <ModelsView />
  {:else}
    <LineageBar />
    <div class="body">
      {#if info.name}
        <MetaPanel />
      {/if}
      <Flow dark={ui.dark} />
      {#if selection.name}
        <DetailPanel />
      {/if}
    </div>
    <!-- why the lineage opens on a single model rather than the whole DAG -->
    <div class="foot">
      <span class="hint"
        >a random model (with its upstreams) is shown each time the lineage loads or the filter
        is cleared, to keep the app running fast</span
      >
    </div>
  {/if}
  <Legend />
</div>

<style>
  .wrap {
    font-family: var(--font-ui);
    width: 100vw;
    height: 100vh;
    display: flex;
    flex-direction: column;
  }
  .body {
    flex: 1;
    min-height: 0;
    display: flex;
  }
  /* the same footer hint as the runs and grid tabs */
  .foot {
    padding: 6px 16px;
    border-top: 1px solid var(--border);
    background: var(--bg);
    text-align: center;
  }
  .hint {
    font-size: 12px;
    font-style: italic;
    color: var(--muted);
  }
</style>
