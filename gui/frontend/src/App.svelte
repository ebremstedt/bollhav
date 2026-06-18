<script>
  import { onMount } from "svelte";
  import { selection, info } from "./lib/selection.svelte.js";
  import { init, view } from "./lib/view.svelte.js";
  import Header from "./components/Header.svelte";
  import Flow from "./components/Flow.svelte";
  import DetailPanel from "./components/DetailPanel.svelte";
  import MetaPanel from "./components/MetaPanel.svelte";
  import Legend from "./components/Legend.svelte";
  import LineageBar from "./components/LineageBar.svelte";
  import RunsView from "./components/RunsView.svelte";
  import GridView from "./components/GridView.svelte";

  let dark = $state(true);

  onMount(init);
</script>

<div class="wrap" class:dark>
  <Header bind:dark />
  {#if view.tab === "runs"}
    <RunsView />
  {:else if view.tab === "grid"}
    <GridView />
  {:else}
    <LineageBar />
    <div class="body">
      {#if info.name}
        <MetaPanel />
      {/if}
      <Flow {dark} />
      {#if selection.name}
        <DetailPanel />
      {/if}
    </div>
  {/if}
  <Legend />
</div>

<style>
  .wrap {
    width: 100vw;
    height: 100vh;
    display: flex;
    flex-direction: column;
    font-family: system-ui, sans-serif;
  }
  .body {
    flex: 1;
    min-height: 0;
    display: flex;
  }
</style>
