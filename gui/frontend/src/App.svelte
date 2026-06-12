<script>
  import { onMount } from "svelte";
  import { selection } from "./lib/selection.svelte.js";
  import { loadGraph } from "./lib/view.svelte.js";
  import Header from "./components/Header.svelte";
  import Flow from "./components/Flow.svelte";
  import DetailPanel from "./components/DetailPanel.svelte";
  import Legend from "./components/Legend.svelte";

  let dark = $state(true);

  onMount(loadGraph);
</script>

<div class="wrap" class:dark>
  <Header bind:dark />
  <div class="body">
    <Flow {dark} />
    {#if selection.name}
      <DetailPanel />
    {/if}
  </div>
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
