<script>
  import {
    SvelteFlow,
    Background,
    Controls,
    MiniMap,
  } from "@xyflow/svelte";
  import LineageNode from "./LineageNode.svelte";
  import { view } from "../lib/view.svelte.js";

  let { dark } = $props();

  const nodeTypes = { model: LineageNode, external: LineageNode };
</script>

<div class="flow">
  <!-- remount on focus change so fitView re-frames the narrowed graph -->
  {#key view.focused ?? "all"}
    <SvelteFlow
      bind:nodes={view.nodes}
      bind:edges={view.edges}
      {nodeTypes}
      colorMode={dark ? "dark" : "light"}
      fitView
    >
      <Background />
      <Controls />
      <MiniMap />
    </SvelteFlow>
  {/key}
</div>

<style>
  .flow {
    flex: 1;
    min-height: 0;
  }
</style>
