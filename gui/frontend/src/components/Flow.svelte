<script>
  import {
    SvelteFlow,
    Background,
    Controls,
    MiniMap,
  } from "@xyflow/svelte";
  import LineageNode from "./LineageNode.svelte";
  import ContractEdge from "./ContractEdge.svelte";
  import Recenter from "./Recenter.svelte";
  import { view } from "../lib/view.svelte.js";

  let { dark } = $props();

  const nodeTypes = { model: LineageNode, external: LineageNode };
  const edgeTypes = { contract: ContractEdge };
</script>

<div class="flow">
  <!-- remount on focus change so fitView re-frames the narrowed graph -->
  {#key view.focused ?? "all"}
    <SvelteFlow
      bind:nodes={view.nodes}
      bind:edges={view.edges}
      {nodeTypes}
      {edgeTypes}
      colorMode={dark ? "dark" : "light"}
      fitView
      fitViewOptions={{ maxZoom: 1, padding: 0.2 }}
    >
      <Recenter />
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
