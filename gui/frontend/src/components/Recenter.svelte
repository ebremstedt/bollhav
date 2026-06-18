<script>
  import { useSvelteFlow } from "@xyflow/svelte";
  import { view } from "../lib/view.svelte.js";

  // Renders nothing — it just bridges the sub-bar's "recenter" button to the
  // flow. Must live inside <SvelteFlow> so useSvelteFlow() has the context.
  const { fitView } = useSvelteFlow();

  let seen = view.recenterAt;
  $effect(() => {
    // skip the initial mount; only react to real button presses (the flow
    // already auto-fits via the fitView prop on load / focus change).
    if (view.recenterAt !== seen) {
      seen = view.recenterAt;
      fitView({ maxZoom: 1, padding: 0.2, duration: 300 });
    }
  });
</script>
