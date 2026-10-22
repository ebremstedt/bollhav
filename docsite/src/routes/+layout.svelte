<script lang="ts">
  import '@fontsource-variable/familjen-grotesk'
  import '@fontsource/jetbrains-mono/400.css'
  import '@fontsource/jetbrains-mono/600.css'
  import '../app.css'
  import type { Snippet } from 'svelte'
  import Nav from '$lib/components/Nav.svelte'
  import Sidebar from '$lib/components/Sidebar.svelte'
  import Pager from '$lib/components/Pager.svelte'

  let { children }: { children: Snippet } = $props()
</script>

<Nav />
<div class="shell layout">
  <Sidebar />
  <main>
    {@render children()}
    <Pager />
  </main>
</div>
