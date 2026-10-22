<script lang="ts">
  import { mermaidDiagrams } from '$lib/mermaid'
  import Sea from '$lib/Sea.svelte'
  import SeaControls from '$lib/components/SeaControls.svelte'

  let { data } = $props()
  // the home page's sea, calmer than the deck's to begin with
  let density = $state(0.6)
  let storm = $state(0.5)
  const github = 'https://github.com/ebremstedt/bollhav/blob/main'
</script>

<svelte:head>
  <title>{data.path === 'README.md' ? 'bollhav' : `${data.title} · bollhav`}</title>
</svelte:head>

{#if data.path === 'README.md'}
  <Sea {density} {storm} />
  <SeaControls bind:density bind:storm />
{/if}

<article class="prose" class:home={data.path === 'README.md'} data-pagefind-body use:mermaidDiagrams={data.path}>
  {@html data.html}
</article>
<p class="eyebrow source">
  <a href="{github}/{data.source}">{data.source} on GitHub</a>
</p>
