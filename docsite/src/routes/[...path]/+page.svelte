<script lang="ts">
  import { mermaidDiagrams } from '$lib/mermaid'
  import Sea from '$lib/Sea.svelte'
  import { sea } from '$lib/ui.svelte'

  let { data } = $props()
  const github = 'https://github.com/ebremstedt/bollhav/blob/main'
</script>

<svelte:head>
  <title>{data.path === 'README.md' ? 'bollhav' : `${data.title} · bollhav`}</title>
</svelte:head>

{#if data.path === 'README.md'}
  <Sea density={sea.density} storm={sea.storm} />
{/if}

<article class="prose" class:home={data.path === 'README.md'} data-pagefind-body use:mermaidDiagrams={data.path}>
  {@html data.html}
</article>
<p class="eyebrow source">
  <a href="{github}/{data.source}">{data.source} on GitHub</a>
</p>
