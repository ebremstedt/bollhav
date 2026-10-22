<script lang="ts">
  import { mermaidDiagrams } from '$lib/mermaid'

  let { data } = $props()
  const github = 'https://github.com/ebremstedt/bollhav/blob/main'
</script>

<svelte:head>
  <title>{data.path === 'README.md' ? 'bollhav' : `${data.title} · bollhav`}</title>
</svelte:head>

<article class="prose" data-pagefind-body use:mermaidDiagrams={data.path}>
  {@html data.html}
</article>
<p class="eyebrow source">
  <a href="{github}/{data.source}">{data.source} on GitHub</a>
</p>
