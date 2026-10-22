<script lang="ts">
  import { base } from '$app/paths'
  import { page } from '$app/state'
</script>

<svelte:head>
  <title>{page.status} · bollhav</title>
</svelte:head>

<article class="prose">
  <p class="eyebrow">{page.status}</p>
  <h1>{page.error?.message ?? 'Something went wrong'}</h1>
  <p>A page is a Markdown file in the repo, at the same path. <a href="{base}/">Back to the start.</a></p>
</article>
