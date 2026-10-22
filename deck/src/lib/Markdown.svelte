<script>
  // A slide's text from a Markdown file: import it with `?raw`, hand it over,
  // and edit the .md while the dev server runs; the slide follows the save.
  import { marked } from 'marked'

  let { source = '' } = $props()
  const html = $derived(marked.parse(source))
</script>

{@html html}
