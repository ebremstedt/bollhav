<script>
  // A code block for a slide.
  //
  //   <Code lang="python" file="models.py" code={`
  //     app_config = Model(...)
  //   `} />
  //
  // The template literal's indentation is stripped, so it can sit indented in
  // the slide's markup. `lang` is python (default), bash, sql or plain.
  import { dedent, highlight } from './highlight.js'

  let { code = '', lang = 'python', file = '' } = $props()
  const html = $derived(highlight(dedent(code), lang))
</script>

<figure class="code">
  {#if file}<figcaption>{file}</figcaption>{/if}
  <pre><code>{@html html}</code></pre>
</figure>
