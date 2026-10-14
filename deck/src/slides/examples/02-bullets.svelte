<script>
  import Slide from '../../lib/Slide.svelte'
</script>

<Slide title="A slide with bullets">
  <p class="lead">Placeholder. A heading, a lead line, and a short list.</p>
  <ul>
    <li>Each slide is one Svelte file in <code>src/slides/</code>.</li>
    <li>Wrap it in <code>&lt;Slide title="…"&gt;</code> and write plain HTML inside.</li>
    <li>Headings, lists, code, tables and images are styled for you.</li>
    <li>Order lives in <code>src/slides/index.js</code>.</li>
  </ul>
</Slide>
