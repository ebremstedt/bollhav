<script>
  import Slide from '../../lib/Slide.svelte'
</script>

<Slide title="A table">
  <p class="lead">Placeholder. A plain <code>&lt;table&gt;</code>, wrapped in <code>div.table</code> so it can scroll sideways on a phone.</p>
  <div class="table">
    <table>
      <thead>
        <tr><th>Status</th><th>Meaning</th></tr>
      </thead>
      <tbody>
        <tr><td><code>pending</code></td><td>Queued to run.</td></tr>
        <tr><td><code>running</code></td><td>Being processed right now.</td></tr>
        <tr><td><code>applied</code></td><td>Completed successfully.</td></tr>
        <tr><td><code>blocked</code></td><td>Waiting on an upstream.</td></tr>
        <tr><td><code>error</code></td><td>Execute raised; details in the errors table.</td></tr>
      </tbody>
    </table>
  </div>
</Slide>
