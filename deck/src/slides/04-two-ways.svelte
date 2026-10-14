<script>
  import Slide from '../lib/Slide.svelte'
  import Schema from '../lib/Schema.svelte'
</script>

<Slide kicker="Virtual environments" title="Two ways to version models" layout="wide">
  <div class="cols">
    <div>
      <h3>Version the schema</h3>
      <p>
        Create the schema <code>orders_v2</code> with the changed models <code>a</code>, <code>b</code>, <code>c</code>.
        Some users append a date to the model, to indicate when support for the old version ends.
      </p>
      <div class="row">
        <Schema name="orders" tables={['a', 'b', 'c']} old />
        <span class="arrow" aria-hidden="true">→</span>
        <Schema name="orders_v2" tables={['a', 'b', 'c']} highlight={['a', 'b', 'c']} />
      </div>
    </div>
    <div>
      <h3>Version the tables</h3>
      <p>
        Keep the schema <code>orders</code> as is and create the new tables inside it (with a chosen suffix), next to
        the old ones:
        <code>a_v2</code>, <code>b_v2</code>, <code>c_v2</code>.
      </p>
      <div class="row">
        <Schema name="orders" tables={['a', 'b', 'c', 'a_v2', 'b_v2', 'c_v2']} highlight={['a_v2', 'b_v2', 'c_v2']} />
      </div>
    </div>
  </div>
</Slide>

<style>
  .row {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.8rem;
  }

  .arrow {
    font-size: 1.6rem;
    color: var(--fg-2);
  }
</style>
