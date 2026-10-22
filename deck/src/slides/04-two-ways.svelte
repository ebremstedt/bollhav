<script>
  import Slide from '../lib/Slide.svelte'
  import Schema from '../lib/Schema.svelte'
</script>

<Slide kicker="Virtual environments" title="Two ways to version models" layout="wide">
  {#snippet heading()}
    Two ways to version
    <span class="noted"
      >models<span class="note">
        <svg class="arrow" viewBox="0 0 60 50" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
          <path d="M56 4 C 42 6, 24 20, 6 44" />
          <path d="M7 31 L6 44 L19 42" />
        </svg>
        <span class="text">an umbrella term:<br />a table or a view</span>
      </span></span
    >
  {/snippet}
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
  /* a note above "models" in the heading, the arrow rising from the word up
     to the right, the words at its end */
  .noted {
    position: relative;
    white-space: nowrap;
  }

  .note {
    position: absolute;
    left: 55%;
    bottom: 100%;
    display: flex;
    align-items: flex-end;
    gap: 0.3rem;
    width: max-content;
    text-align: left;
    white-space: normal;
    font-family: var(--font-mono);
    font-size: 0.78rem;
    font-weight: 400;
    line-height: 1.35;
    letter-spacing: 0.02em;
    color: var(--fg-2);
  }

  .note .arrow {
    flex: none;
    width: 2.4rem;
    height: auto;
    color: var(--accent);
  }

  .note .text {
    margin-bottom: 1.1rem;
    transform: rotate(-3deg);
  }

  @media (max-width: 900px) {
    .note {
      display: none;
    }
  }

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
