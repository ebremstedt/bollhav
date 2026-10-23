<script>
  import Slide from '../lib/Slide.svelte'
</script>

<Slide kicker="Writing data" title="Write modes">
  {#snippet heading()}
    Write
    <span class="noted"
      >modes<span class="note">
        <svg class="arrow" viewBox="0 0 60 50" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
          <path d="M56 4 C 42 6, 24 20, 6 44" />
          <path d="M7 31 L6 44 L19 42" />
        </svg>
        <span class="text">you are not forced to use them,<br />write your own functions if you want!</span>
      </span></span
    >
  {/snippet}
  <p class="lead">How incoming rows land in the target table.</p>
  <div class="table">
    <table>
      <thead>
        <tr><th>Mode</th><th>Inserts</th><th>Updates</th><th>Deletes</th><th>Use when</th></tr>
      </thead>
      <tbody>
        <tr><td><code>APPEND</code></td><td>✓</td><td></td><td></td><td>duplicates are fine, or a raw landing table</td></tr>
        <tr><td><code>UPSERT_NO_DELETE</code></td><td>✓</td><td>✓</td><td></td><td>keyed rows, source deletes never propagate</td></tr>
        <tr><td><code>RECREATE_PARTITION</code></td><td>✓</td><td>✓</td><td>✓</td><td>replace a time window outright, idempotent</td></tr>
        <tr><td><code>MERGE</code></td><td>✓</td><td>✓</td><td>✓</td><td>full sync, Postgres 15+</td></tr>
      </tbody>
    </table>
  </div>
  <p class="muted">A view is not a write mode, but <code>materialization=Materialization.VIEW</code></p>
</Slide>

<style>
  /* a note above "modes" in the heading, the arrow rising from the word up
     to the right, the words at its end; same as the two-ways slide */
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
</style>
