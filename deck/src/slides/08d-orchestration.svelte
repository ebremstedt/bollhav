<script>
  import Slide from '../lib/Slide.svelte'
</script>

<Slide kicker="Running the models" title="Orchestration vs Choreography">
  <div class="cols">
    <div class="half">
      <img class="plain pic" src="./conductor.png" alt="" width="512" height="512" />
      <div>
        <h3>Orchestration</h3>
        <p class="muted how">the traditional way</p>
        <p>Bollhav is built to accept instructions from an orchestrator like Airflow or Dagster.</p>
      </div>
    </div>
    <div class="half">
      <div>
        <h3>Choreography</h3>
        <p class="muted how">a new approach</p>
        <p>
          Bollhav models can also be choreographed by <strong>bollhav state</strong>. Instead of being told what
          to do, bollhav models ask state what is missing, and run those things on an interval of your own choosing.
        </p>
      </div>
      <img class="plain pic" src="./dancer.png" alt="" width="512" height="512" />
    </div>
  </div>
</Slide>

<style>
  /* a conductor and the orchestra beside the one, dancers beside the other */
  .half {
    display: flex;
    align-items: flex-start;
    gap: 1.25rem;
    min-width: 0;
  }

  /* the pictures are black line art on transparent: on the dark theme they
     are inverted, so the lines come out light */
  .pic {
    flex: none;
    width: clamp(6.5rem, 13vw, 11rem);
    height: auto;
    margin-top: 0.3rem;
  }

  :global([data-theme='dark']) .pic {
    filter: invert(1);
  }

  @media (prefers-color-scheme: dark) {
    :global(:root:not([data-theme='light'])) .pic {
      filter: invert(1);
    }
  }

  .how {
    font-family: var(--font-mono);
    font-size: 0.85rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-bottom: 0.6rem;
  }

  @media (max-width: 760px) {
    .half {
      flex-direction: column;
    }
    .half:last-child {
      flex-direction: column-reverse;
    }
  }
</style>
