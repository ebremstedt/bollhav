<script>
  import Slide from '../lib/Slide.svelte'
</script>

<Slide kicker="Running the models" title="Orchestration vs Choreography">
  <div class="cols">
    <div class="half">
      <svg class="pic" viewBox="0 0 120 100" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
    <g fill="currentColor" stroke="none" opacity="0.75">
      <circle cx="11.1" cy="80.2" r="2.6" />
      <circle cx="16.9" cy="68.9" r="2.6" />
      <circle cx="25.2" cy="59.4" r="2.6" />
      <circle cx="35.6" cy="52.1" r="2.6" />
      <circle cx="47.4" cy="47.5" r="2.6" />
      <circle cx="60.0" cy="46.0" r="2.6" />
      <circle cx="72.6" cy="47.5" r="2.6" />
      <circle cx="84.4" cy="52.1" r="2.6" />
      <circle cx="94.8" cy="59.4" r="2.6" />
      <circle cx="103.1" cy="68.9" r="2.6" />
      <circle cx="108.9" cy="80.2" r="2.6" />
      <circle cx="24.3" cy="85.0" r="2.6" />
      <circle cx="30.9" cy="73.6" r="2.6" />
      <circle cx="41.0" cy="65.1" r="2.6" />
      <circle cx="53.4" cy="60.6" r="2.6" />
      <circle cx="66.6" cy="60.6" r="2.6" />
      <circle cx="79.0" cy="65.1" r="2.6" />
      <circle cx="89.1" cy="73.6" r="2.6" />
      <circle cx="95.7" cy="85.0" r="2.6" />
      <circle cx="37.4" cy="89.8" r="2.6" />
      <circle cx="46.2" cy="78.3" r="2.6" />
      <circle cx="60.0" cy="74.0" r="2.6" />
      <circle cx="73.8" cy="78.3" r="2.6" />
      <circle cx="82.6" cy="89.8" r="2.6" />
    </g>
    <g class="lead">
      <circle cx="60" cy="72" r="5.5" />
      <path d="M60 78 V92" />
      <path d="M60 83 L47 70" />
      <path d="M60 83 L74 73 L84 61" />
      <path d="M84 61 L90 54" stroke-width="1.5" />
      <path d="M60 92 L53 100 M60 92 L67 100" />
    </g>
  </svg>
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
          But bollhav models can also be choreographed by <strong>bollhav state</strong>. Instead of being told what
          to do, bollhav models ask state what is missing, and run those things on an interval of your own choosing.
        </p>
      </div>
      <svg class="pic" viewBox="0 0 120 100" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
    <g>
      <circle cx="24" cy="30" r="5" />
      <path d="M24 35 L27 58" />
      <path d="M25 42 L10 30 M25 42 L41 28" />
      <path d="M27 58 L13 78 M27 58 L44 72" />
    </g>
    <g class="lead">
      <circle cx="62" cy="20" r="5" />
      <path d="M62 25 L60 50" />
      <path d="M61 34 L44 42 M61 34 L80 22" />
      <path d="M60 50 L46 74 M60 50 L86 58" />
    </g>
    <g>
      <circle cx="98" cy="36" r="5" />
      <path d="M98 41 L96 63" />
      <path d="M97 48 L82 58 M97 48 L114 42" />
      <path d="M96 63 L86 84 M96 63 L109 81" />
    </g>
  </svg>
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

  .pic {
    flex: none;
    width: clamp(6.5rem, 13vw, 11rem);
    height: auto;
    margin-top: 0.3rem;
    color: var(--fg-2);
  }

  .pic .lead {
    color: var(--accent);
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
