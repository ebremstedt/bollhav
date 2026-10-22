<script>
  import Slide from '../lib/Slide.svelte'
  import Code from '../lib/Code.svelte'
</script>

<Slide kicker="Memory" title="Generators are encouraged">
  <div class="top">
    <div>
      <p class="lead">
        While bollhav does not force you to write data in a specific way, there is built in support to do so using
        generators of dataframes.
      </p>
      <p>
        Preferably, the read, transform and write steps each hand over a generator, so only one batch is in memory at
        a time. Peak memory for the pipeline stays very, very low.
      </p>
    </div>
    <!-- memory over time, the two ways -->
    <div class="graphs">
      <figure class="graph">
        <svg viewBox="0 0 200 52" aria-hidden="true">
          <path class="area" d="M12 46 L26 20 L33 26 L44 9 L55 45 L70 18 L79 24 L92 7 L103 45 L121 24 L128 16 L146 8 L155 45 L173 20 L180 25 L196 11 L196 46 L12 46 Z" />
          <path class="line" d="M12 46 L26 20 L33 26 L44 9 L55 45 L70 18 L79 24 L92 7 L103 45 L121 24 L128 16 L146 8 L155 45 L173 20 L180 25 L196 11" />
          <g class="axis">
            <path d="M12 46 H196 M12 46 V4" />
            <path d="M193 43 L196 46 L193 49 M9 7 L12 4 L15 7" />
          </g>
          <text class="label" x="190" y="43" text-anchor="end">time</text>
          <text class="label" x="15" y="8">memory</text>
        </svg>
        <figcaption>without generators</figcaption>
      </figure>
      <figure class="graph">
        <svg viewBox="0 0 200 52" aria-hidden="true">
          <path class="area" d="M12 46 L16 36 L23 37 L30 36 L37 38 L44 34 L51 34 L58 34 L65 37 L72 34 L79 36 L86 34 L93 34 L100 38 L107 38 L114 34 L121 36 L128 34 L135 38 L142 34 L149 34 L156 36 L163 34 L170 38 L177 34 L184 36 L191 34 L196 36 L196 46 L12 46 Z" />
          <path class="line" d="M12 46 L16 36 L23 37 L30 36 L37 38 L44 34 L51 34 L58 34 L65 37 L72 34 L79 36 L86 34 L93 34 L100 38 L107 38 L114 34 L121 36 L128 34 L135 38 L142 34 L149 34 L156 36 L163 34 L170 38 L177 34 L184 36 L191 34 L196 36" />
          <g class="axis">
            <path d="M12 46 H196 M12 46 V4" />
            <path d="M193 43 L196 46 L193 49 M9 7 L12 4 L15 7" />
          </g>
          <text class="label" x="190" y="43" text-anchor="end">time</text>
          <text class="label" x="15" y="8">memory</text>
        </svg>
        <figcaption>with generators</figcaption>
      </figure>
    </div>
  </div>
  <Code
    file="execute.py"
    code={`
      @execute_lifecycle
      def execute(run: ModelRun, interval, data_conn, state_conn=None):
          df_gen = read(run, interval)      # a generator of DataFrames
          df_gen = transform(df_gen)        # still lazy
          write(conn=data_conn, run=run, df_gen=df_gen)   # one batch at a time
    `}
  />
</Slide>

<style>
  .top {
    display: grid;
    grid-template-columns: minmax(0, 1.3fr) minmax(0, 1fr);
    gap: 2rem;
    align-items: center;
  }

  .graphs {
    display: grid;
    gap: 0.9rem;
    max-width: 19rem;
    justify-self: end;
    width: 100%;
  }

  .graph {
    margin: 0;
  }

  .graph svg {
    display: block;
    width: 100%;
    height: auto;
  }

  .graph .line {
    fill: none;
    stroke: var(--accent);
    stroke-width: 2;
    stroke-linejoin: round;
    stroke-linecap: round;
  }

  .graph .area {
    fill: var(--accent);
    opacity: 0.16;
  }

  .graph .axis {
    fill: none;
    stroke: var(--fg-2);
    stroke-width: 1;
    stroke-linecap: round;
    stroke-linejoin: round;
    opacity: 0.6;
  }

  .graph .label {
    font-family: var(--font-mono);
    font-size: 5.5px;
    letter-spacing: 0.06em;
    fill: var(--fg-2);
  }

  .graph figcaption {
    margin-top: 0.3rem;
    font-family: var(--font-mono);
    font-size: 0.66rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--fg-2);
    text-align: center;
  }

  @media (max-width: 900px) {
    .top {
      grid-template-columns: 1fr;
    }
  }
</style>
