<script>
  import Slide from '../lib/Slide.svelte'
  import Code from '../lib/Code.svelte'
</script>

<Slide kicker="Memory" title="Generators are encouraged">
  <p class="lead">
    While bollhav does not force you to write data in a specific way, there is built in support to do so using
    generators of dataframes.
  </p>
  <p>
    Preferably, the read, transform and write steps each hand over a generator, so only one batch is in memory at a
    time. Peak memory for the pipeline stays very, very low.
  </p>
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
