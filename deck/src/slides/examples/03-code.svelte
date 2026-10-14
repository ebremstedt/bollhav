<script>
  import Slide from '../../lib/Slide.svelte'
  import Code from '../../lib/Code.svelte'
</script>

<Slide title="A slide with code">
  <p class="lead">Placeholder. A model is a pure data object.</p>
  <Code
    file="models.py"
    code={`
      from bollhav.model import Database, Model, Target, Temporality, WriteMode
      from bollhav.postgres import PostgresColumn, PostgresType


      app_config = Model(
          temporality=Temporality.TIMELESS,
          target=Target(
              name="app_config",
              schema="warehouse",
              database=Database.POSTGRES,
              write_mode=WriteMode.APPEND,
              columns=[
                  PostgresColumn("key", PostgresType.TEXT, nullable=False),
                  PostgresColumn("value", PostgresType.TEXT, nullable=False),
              ],
          ),
      )  # no execution logic in here
    `}
  />
</Slide>
