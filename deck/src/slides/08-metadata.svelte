<script>
  import Slide from '../lib/Slide.svelte'
  import Code from '../lib/Code.svelte'

  // the model from docs/examples/every_feature.md, trimmed to fit a box
  const model = `
    daily_orders = Model(
        description="One row per customer and day: how many orders, and what they came to.",
        target=Target(
            name="daily_orders",
            schema="marts",
            catalog="shop",
            database=Database.POSTGRES,
            write_mode=WriteMode.UPSERT_NO_DELETE,
            staging=PostgresStaging(),
            columns=[
                PostgresColumn("day", data_type=PostgresType.DATE, nullable=False, primary_key=True, description="Stockholm time"),
                PostgresColumn("customer_id", data_type=PostgresType.BIGINT, nullable=False, primary_key=True),
                PostgresColumn("order_count", data_type=PostgresType.INTEGER, nullable=False),
                PostgresColumn("total", data_type=PostgresType.NUMERIC, nullable=False, precision=12, scale=2),
            ],
        ),
        temporality=Temporality.TEMPORAL,
        contract=Contract(begin=datetime(2024, 1, 1, tzinfo=STOCKHOLM)),
        batching=Batch(
            time=TimeChunking(
                chunk="@daily",
                tz=STOCKHOLM,
                lookback=2,
            ),
            size=50_000,
            retries=2,
        ),
        state=State(mode=StateMode.DISCOVER),
        upstream=[
            Source("shop.raw.orders", type=SourceModel(), contract=UpstreamContract.ENCAPSULATE),
            Source("shop.raw.customers", type=SourceModel(), contract=UpstreamContract.WHOLE),
        ],
        curfew=Curfew(windows=[(time(2), time(4))], tz=STOCKHOLM),
        tagging=Tags(tags={"finance", "daily"}),
        people=People(
            owners=(Contact("Maja Lindqvist", "maja.lindqvist@example.com"),),
            maintainers=(Contact("Jonas Ek", "jonas.ek@example.com"), Contact("Priya Nair", "priya.nair@example.com")),
            creators=(Contact("Amira Haddad"),),
        ),
        enabled=True,
    )
  `
</script>

<Slide kicker="Metadata" title="Excellent metadata" layout="wide">
  <p class="lead">The model object is built to produce great metadata, for execution, lineage, state, orchestration.</p>
  <!-- the whole model, small, in its own scrolling box under the text -->
  <div class="box">
    <Code file="models.py" code={model} />
  </div>
</Slide>

<style>
  .box {
    min-width: 0;
  }

  /* the box is the one thing that scrolls, down and sideways; the caption
     stays put in its corner */
  .box :global(.code) {
    font-size: 0.85rem;
    line-height: 1.5;
    max-height: min(44vh, 24rem);
    overflow: auto;
  }

  .box :global(.code pre) {
    overflow: visible;
    width: max-content;
    min-width: 100%;
  }

  .box :global(.code figcaption) {
    position: sticky;
    top: 0;
    left: 0;
    background: var(--code-bg);
  }
</style>
