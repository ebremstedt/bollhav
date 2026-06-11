"""daily_summary — an INTERVAL table (staging + state) that declares gated
**upstream** Sources of all three contract kinds.

This is the point of the example. Its `upstream` is a list of `Source`s, each
carrying a contract, checked by the upstream's shape before an interval runs:

  * SourceModel + IntervalContract on warehouse.orders — satisfied when orders
                                            has an applied state row covering
                                            this summary interval's window.
  * SourceModel + ViewContract on warehouse.customers — satisfied when the
                                            customers view exists (row applied).
  * SourceModel + MonolithicContract on warehouse.app_config — satisfied when
                                            app_config is loaded (row applied).

At each interval `@execute_lifecycle` checks every gated upstream. If any is
unsatisfied the interval is marked `blocked` (with a reason naming each
missing upstream) and skipped; when all are satisfied, it runs. A gated
upstream whose model was never registered is a hard error, not a skip.
"""

from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    Bounds,
    Database,
    TimeChunking,
    IntervalContract,
    Kind,
    Model,
    MonolithicContract,
    Source,
    SourceModel,
    Staging,
    State,
    Tags,
    Target,
    ViewContract,
    WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType


daily_summary = Model(
    target=Target(
        name="daily_summary",
        schema="warehouse",
        catalog="demo",
        database=Database.POSTGRES,
        write_mode=WriteMode.APPEND,
        dsn_env_var="TARGET_DSN",
        staging=Staging(),
        columns=[
            PostgresColumn(
                name="day", data_type=PostgresType.TIMESTAMPTZ, nullable=False
            ),
            PostgresColumn(
                name="order_count", data_type=PostgresType.BIGINT, nullable=False
            ),
            PostgresColumn(
                name="total", data_type=PostgresType.NUMERIC, nullable=False
            ),
        ],
    ),
    kind=Kind.INTERVAL,
    state=State(),
    batching=Batch(time=TimeChunking(chunk="@daily")),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    # Upstreams are referenced by full identity (catalog.schema.table).
    upstream=[
        Source(
            "demo.warehouse.orders", type=SourceModel(), contract=IntervalContract()
        ),
        Source("demo.warehouse.customers", type=SourceModel(), contract=ViewContract()),
        Source(
            "demo.warehouse.app_config",
            type=SourceModel(),
            contract=MonolithicContract(),
        ),
    ],
    tagging=Tags(tags={"demo"}),
)
