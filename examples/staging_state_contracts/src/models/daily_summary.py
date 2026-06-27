"""daily_summary — an INTERVAL table (staging + state) that declares gated
**upstream** Sources, each with an `UpstreamContract` level.

This is the point of the example. Its `upstream` is a list of `Source`s, each
carrying a contract level. The level says *how much* must be ready; the
upstream's **shape** (read from the registry) decides what that means, so the
same `ENCAPSULATE` level resolves differently per upstream:

  * warehouse.orders (an interval model) + ENCAPSULATE — satisfied when orders has
                                            an applied state row covering this
                                            summary interval's window.
  * warehouse.customers (a view) + ENCAPSULATE — satisfied when the customers view
                                            exists (its existence row applied).
  * warehouse.app_config (monolithic) + ENCAPSULATE — satisfied when app_config is
                                            loaded (its existence row applied).

At each interval `@execute_lifecycle` checks every gated upstream. If any is
unsatisfied the interval is marked `blocked` (with a reason naming each
missing upstream) and skipped; when all are satisfied, it runs. A gated
upstream whose model was never registered is a hard error, not a skip.
"""

from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    Contract,
    Database,
    TimeChunking,
    Temporality,
    Model,
    Source,
    SourceModel,
    Staging,
    State,
    Tags,
    Target,
    UpstreamContract,
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
    temporality=Temporality.TEMPORAL,
    state=State(),
    batching=Batch(time=TimeChunking(chunk="@daily")),
    contract=Contract(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    # Upstreams are referenced by full identity (catalog.schema.table).
    upstream=[
        Source(
            "demo.warehouse.orders",
            type=SourceModel(),
            contract=UpstreamContract.ENCAPSULATE,
        ),
        Source(
            "demo.warehouse.customers",
            type=SourceModel(),
            contract=UpstreamContract.ENCAPSULATE,
        ),
        Source(
            "demo.warehouse.app_config",
            type=SourceModel(),
            contract=UpstreamContract.ENCAPSULATE,
        ),
    ],
    tagging=Tags(tags={"demo"}),
)
