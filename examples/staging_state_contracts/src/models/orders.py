"""orders — an INTERVAL table, with staging + state.

The classic shape: a batched table whose unit of work is a time window.
`@daily` over 2024-01-01..2024-01-04 → three intervals. Each interval's
rows COPY into a per-interval staging table; on a clean run the lifecycle
merges staging → target and flips that interval's state row to `applied`.

This is the interval upstream `daily_summary` gates with `UpstreamContract.WINDOW`.
"""

from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    Contract,
    Database,
    TimeChunking,
    Kind,
    Model,
    Staging,
    State,
    Tags,
    Target,
    WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType


orders = Model(
    target=Target(
        name="orders",
        schema="warehouse",
        catalog="demo",
        database=Database.POSTGRES,
        write_mode=WriteMode.APPEND,
        dsn_env_var="TARGET_DSN",
        staging=Staging(),  # defaults: central z_bollhav schema, UNLOGGED, APPEND
        columns=[
            PostgresColumn(name="id", data_type=PostgresType.BIGINT, nullable=False),
            PostgresColumn(
                name="customer_id", data_type=PostgresType.BIGINT, nullable=False
            ),
            PostgresColumn(
                name="total", data_type=PostgresType.NUMERIC, nullable=False
            ),
            PostgresColumn(
                name="order_date",
                data_type=PostgresType.TIMESTAMPTZ,
                nullable=False,
            ),
        ],
    ),
    kind=Kind.TEMPORAL,
    state=State(),  # required for staging; tracks one row per interval
    batching=Batch(time=TimeChunking(chunk="@daily")),
    contract=Contract(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    tagging=Tags(tags={"demo"}),
)
