"""orders — an INTERVAL table, with staging + state.

The classic shape: a batched table whose unit of work is a time window.
`@daily` over 2024-01-01..2024-01-04 → three intervals. Each interval's
rows COPY into a per-interval staging table; on a clean run the lifecycle
merges staging → target and flips that interval's state row to `applied`.

This is the IntervalContract upstream for `daily_summary`.
"""

from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    Bounds,
    Database,
    IntervalChunks,
    Kind,
    Model,
    Staging,
    State,
    Tags,
    Target,
    TargetSchema,
    WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType


orders = Model(
    target=Target(
        name="orders",
        schema=TargetSchema(name="warehouse"),
        database=Database.POSTGRES,
        write_mode=WriteMode.APPEND,
        dsn_env_var="TARGET_DSN",
        staging=Staging(),  # defaults: z_warehouse schema, UNLOGGED, APPEND
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
    kind=Kind.INTERVAL,
    state=State(),  # required for staging; tracks one row per interval
    batching=Batch(interval=IntervalChunks(expression="@daily")),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    tagging=Tags(tags={"demo"}),
)
