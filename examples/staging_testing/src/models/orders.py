"""Orders model — exercises staging.

Three @daily intervals over 2024-01-01..2024-01-04. Each interval
generates ~5000 mock rows that the pipeline writes in 2000-row chunks.
With `Target(staging=Staging())`, every chunk COPYs into a per-interval
staging table; on interval completion, one transaction moves staging
→ target and flips the state row to applied.
"""

from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    Bounds,
    Database,
    IntervalChunks,
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
        staging=Staging(
            # All defaults: staging schema = z_warehouse,
            #               staging tables = orders_staging_<run_id_short>,
            #               UNLOGGED, dropped on flush.
        ),
        columns=[
            PostgresColumn(name="id", data_type=PostgresType.BIGINT, nullable=False),
            PostgresColumn(
                name="customer_id",
                data_type=PostgresType.BIGINT,
                nullable=False,
            ),
            PostgresColumn(
                name="total",
                data_type=PostgresType.NUMERIC,
                nullable=False,
            ),
            PostgresColumn(
                name="order_date",
                data_type=PostgresType.TIMESTAMPTZ,
                nullable=False,
            ),
        ],
    ),
    state=State(),  # required when staging is set
    batching=Batch(interval=IntervalChunks(expression="@daily")),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    tagging=Tags(tags={"orders"}),
)
