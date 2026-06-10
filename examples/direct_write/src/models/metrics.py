"""metrics — an INTERVAL table written DIRECTLY with RECREATE_PARTITION.

Each interval OVERWRITES its window in place: `DELETE` everything where the
partition column is in `[since, until)`, then COPY the fresh rows — no
staging. That makes the write idempotent by construction (rerunning a window
replaces it), which is exactly when the direct path is a good fit.

The partition column is marked `partition_on=True`; `run_interval` passes the
interval's `since`/`until` to `write()` for the DELETE bounds.
"""

from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    Bounds,
    Database,
    IntervalChunks,
    Kind,
    Model,
    State,
    Tags,
    Target,
    WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType


metrics = Model(
    target=Target(
        name="metrics",
        schema="warehouse",
        catalog="demo",
        database=Database.POSTGRES,
        write_mode=WriteMode.RECREATE_PARTITION,
        dsn_env_var="TARGET_DSN",
        # no `staging=` → direct DELETE-then-COPY of the interval's window
        columns=[
            PostgresColumn(
                name="metric_time",
                data_type=PostgresType.TIMESTAMPTZ,
                nullable=False,
                partition_on=True,  # the DELETE window column
            ),
            PostgresColumn(name="name", data_type=PostgresType.TEXT, nullable=False),
            PostgresColumn(
                name="value", data_type=PostgresType.NUMERIC, nullable=False
            ),
        ],
    ),
    kind=Kind.INTERVAL,
    state=State(),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    tagging=Tags(tags={"demo"}),
)
