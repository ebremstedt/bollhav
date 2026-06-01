"""Events model — MSSQL, interval windows + read-time row batching.

This is the example for the two-knob chunking model:

  * `batching.interval` = `@daily` → one `(since, until)` window per day.
    The runner loops these and hands each pair to `execute`.
  * `batching.size` = 5000 → the *read* function slices each day's rows
    into 5000-row frames before writing. See `read.py`.

Bounds span three days (2024-01-01 .. 2024-01-04), so `model.intervals`
yields three windows. Each day produces 12_000 rows, so each interval
streams as 5000 + 5000 + 2000 — three write chunks per day.

Target write mode is APPEND with no staging and no `State()` — MSSQL
state coordination isn't implemented yet, so this run is not gated on
applied rows. `main.py` drops the target first to keep reruns clean.
"""

from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    Bounds,
    Database,
    IntervalChunks,
    Model,
    Tags,
    Target,
    TargetSchema,
    WriteMode,
)
from bollhav.mssql import MssqlColumn, MssqlType


events = Model(
    target=Target(
        name="events",
        schema=TargetSchema(name="warehouse"),
        database=Database.MSSQL,
        write_mode=WriteMode.APPEND,
        dsn_env_var="BOLLHAV_MSSQL_DSN",
        columns=[
            MssqlColumn(
                name="event_id",
                data_type=MssqlType.BIGINT,
                nullable=False,
                primary_key=True,
            ),
            MssqlColumn(
                name="event_time",
                data_type=MssqlType.DATETIME2,
                nullable=False,
            ),
            MssqlColumn(
                name="payload",
                data_type=MssqlType.NVARCHAR,
                length=100,
                nullable=False,
            ),
        ],
    ),
    # Two independent knobs: interval windows + rows per read chunk.
    batching=Batch(
        interval=IntervalChunks(expression="@daily"),
        size=5000,
    ),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    tagging=Tags(tags={"events"}),
)
