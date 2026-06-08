"""Sales model — MSSQL, interval windows + **staging**.

The point of this example: stage an MSSQL table through the same
lifecycle hooks as Postgres. Two knobs are at play:

  * `batching.interval` = `@daily` → one `(since, until)` window per day.
  * `target.staging` = `MssqlStaging()` → each window's chunks bulk-insert
    into a per-interval staging table; one transaction then applies that
    staging table to the target and drops it. A crash mid-stream leaves
    no partial rows in the target.

`write_mode = UPSERT_NO_DELETE`, so the apply step is a `MERGE` keyed on
the primary key — staging collects the day's raw rows, then a single
MERGE folds them into the target. Re-running a day is therefore
idempotent (the same keys merge, no duplicates).

No `State()` — MSSQL has no state coordination, so there's no applied
gate; every interval reruns each run. Staging still gives you chunked,
atomic-per-interval apply. (Setting `State()` on an MSSQL model is a
hard error — `MssqlData` rejects it.)
"""

from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    Bounds,
    Database,
    IntervalChunks,
    Kind,
    Model,
    Tags,
    Target,
    WriteMode,
)
from bollhav.mssql import MssqlColumn, MssqlType
from bollhav.mssql.staging import MssqlStaging


sales = Model(
    target=Target(
        name="sales",
        schema="warehouse",
        catalog="demo",
        database=Database.MSSQL,
        write_mode=WriteMode.UPSERT_NO_DELETE,  # apply = MERGE on the PK
        dsn_env_var="BOLLHAV_MSSQL_DSN",
        staging=MssqlStaging(),  # ← opt in to staging
        columns=[
            MssqlColumn(
                name="sale_id",
                data_type=MssqlType.BIGINT,
                nullable=False,
                primary_key=True,  # MERGE key
            ),
            MssqlColumn(
                name="sold_at",
                data_type=MssqlType.DATETIME2,
                nullable=False,
            ),
            MssqlColumn(
                name="amount",
                data_type=MssqlType.DECIMAL,
                precision=12,
                scale=2,
                nullable=False,
            ),
        ],
    ),
    kind=Kind.INTERVAL,
    # Two independent knobs: daily interval windows + rows per read chunk.
    batching=Batch(
        interval=IntervalChunks(expression="@daily"),
        size=2000,
    ),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    tagging=Tags(tags={"sales"}),
)
