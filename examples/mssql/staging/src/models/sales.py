"""Sales model — MSSQL data + **staging**, with state tracked in **Postgres**.

A model whose *data* lives in MSSQL (staged through the same lifecycle hooks
as Postgres) but whose *state* lives in Postgres. Three knobs:

  * `batching.time` = `@daily` → one `(since, until)` window per day.
  * `target.staging` = `MssqlStaging()` → each window's chunks bulk-insert
    into a per-interval staging table; one transaction then applies that
    staging table to the target and drops it. A crash mid-stream leaves
    no partial rows in the target.
  * `state = State()` → the run is gated/tracked. State always lives in
    Postgres (the only backend), so an MSSQL-data model keeps its state in
    Postgres: the run passes a **separate** Postgres `state_conn` alongside
    the MSSQL `data_conn`. `@model_lifecycle`'s `_conns` enforces this —
    forgetting the Postgres connection is a clear error, not a driver crash.

`write_mode = UPSERT_NO_DELETE`, so the apply step is a `MERGE` keyed on
the primary key — staging collects the day's raw rows, then a single
MERGE folds them into the target. Re-running a day is idempotent (same
keys merge), and with state the applied gate skips it entirely.
"""

from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    Contract,
    Database,
    TimeChunking,
    Kind,
    Model,
    State,
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
    kind=Kind.TEMPORAL,
    state=State(),  # tracked/gated; state rows live in Postgres (separate conn)
    # Two independent knobs: daily interval windows + rows per read chunk.
    batching=Batch(
        time=TimeChunking(chunk="@daily"),
        size=2000,
    ),
    contract=Contract(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    tagging=Tags(tags={"sales"}),
)
