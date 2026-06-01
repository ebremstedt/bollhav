"""Orders model — exercises staging with UPSERT_NO_DELETE on both sides.

Three @daily intervals over 2024-01-01..2024-01-04. Each interval
emits ~300 orders × 4 status updates = 1200 rows in chunks, with
duplicates spread across chunks.

  - `Staging(write_mode=WriteMode.UPSERT_NO_DELETE)` — each chunk
    MERGEs into staging on `id`, keeping only the latest status per
    order in the staging table.
  - `Target.write_mode=WriteMode.UPSERT_NO_DELETE` — the apply step
    MERGEs the (already-deduped) staging into the target.

Net result per interval: target.orders has exactly 300 rows, one per
order id, with the LATEST status observed in that interval.
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
        write_mode=WriteMode.UPSERT_NO_DELETE,
        dsn_env_var="TARGET_DSN",
        staging=Staging(
            # Chunks MERGE into staging on the unique cols → staging
            # holds one row per `id`, never accumulating dupes.
            write_mode=WriteMode.UPSERT_NO_DELETE,
        ),
        columns=[
            PostgresColumn(
                name="id",
                data_type=PostgresType.BIGINT,
                nullable=False,
                primary_key=True,
                unique=True,
            ),
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
                name="status",
                data_type=PostgresType.TEXT,
                nullable=False,
            ),
            PostgresColumn(
                name="updated_at",
                data_type=PostgresType.TIMESTAMPTZ,
                nullable=False,
            ),
        ],
    ),
    state=State(),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    tagging=Tags(tags={"orders"}),
)
