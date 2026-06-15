"""events — an INTERVAL table written DIRECTLY (no staging), APPEND mode.

Each interval's rows are COPYed straight into the target in their own
transaction — there's no per-interval staging table. Simpler than the staged
path, but a crash mid-stream can leave partial rows (staging is what makes a
write atomic). State still tracks one row per interval, so reruns are gated.

This shows that `state` and `staging` are independent: you can have a fully
state-tracked, idempotent-by-gating pipeline with no staging at all.
"""

from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    Contract,
    Database,
    TimeChunking,
    Temporality,
    Model,
    State,
    Tags,
    Target,
    WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType


events = Model(
    target=Target(
        name="events",
        schema="warehouse",
        catalog="demo",
        database=Database.POSTGRES,
        write_mode=WriteMode.APPEND,
        dsn_env_var="TARGET_DSN",
        # no `staging=` → the direct write path (write_dataframes → append)
        columns=[
            PostgresColumn(name="id", data_type=PostgresType.BIGINT, nullable=False),
            PostgresColumn(name="kind", data_type=PostgresType.TEXT, nullable=False),
            PostgresColumn(
                name="event_time",
                data_type=PostgresType.TIMESTAMPTZ,
                nullable=False,
            ),
        ],
    ),
    temporality=Temporality.TEMPORAL,
    state=State(),  # one state row per interval — gates reruns
    batching=Batch(time=TimeChunking(chunk="@daily")),
    contract=Contract(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    tagging=Tags(tags={"demo"}),
)
