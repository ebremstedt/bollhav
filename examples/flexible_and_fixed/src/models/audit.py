"""audit — a FIXED interval table gating `EXACT` on a FIXED upstream.

`EXACT` demands an applied upstream row whose `(since, until)` equals this
audit interval's window exactly. That's valid here because `raw_events` is
FIXED — its per-`@daily` rows are durable and at the same grain, so each
audit day finds its exact match. (The same `EXACT` against a FLEXIBLE upstream
would be rejected — that's the forbidden combo in the README.)
"""

from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    Contract,
    Database,
    Model,
    Source,
    SourceModel,
    Staging,
    State,
    Tags,
    Target,
    Temporality,
    TimeChunking,
    UpstreamContract,
    WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType


audit = Model(
    target=Target(
        name="audit",
        schema="lake",
        catalog="demo",
        database=Database.POSTGRES,
        write_mode=WriteMode.APPEND,
        dsn_env_var="TARGET_DSN",
        staging=Staging(),
        columns=[
            PostgresColumn(
                name="day", data_type=PostgresType.TIMESTAMPTZ, nullable=False
            ),
            PostgresColumn(name="checked", data_type=PostgresType.BOOLEAN, nullable=False),
        ],
    ),
    temporality=Temporality.TEMPORAL,
    state=State(),
    batching=Batch(time=TimeChunking(chunk="@daily")),
    contract=Contract(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    upstream=[
        Source(
            "demo.lake.raw_events",
            type=SourceModel(),
            contract=UpstreamContract.EXACT,  # ✓ exact-grain match on a FIXED upstream
        ),
    ],
    tagging=Tags(tags={"demo"}),
)
