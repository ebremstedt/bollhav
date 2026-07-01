"""daily_report — a FIXED interval table downstream of the FLEXIBLE one.

The headline valid combination: it gates `clean_events` (FLEXIBLE) with
`ENCAPSULATE`. Coverage works fine against a flexible upstream — once
`clean_events` has covered this report's window, the gate is satisfied. (Had
this used `EXACT` on `clean_events`, it would be a hard error — a flexible
upstream has no durable exact-grain row to match.)

It aggregates, so it must stay FIXED itself (`ChunkFix`, the
default): an aggregate is not partition-invariant.
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


daily_report = Model(
    target=Target(
        name="daily_report",
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
            PostgresColumn(
                name="event_count", data_type=PostgresType.BIGINT, nullable=False
            ),
            PostgresColumn(
                name="total", data_type=PostgresType.NUMERIC, nullable=False
            ),
        ],
    ),
    temporality=Temporality.TEMPORAL,
    state=State(),
    batching=Batch(time=TimeChunking(chunk="@daily")),  # fixed — it aggregates
    contract=Contract(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    upstream=[
        Source(
            "demo.lake.clean_events",
            type=SourceModel(),
            contract=UpstreamContract.ENCAPSULATE,  # ✓ coverage on a FLEXIBLE upstream
        ),
    ],
    tagging=Tags(tags={"demo"}),
)
