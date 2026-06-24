"""raw_events — a FIXED interval table (the default).

`fixed_intervals=True` (implicit): state is a grid, one durable `applied` row
per `@daily` chunk at a stable grain. Its exact-grain rows persist, so a
downstream may gate it with ANY contract level — including `EXACT` (see
`audit`) and `ENCAPSULATE` (see `clean_events`).
"""

from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    Contract,
    Database,
    Model,
    Staging,
    State,
    Tags,
    Target,
    Temporality,
    TimeChunking,
    WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType


raw_events = Model(
    target=Target(
        name="raw_events",
        schema="lake",
        catalog="demo",
        database=Database.POSTGRES,
        write_mode=WriteMode.APPEND,
        dsn_env_var="TARGET_DSN",
        staging=Staging(),
        columns=[
            PostgresColumn(name="id", data_type=PostgresType.BIGINT, nullable=False),
            PostgresColumn(
                name="device_id", data_type=PostgresType.BIGINT, nullable=False
            ),
            PostgresColumn(
                name="value", data_type=PostgresType.NUMERIC, nullable=False
            ),
            PostgresColumn(
                name="event_time", data_type=PostgresType.TIMESTAMPTZ, nullable=False
            ),
        ],
    ),
    temporality=Temporality.TEMPORAL,
    state=State(),
    # fixed_intervals defaults to True — this is a fixed grid.
    batching=Batch(time=TimeChunking(chunk="@daily")),
    contract=Contract(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    tagging=Tags(tags={"demo"}),
)
