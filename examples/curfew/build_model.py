"""Build the `events` model with a given curfew.

`Model` is frozen and `curfew` is part of the definition, so each run that
wants a different curfew builds a fresh model. Everything else (target, state,
batching, bounds) is identical between runs — only the curfew window moves —
so the demo isolates the one variable: is *now* inside a denied window?
"""

from __future__ import annotations

from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    Contract,
    Curfew,
    Database,
    Temporality,
    Model,
    State,
    Tags,
    Target,
    TimeChunking,
    WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType


def events_model(curfew: Curfew) -> Model:
    return Model(
        target=Target(
            name="events",
            schema="warehouse",
            catalog="demo",
            database=Database.POSTGRES,
            write_mode=WriteMode.APPEND,
            dsn_env_var="TARGET_DSN",
            columns=[
                PostgresColumn(
                    name="id", data_type=PostgresType.BIGINT, nullable=False
                ),
                PostgresColumn(
                    name="kind", data_type=PostgresType.TEXT, nullable=False
                ),
                PostgresColumn(
                    name="event_time",
                    data_type=PostgresType.TIMESTAMPTZ,
                    nullable=False,
                ),
            ],
        ),
        temporality=Temporality.TEMPORAL,
        state=State(),  # stateful → curfew-skipped intervals stay pending
        curfew=curfew,
        batching=Batch(time=TimeChunking(chunk="@daily")),
        contract=Contract(
            begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 4, tzinfo=timezone.utc),  # → 3 daily intervals
        ),
        tagging=Tags(tags={"demo"}),
    )
