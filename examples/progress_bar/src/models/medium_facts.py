from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    IntervalChunks,
    Model,
    Schema,
    Source,
    Tags,
    Target,
    WriteMode,
)
from bollhav.model.bounds import Bounds


# Hourly model with a 12-hour backfill window — 12 chunks.
medium_facts = Model(
    source=Source(name="medium_facts_source"),
    target=Target(
        name="medium_facts",
        schema=Schema(name="warehouse_clean"),
        write_mode=WriteMode.APPEND,
    ),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
        end=datetime(2024, 1, 1, 12, tzinfo=timezone.utc),
    ),
    tagging=Tags(tags={"facts"}),
    batching=Batch(interval=IntervalChunks(expression="@hourly")),
)
