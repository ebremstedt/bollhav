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


# Hourly model with a full-day backfill window — 24 chunks.
slow_facts = Model(
    source=Source(name="slow_facts_source"),
    target=Target(
        name="slow_facts",
        schema=Schema(name="warehouse_clean"),
        write_mode=WriteMode.APPEND,
    ),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
        end=datetime(2024, 1, 2, 0, tzinfo=timezone.utc),
    ),
    tagging=Tags(tags={"facts"}),
    batching=Batch(interval=IntervalChunks(expression="@hourly")),
)
