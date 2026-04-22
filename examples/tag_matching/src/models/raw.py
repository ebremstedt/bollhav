from datetime import datetime, timezone
from bollhav.model import (
    Model,
    SourceTable,
    Target,
    TargetSchema,
    WriteMode,
    Bounds,
    Batch,
    IntervalChunks,
)


# raw_events: NO explicit tags.
#
# "raw" is already contributed by the schema-split of "warehouse_raw"
# AND by the name-split of "raw_events". So TAGS="[raw]" matches this
# model without us adding anything manually. If you ever find yourself
# writing `tagging=Tags(tags={"raw"})` on a model already named or
# schema'd `*_raw_*` — delete it, it's doing nothing.
raw_events = Model(
    source=SourceTable(name="raw_events"),
    target=Target(
        name="raw_events",
        schema=TargetSchema(name="warehouse_raw"),
        write_mode=WriteMode.APPEND,
    ),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 3, tzinfo=timezone.utc),
    ),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
)
