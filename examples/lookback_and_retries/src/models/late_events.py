from datetime import datetime, timezone
from bollhav.model import (
    Model,
    Source,
    Target,
    Schema,
    WriteMode,
    Tags,
    Bounds,
    Batch,
    IntervalChunks,
)


# Hourly model with lookback and retries configured.
#
# lookback=2 → each run's interval is extended 2 hours backwards.
#   Why: upstream emits late-arriving events. A chunk for 12:00-13:00
#   might miss rows that belong to 12:30 but didn't hit the source until
#   13:45. Looking back two hours on every run means we reprocess
#   11:00-13:00 every time we write the 13:00-14:00 chunk, picking up
#   stragglers. Only safe when the write mode is idempotent under
#   reprocessing (here: RECREATE_PARTITION keyed on event_hour, or an
#   UPSERT). This example uses APPEND purely to keep the config short —
#   do not combine lookback with APPEND in real pipelines unless you want
#   duplicates.
#
# retries=3 → a failed chunk is retried up to 3 times before giving up.
#   Note: bollhav stores the value on the model (`model.batching.retries`)
#   but does not implement the retry loop itself. That's the user's job —
#   execute() in this example reads the value and runs the loop.
late_events = Model(
    source=Source(name="late_events"),
    target=Target(
        name="late_events",
        schema=Schema(name="warehouse_events"),
        write_mode=WriteMode.APPEND,
    ),
    tagging=Tags(tags={"events"}),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
        end=datetime(2024, 1, 1, 6, tzinfo=timezone.utc),
    ),
    batching=Batch(
        interval=IntervalChunks(expression="@hourly", lookback=2), retries=3
    ),
)
