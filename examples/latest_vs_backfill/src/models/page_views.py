from datetime import datetime, timezone
from bollhav.model import Model, Source, Target, Schema, WriteMode, Tags, Bounds, Batch


# Single hourly model. Bounds cover three days of history.
#
# Under LATEST_ENABLED=true this yields one chunk — the most recent
# completed hour (the current hour is "in progress" and is never returned).
#
# Under BACKFILL_ENABLED=true (default) with BACKFILL_SINCE/BACKFILL_UNTIL
# set, you get every hour in that window: 72 chunks for a full 3-day
# backfill, chunked by the batch expression.
page_views = Model(
    source=Source(name="page_views"),
    target=Target(
        name="page_views",
        schema=Schema(name="warehouse_web"),
        write_mode=WriteMode.APPEND,
    ),
    tagging=Tags(tags={"web"}),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@hourly"),
)
