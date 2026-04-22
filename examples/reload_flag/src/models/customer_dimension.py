from datetime import datetime, timezone
from bollhav.model import (
    Model,
    SourceTable,
    Target,
    TargetSchema,
    WriteMode,
    Tags,
    Bounds,
    Batch,
    IntervalChunks,
)


# A daily dimension table. Bounds define the full history we care about.
#
# Normal incremental runs (no `r:` prefix, LATEST_ENABLED or BACKFILL):
#   - latest mode     → one chunk for the most recent complete day
#   - backfill mode   → whatever BACKFILL_SINCE/BACKFILL_UNTIL spans
#
# Reload runs — triggered by matching this model with an `r:` tag prefix:
#   - bounds.begin → bounds.end is used as the interval
#   - ignores LATEST_ENABLED / BACKFILL_SINCE — they are overridden
#   - chunks the full bounds window by interval_expression
#
# Use case for reload: an upstream schema change invalidated historical
# rows and you want to reprocess the whole history for this model
# specifically, without kicking off a global backfill.
customer_dimension = Model(
    source=SourceTable(name="customers"),
    target=Target(
        name="customer_dimension",
        schema=TargetSchema(name="warehouse_clean"),
        write_mode=WriteMode.APPEND,
        truncate_table=True,
    ),
    tagging=Tags(tags={"customers"}),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 11, tzinfo=timezone.utc),
    ),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
)
