from bollhav.model import (
    Batch,
    ChunkMode,
    Model,
    RowChunks,
    TargetSchema,
    SourceTable,
    Tags,
    Target,
    WriteMode,
)
from bollhav.model.bounds import Bounds


# A high-throughput append-only event stream.
#
# We deliberately use ROW mode here because:
#   - the source has no useful time column to slice on,
#   - the row volume is uneven across time (clusters of activity), so any
#     time-interval chunking would produce wildly different chunk sizes,
#   - the downstream sink prefers fixed-size batches for back-pressure.
#
# ROW mode is reload-only — latest and backfill runs raise. Run this with
# the `r:` tag prefix below to put it into reload mode. WriteMode must be
# APPEND (enforced at Model construction).
event_stream = Model(
    source=SourceTable(name="event_stream_raw"),
    target=Target(
        name="event_stream",
        schema=TargetSchema(name="warehouse_raw"),
        write_mode=WriteMode.APPEND,
    ),
    bounds=Bounds(),
    tagging=Tags(tags={"events"}),
    batching=Batch(
        mode=ChunkMode.ROW,
        row=RowChunks(batch_size=250),
    ),
)
