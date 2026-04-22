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

# truncate_table — wipes the table before the load, then appends.
customers = Model(
    source=SourceTable(name="customers"),
    target=Target(
        name="customer_master_data",
        schema=TargetSchema(name="warehouse_clean"),
        write_mode=WriteMode.APPEND,
        truncate_table=True,
    ),
    tagging=Tags(tags={"customers"}),
    batch_sleep=0.05,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
)
