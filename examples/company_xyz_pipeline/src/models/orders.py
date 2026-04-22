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

# recreate_table — drops and recreates the table before the load, then appends.
# Depends on customers being loaded first.
orders = Model(
    source=Source(name="orders"),
    target=Target(
        name="orders",
        schema=Schema(name="warehouse_clean"),
        write_mode=WriteMode.APPEND,
        recreate_table=True,
    ),
    tagging=Tags(tags={"orders"}),
    upstream=["warehouse_clean.customer_master_data"],
    batch_sleep=0.25,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 11, tzinfo=timezone.utc),
    ),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
)
