from datetime import datetime, timezone
from bollhav.model import Model, Source, Target, Schema, WriteMode, Tags, Bounds, Batch

# APPEND — rows are added every run, nothing is ever removed or deduplicated.
products = Model(
    source=Source(name="products"),
    target=Target(
        name="products",
        schema=Schema(name="warehouse_raw"),
        write_mode=WriteMode.APPEND,
    ),
    tagging=Tags(tags={"products"}),
    batch_sleep=0.15,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 11, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)
