from datetime import datetime, timezone
from bollhav.model import Model, Source, Target, Schema, WriteMode, Tags, Bounds, Batch

# APPEND — rows are added every run, nothing is ever removed or deduplicated.
# Good for event streams or audit logs where you want the full history.
products = Model(
    name="products",
    source=Source(name="products"),
    target=Target(
        name="products",
        schema=Schema(name="cosmic_raw"),
        write_mode=WriteMode.APPEND,
    ),
    tagging=Tags(tags={"products"}),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 11, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)
