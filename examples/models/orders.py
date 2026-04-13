from datetime import datetime, timezone
from bollhav.model import Model, Source, Target, Schema, WriteMode, Tags, Bounds, Batch

# RECREATE_TABLE_INSERT — drops and recreates the table then inserts.
# Good for tables whose schema might change between runs, or when truncate is not available.
orders = Model(
    name="orders",
    source=Source(name="orders"),
    target=Target(
        name="orders",
        schema=Schema(name="cosmic_5583"),
        write_mode=WriteMode.RECREATE_TABLE_INSERT,
    ),
    tagging=Tags(tags={"orders"}),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 11, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)
