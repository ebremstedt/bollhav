from datetime import datetime, timezone
from bollhav.model import Model, Source, Target, Schema, WriteMode, Tags, Bounds, Batch

# RECREATE_TABLE_INSERT — drops and recreates the table then inserts.
# Depends on customers being loaded first.
orders = Model(
    source=Source(name="orders"),
    target=Target(
        name="orders",
        schema=Schema(name="warehouse_clean"),
        write_mode=WriteMode.RECREATE_TABLE_INSERT,
    ),
    tagging=Tags(tags={"orders"}),
    upstream=["warehouse_clean.customer_master_data"],
    batch_sleep=0.25,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 11, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)
