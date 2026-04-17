from datetime import datetime, timezone
from bollhav.model import Model, Source, Target, Schema, WriteMode, Tags, Bounds, Batch

# TRUNCATE_TABLE_INSERT — wipes the table then bulk inserts fresh data every run.
customers = Model(
    source=Source(name="customers"),
    target=Target(
        name="customer_master_data",
        schema=Schema(name="warehouse_clean"),
        write_mode=WriteMode.TRUNCATE_TABLE_INSERT,
    ),
    tagging=Tags(tags={"customers"}),
    batch_sleep=0.05,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)
