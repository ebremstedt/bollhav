from datetime import datetime, timezone
from bollhav.model import Model, Source, Target, Schema, WriteMode, Tags, Bounds, Batch

# TRUNCATE_TABLE_INSERT — wipes the table then bulk inserts fresh data every run.
# Good for dimension tables or reference data that are small enough to fully reload.
customers = Model(
    name="customers",
    source=Source(name="customers"),
    target=Target(
        name="customers",
        schema=Schema(name="cosmic_clean"),
        write_mode=WriteMode.TRUNCATE_TABLE_INSERT,
    ),
    tagging=Tags(tags={"customers"}),
    batch_sleep=0.05,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 11, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)
