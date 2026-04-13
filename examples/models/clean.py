from datetime import datetime, timezone
from bollhav.model import Model, Source, Target, Schema, WriteMode, Tags, Bounds, Batch

product_price_history = Model(
    source=Source(name="products"),
    target=Target(
        name="product_price_history",
        schema=Schema(name="warehouse_clean"),
        write_mode=WriteMode.APPEND,
    ),
    tagging=Tags(tags={"clean"}),
    upstream=["warehouse_raw.products"],
    batch_sleep=0.12,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 11, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)

shipment_tracking_summary = Model(
    source=Source(name="shipment_events"),
    target=Target(
        name="shipment_tracking_summary",
        schema=Schema(name="warehouse_clean"),
        write_mode=WriteMode.RECREATE_TABLE_INSERT,
    ),
    tagging=Tags(tags={"clean"}),
    upstream=["warehouse_raw.shipment_events"],
    batch_sleep=0.3,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 6, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)

inventory_snapshot = Model(
    source=Source(name="inventory_transactions"),
    target=Target(
        name="inventory_snapshot",
        schema=Schema(name="warehouse_clean"),
        write_mode=WriteMode.TRUNCATE_TABLE_INSERT,
    ),
    tagging=Tags(tags={"clean"}),
    upstream=["warehouse_raw.inventory_transactions"],
    batch_sleep=0.01,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 2, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)
