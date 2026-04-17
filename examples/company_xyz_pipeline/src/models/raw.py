from datetime import datetime, timezone
from bollhav.model import Model, Source, Target, Schema, WriteMode, Tags, Bounds, Batch

inventory_transactions = Model(
    source=Source(name="inventory_transactions"),
    target=Target(
        name="inventory_transactions",
        schema=Schema(name="warehouse_raw"),
        write_mode=WriteMode.APPEND,
    ),
    tagging=Tags(tags={"raw"}),
    batch_sleep=0.4,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 11, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)

supplier_catalog = Model(
    source=Source(name="supplier_catalog"),
    target=Target(
        name="supplier_catalog",
        schema=Schema(name="warehouse_raw"),
        write_mode=WriteMode.RECREATE_TABLE_INSERT,
    ),
    tagging=Tags(tags={"raw"}),
    batch_sleep=0.02,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 3, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)

shipment_events = Model(
    source=Source(name="shipment_events"),
    target=Target(
        name="shipment_events",
        schema=Schema(name="warehouse_raw"),
        write_mode=WriteMode.APPEND,
    ),
    tagging=Tags(tags={"raw"}),
    batch_sleep=0.08,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 11, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)

exchange_rates = Model(
    source=Source(name="exchange_rates"),
    target=Target(
        name="exchange_rates",
        schema=Schema(name="warehouse_raw"),
        write_mode=WriteMode.APPEND,
    ),
    tagging=Tags(tags={"raw"}),
    batch_sleep=0.03,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 21, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)

audit_log = Model(
    source=Source(name="audit_log"),
    target=Target(
        name="audit_log",
        schema=Schema(name="warehouse_raw"),
        write_mode=WriteMode.APPEND,
    ),
    tagging=Tags(tags={"raw"}),
    batch_sleep=0.06,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 8, tzinfo=timezone.utc),
    ),
    batching=Batch(batch_expression="@daily"),
)
