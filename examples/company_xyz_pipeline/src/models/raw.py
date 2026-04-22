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

inventory_transactions = Model(
    source=SourceTable(name="inventory_transactions"),
    target=Target(
        name="inventory_transactions",
        schema=TargetSchema(name="warehouse_raw"),
        write_mode=WriteMode.APPEND,
    ),
    tagging=Tags(tags={"raw"}),
    batch_sleep=0.4,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 11, tzinfo=timezone.utc),
    ),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
)

supplier_catalog = Model(
    source=SourceTable(name="supplier_catalog"),
    target=Target(
        name="supplier_catalog",
        schema=TargetSchema(name="warehouse_raw"),
        write_mode=WriteMode.APPEND,
        recreate_table=True,
    ),
    tagging=Tags(tags={"raw"}),
    batch_sleep=0.02,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 3, tzinfo=timezone.utc),
    ),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
)

shipment_events = Model(
    source=SourceTable(name="shipment_events"),
    target=Target(
        name="shipment_events",
        schema=TargetSchema(name="warehouse_raw"),
        write_mode=WriteMode.APPEND,
    ),
    tagging=Tags(tags={"raw"}),
    batch_sleep=0.08,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 11, tzinfo=timezone.utc),
    ),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
)

exchange_rates = Model(
    source=SourceTable(name="exchange_rates"),
    target=Target(
        name="exchange_rates",
        schema=TargetSchema(name="warehouse_raw"),
        write_mode=WriteMode.APPEND,
    ),
    tagging=Tags(tags={"raw"}),
    batch_sleep=0.03,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 21, tzinfo=timezone.utc),
    ),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
)

audit_log = Model(
    source=SourceTable(name="audit_log"),
    target=Target(
        name="audit_log",
        schema=TargetSchema(name="warehouse_raw"),
        write_mode=WriteMode.APPEND,
    ),
    tagging=Tags(tags={"raw"}),
    batch_sleep=0.06,
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 8, tzinfo=timezone.utc),
    ),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
)
