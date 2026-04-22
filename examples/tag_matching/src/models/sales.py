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


# sales_orders: NO explicit tags at all.
#
# You might be tempted to write `tagging=Tags(tags={"sales"})` here — but
# you don't have to. "sales" is already derived automatically from the
# schema name "warehouse_sales" (snake-split) AND from the table name
# "sales_orders" (snake-split). Writing it again would be redundant.
#
# Final auto-tag set for this model:
#   {all, orders, sales, sales_orders,
#    warehouse, warehouse_sales, warehouse_sales.sales_orders}
sales_orders = Model(
    source=SourceTable(name="sales_orders"),
    target=Target(
        name="sales_orders",
        schema=TargetSchema(name="warehouse_sales"),
        write_mode=WriteMode.APPEND,
    ),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 3, tzinfo=timezone.utc),
    ),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
)


# sales_forecasts: only ONE explicit tag — "forecast".
#
# "sales" comes for free from the schema and the name. But "forecast"
# (singular) is NOT derivable from anywhere — the snake-split of
# "sales_forecasts" gives ["sales", "forecasts"] (plural). If we want
# TAGS="[forecast]" to match this model, we have to add it by hand.
#
# The rule of thumb: only list tags that cannot be derived from the
# model's name or schema.
sales_forecasts = Model(
    source=SourceTable(name="sales_forecasts"),
    target=Target(
        name="sales_forecasts",
        schema=TargetSchema(name="warehouse_sales"),
        write_mode=WriteMode.APPEND,
        truncate_table=True,
    ),
    tagging=Tags(tags={"forecast"}),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 3, tzinfo=timezone.utc),
    ),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
)
