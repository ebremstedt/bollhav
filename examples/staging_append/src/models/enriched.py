"""Downstream model depending on `warehouse.orders`.

Demonstrates the cross-pipeline DAG: this model can be run in a
separate pipeline from orders. The bootstrap looks `warehouse.orders`
up in the model library and checks for satisfying applied rows; if
orders' state covers each of enriched's (since, until) windows, the
intervals are inserted as `pending`. Otherwise they're inserted as
`blocked` with a reason.

To exercise the path:

    # 1. Run orders first — registers it in the library, applies state.
    TAGS="[orders]" python main.py

    # 2. Now run enriched — library lookup finds orders, applied rows
    #    cover enriched's contract, so intervals come up as pending.
    TAGS="[enriched]" UPSTREAM=ignore_completely python main.py
"""

from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    Bounds,
    Database,
    IntervalChunks,
    Model,
    Staging,
    State,
    Tags,
    Target,
    TargetSchema,
    WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType


enriched = Model(
    target=Target(
        name="enriched",
        schema=TargetSchema(name="warehouse"),
        database=Database.POSTGRES,
        write_mode=WriteMode.APPEND,
        dsn_env_var="TARGET_DSN",
        staging=Staging(),
        columns=[
            PostgresColumn(name="id", data_type=PostgresType.BIGINT, nullable=False),
            PostgresColumn(
                name="customer_id", data_type=PostgresType.BIGINT, nullable=False
            ),
            PostgresColumn(
                name="total", data_type=PostgresType.NUMERIC, nullable=False
            ),
            PostgresColumn(
                name="order_date",
                data_type=PostgresType.TIMESTAMPTZ,
                nullable=False,
            ),
        ],
    ),
    state=State(),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    upstream=["warehouse.orders"],
    tagging=Tags(tags={"enriched"}),
)
