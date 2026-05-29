"""Downstream of the view — state-tracked, claims the view as upstream.

This is the point of the example: a state-tracked model whose
upstream is a VIEW.

At bootstrap:
  1. `warehouse.orders` is in the matched set → skipped from upstream
     check (topo order handles it).
  2. `warehouse.v_high_value_orders` is also in the matched set →
     skipped from upstream check.
  3. So at bootstrap time every interval lands as `pending`.

At runtime (`@state` decorator's live re-check before each interval):
  1. Library lookup for `warehouse.v_high_value_orders`.
  2. Entry has `model_type=VIEW`, `state_schema=NULL`, `state_table=NULL`.
  3. `is_satisfied` returns True on presence alone — no applied-row
     query against a non-existent state table.

If you run this model WITHOUT first having run the view (e.g.
`TAGS=[sums]` with no view registered yet), the bootstrap blocks
every interval with `STATE_001: upstream 'warehouse.v_high_value_orders'
not registered`.
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


high_value_sums = Model(
    target=Target(
        name="high_value_sums",
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
    upstream=["warehouse.v_high_value_orders"],
    tagging=Tags(tags={"sums"}),
)
