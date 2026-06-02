"""Downstream of the view — state-tracked, references the view as upstream.

The point of the example: a state-tracked model whose `upstream` is a
VIEW. Views don't register in the library (only state-tracked models
do), so this dependency is **not enforced** — it's documentation.

At bootstrap and at each interval's live re-check, the upstream
`warehouse.v_high_value_orders` is looked up in the library and found
to be absent. An unregistered upstream is treated as documentation, so
the interval is **not blocked** — it proceeds as `pending` and runs.

Enforcement happens only between state-tracked models (both registered):
there the downstream waits for the upstream's matching interval to be
`applied`. A view (or any state-less table) can be referenced for
documentation, but bollhav won't gate on it.
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
