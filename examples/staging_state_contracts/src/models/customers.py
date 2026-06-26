"""customers — a VIEW, with state.

A view's "asset" is its definition: `@model_lifecycle` runs the
`CREATE OR REPLACE VIEW` (from `model.query`) — there's no data to write,
so the execute body does nothing. Because it's state-tracked, it gets a
single existence row that flips to `applied` once the view is in place.
That row is what a downstream's `WINDOW` contract resolves to for a view
upstream (its existence row applied).

`temporality=Temporality.TIMELESS, view=True` is what marks it a view; `write_mode` is irrelevant for
views (it's a data-write strategy) and is left at its default.
"""

from bollhav.model import (
    Database,
    Temporality,
    Model,
    Source,
    SourceModel,
    State,
    Tags,
    Target,
)
from bollhav.postgres import PostgresColumn, PostgresType


customers = Model(
    temporality=Temporality.TIMELESS,
    view=True,
    query="SELECT DISTINCT customer_id FROM warehouse.orders",
    target=Target(
        name="customers",
        schema="warehouse",
        catalog="demo",
        database=Database.POSTGRES,
        dsn_env_var="TARGET_DSN",
        columns=[
            PostgresColumn(
                name="customer_id", data_type=PostgresType.BIGINT, nullable=False
            ),
        ],
    ),
    # The view reads the orders table, so the pipeline creates orders first.
    upstream=[
        Source(
            "demo.warehouse.orders",
            type=SourceModel(),
        )
    ],
    state=State(),  # so it registers (kind=view) with an existence row
    tagging=Tags(tags={"demo"}),
)
