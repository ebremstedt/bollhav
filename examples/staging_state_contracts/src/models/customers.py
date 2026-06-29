"""customers — a VIEW, with state.

A view's "asset" is its definition: `@model_lifecycle` runs the
`CREATE OR REPLACE VIEW` from the model's defining `query` (its SELECT body) —
there's no data to write, so the execute body does nothing. Because it's
state-tracked, it gets a single existence row that flips to `applied` once the
view is in place. That row is what a downstream's `ENCAPSULATE` contract
resolves to for a view upstream (its existence row applied).

`materialization=Materialization.VIEW` is what marks it a view (with its `query`
as the body); `write_mode` is irrelevant for views (it's a data-write strategy)
and is left at its default.
"""

from bollhav.model import (
    Database,
    Materialization,
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
    materialization=Materialization.VIEW,
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
    # The view reads the orders table, so the pipeline creates orders first —
    # declared as a plain upstream dependency (the view body lives on `query`).
    upstream=[Source("demo.warehouse.orders", type=SourceModel())],
    state=State(),  # so it registers (kind=view) with an existence row
    tagging=Tags(tags={"demo"}),
)
