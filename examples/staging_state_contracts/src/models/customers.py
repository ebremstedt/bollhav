"""customers — a VIEW, with state.

A view's "asset" is its definition: `@model_lifecycle` runs the
`CREATE OR REPLACE VIEW` (from `SourceTable.query`) — there's no data to
write, so the execute body does nothing. Because it's state-tracked, it
gets a single existence row that flips to `applied` once the view is in
place. That row is what a `ViewContract` checks.

`model_type=VIEW` is what marks it a view; `write_mode` is irrelevant for
views (it's a data-write strategy) and is left at its default.
"""

from bollhav.model import (
    Database,
    Model,
    ModelType,
    SourceTable,
    State,
    Tags,
    Target,
    TargetSchema,
)
from bollhav.postgres import PostgresColumn, PostgresType


customers = Model(
    target=Target(
        name="customers",
        schema=TargetSchema(name="warehouse"),
        database=Database.POSTGRES,
        model_type=ModelType.VIEW,
        dsn_env_var="TARGET_DSN",
        columns=[
            PostgresColumn(
                name="customer_id", data_type=PostgresType.BIGINT, nullable=False
            ),
        ],
    ),
    # The view reads the orders table, so the pipeline creates orders first.
    source=SourceTable(
        name="orders",
        query="SELECT DISTINCT customer_id FROM warehouse.orders",
    ),
    state=State(),  # so it registers (kind=view) with an existence row
    tagging=Tags(tags={"demo"}),
)
