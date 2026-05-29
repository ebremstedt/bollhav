"""High-value orders — a VIEW on top of `warehouse.orders`.

Views don't have intervals or state. To be claimable as an upstream
by a downstream model, the view-model opts in to the library with
`library=True` — same opt-in mechanism as a state-less TABLE.

The downstream `high_value_sums` model declares this view as an
upstream. When the downstream bootstraps, it looks the view up in
the library:

  * Library row found → satisfied (presence is enough — views are
    time-agnostic, they reflect their underlying table at query
    time).
  * Library row missing (`library=True` not set on this model, or it
    has never run) → downstream intervals come up as `blocked` with
    reason `STATE_001: upstream 'warehouse.v_high_value_orders' not
    registered`.

A view declared without `library=True` is still a valid bollhav
model — bollhav will `CREATE OR REPLACE VIEW` it — it just won't
appear in the library and therefore can't be claimed as an upstream.
"""

from bollhav.model import (
    Model,
    ModelType,
    SourceTable,
    Tags,
    Target,
    TargetSchema,
    WriteMode,
)


v_high_value_orders = Model(
    source=SourceTable(
        name="v_high_value_orders",
        query="SELECT * FROM warehouse.orders WHERE total >= 100",
    ),
    target=Target(
        name="v_high_value_orders",
        schema=TargetSchema(name="warehouse"),
        model_type=ModelType.VIEW,
        write_mode=WriteMode.VIEW,
        dsn_env_var="TARGET_DSN",
    ),
    upstream=["warehouse.orders"],
    library=True,  # opt in so `high_value_sums` can claim it as upstream
    tagging=Tags(tags={"view"}),
)
