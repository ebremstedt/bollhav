"""High-value orders — a VIEW on top of `warehouse.orders`.

Views don't have intervals or state. But bollhav's library tracks
every registered model, and views auto-register on every run.

The downstream `high_value_sums` model declares this view as an
upstream. When the downstream bootstraps, it looks the view up in
the library:

  * Library row found → satisfied (presence is enough — views are
    time-agnostic, they reflect their underlying table at query
    time).
  * Library row missing → downstream intervals come up as `blocked`
    with reason `STATE_001: upstream 'warehouse.v_high_value_orders'
    not registered`.

No `library=True` needed here — view-models always auto-register.
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
    tagging=Tags(tags={"view"}),
)
