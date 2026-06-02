"""High-value orders — a VIEW on top of `warehouse.orders`.

Views have no intervals or state, so they don't register in the
cross-pipeline library (only state-tracked models do). The downstream
`high_value_sums` declares this view as an `upstream`, but since the
view isn't in the library that dependency isn't *enforced* — it's
treated as documentation. The downstream's intervals proceed (pending)
rather than blocking on the unregistered view.

bollhav still `CREATE OR REPLACE VIEW`s this model on each run; it just
isn't part of the enforced dependency graph.
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
