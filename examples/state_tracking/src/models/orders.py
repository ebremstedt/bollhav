from datetime import datetime, timezone
from bollhav.model import (
    Batch,
    Bounds,
    IntervalChunks,
    Model,
    SourceTable,
    State,
    Tags,
    Target,
    TargetSchema,
    WriteMode,
)

# orders is state-tracked. Each daily interval gets a row in
# z_warehouse_clean.orders_state (auto-prefixed schema because no
# separate state DSN is configured — state lives in the same DB as
# the target).
orders = Model(
    source=SourceTable(name="orders"),
    target=Target(
        name="orders",
        schema=TargetSchema(name="warehouse_clean"),
        write_mode=WriteMode.APPEND,
        dsn_env_var="TARGET_DSN",
    ),
    tagging=Tags(tags={"orders"}),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 11, tzinfo=timezone.utc),
    ),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
    state=State(),  # opt in; falls back to TARGET_DSN
)
