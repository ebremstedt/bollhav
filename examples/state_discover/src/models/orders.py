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

# 10 daily intervals — 2024-01-01 → 2024-01-11. Small enough to eyeball
# the state table in one psql page, big enough that a mid-run failure
# leaves a meaningful number of pending rows for DISCOVER to pick up.
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
    state=State(),  # falls back to TARGET_DSN, state lives in z_warehouse_clean
)
