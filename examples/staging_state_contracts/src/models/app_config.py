"""app_config — a MONOLITH (whole-table) table, with state.

No batching: its unit of work is "load the whole table," not a time
window. `monolithic=True` says so explicitly (a forgotten `batching`
won't silently make a model monolithic). It gets a single whole-table
state row that flips to `applied` once loaded — what a `MonolithicContract`
checks. Re-runs skip it (already applied) until the state is reset.
"""

from bollhav.model import (
    Database,
    Model,
    State,
    Tags,
    Target,
    TargetSchema,
    WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType


app_config = Model(
    target=Target(
        name="app_config",
        schema=TargetSchema(name="warehouse"),
        database=Database.POSTGRES,
        write_mode=WriteMode.APPEND,
        dsn_env_var="TARGET_DSN",
        columns=[
            PostgresColumn(name="key", data_type=PostgresType.TEXT, nullable=False),
            PostgresColumn(name="value", data_type=PostgresType.TEXT, nullable=False),
        ],
    ),
    state=State(),
    monolithic=True,  # whole-table unit of work, no intervals
    tagging=Tags(tags={"demo"}),
)
