"""app_config — a MONOLITH (whole-table) table, with state.

No batching: its unit of work is "load the whole table," not a time
window. `kind=Kind.TIMELESS` says so explicitly (a forgotten `batching`
won't silently make a model monolithic). It gets a single whole-table
state row that flips to `applied` once loaded — what a downstream's `WINDOW`
contract resolves to for a whole-table upstream (its existence row applied).
Re-runs skip it (already applied) until the state is reset.
"""

from bollhav.model import (
    Database,
    Kind,
    Model,
    State,
    Tags,
    Target,
    WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType


app_config = Model(
    kind=Kind.TIMELESS,
    target=Target(
        name="app_config",
        schema="warehouse",
        catalog="demo",
        database=Database.POSTGRES,
        write_mode=WriteMode.APPEND,
        dsn_env_var="TARGET_DSN",
        columns=[
            PostgresColumn(name="key", data_type=PostgresType.TEXT, nullable=False),
            PostgresColumn(name="value", data_type=PostgresType.TEXT, nullable=False),
        ],
    ),
    state=State(),
    tagging=Tags(tags={"demo"}),
)
