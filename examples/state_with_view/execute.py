"""Per-step execute. Dispatches on `model.target.is_view`.

For state-tracked tables, `@state` does the usual lifecycle (gate on
applied/blocked, mark running, run, mark applied via staged flush).
For the view-model, `@state` is a passthrough (no `state=State(...)`
on the view) and the body just issues a `CREATE OR REPLACE VIEW`.
"""

from __future__ import annotations

import os
from datetime import datetime

import psycopg

from bollhav.model import Model, state
from bollhav.postgres import write
from mock_read import read


@state
def execute(model: Model, since: datetime | None, until: datetime | None) -> None:
    with psycopg.connect(os.environ[model.target.dsn_env_var]) as conn:
        if model.target.is_view:
            write(conn=conn, model=model)
            return
        df_gen = read(since=since, until=until)
        write(conn=conn, model=model, df_gen=df_gen, since=since, until=until)
