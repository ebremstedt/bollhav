"""Per-interval execute function.

`@state` wraps this: gates on the state row (skip if applied), runs,
then lets the staged apply flip the state row inside its own tx.

The `write()` call is the same shape regardless of staging mode —
bollhav dispatches on `model.target.staging` (configured in
`src/models/orders.py`).
"""

from __future__ import annotations

import os
from datetime import datetime

import psycopg

from bollhav.model import Model, state
from bollhav.postgres import write
from mock_read import read


@state
def execute(model: Model, since: datetime, until: datetime) -> None:
    df_gen = read(since=since, until=until)
    with psycopg.connect(os.environ[model.target.dsn_env_var]) as conn:
        write(conn=conn, model=model, df_gen=df_gen, since=since, until=until)
