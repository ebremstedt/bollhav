"""Per-interval execute function.

`@state_tracker` wraps this: gates on the state row (skip if applied),
runs, then either lets the staged flush flip the state row inside its
own tx, or — for non-staged models — issues `mark_applied` after a
successful run.

The `write()` call is the same shape regardless of staging: bollhav
dispatches internally on `model.target.staging` (set on the model in
`src/models/orders.py`).
"""

from __future__ import annotations

import os
from datetime import datetime

import psycopg

from bollhav.model import Model, state_tracker
from bollhav.postgres import write
from mock_read import read


@state_tracker
def execute(model: Model, since: datetime, until: datetime) -> None:
    df_gen = read(since=since, until=until)
    with psycopg.connect(os.environ[model.target.dsn_env_var]) as conn:
        write(conn=conn, model=model, df_gen=df_gen, since=since, until=until)
