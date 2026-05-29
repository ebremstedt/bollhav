"""Per-interval execute function.

`@state` wraps this: gates on the state row (skip if applied),
runs, then either lets the staged flush flip the state row inside its
own tx, or — for non-staged models — issues `mark_applied` after a
successful run.

The `write()` call is the same shape regardless of staging: bollhav
dispatches internally on `model.target.staging` (set on the model in
`src/models/orders.py`).
"""

from __future__ import annotations

import os
import time
from datetime import datetime

import psycopg

from bollhav.model import Model, state
from bollhav.postgres import write
from mock_read import read


@state
def execute(model: Model, since: datetime, until: datetime) -> None:
    # Optional artificial delay — gives the dashboard time to show the
    # `running` spinner before the row flips to `applied`. Default: no
    # sleep. Set SLEEP_PER_INTERVAL=2 to slow each interval by 2s.
    sleep_for = float(os.environ.get("SLEEP_PER_INTERVAL", "0"))
    if sleep_for > 0:
        time.sleep(sleep_for)

    df_gen = read(since=since, until=until)
    with psycopg.connect(os.environ[model.target.dsn_env_var]) as conn:
        write(conn=conn, model=model, df_gen=df_gen, since=since, until=until)
