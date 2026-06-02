"""Per-interval execute function, wrapped by `@interval_lifecycle`.

The hook gates on the state row (skip if applied), takes the
per-interval advisory lock, marks the row `running`, runs this body,
then marks it `applied` on a clean return (or `error` on exception).

The connection is passed in — the loop in `main.py` opens it and threads
it through as `data_conn`. `write()` is the same shape regardless of
staging: bollhav dispatches internally on `model.target.staging` (set
on the model in `src/models/orders.py`).
"""

from __future__ import annotations

import os
import time

from bollhav.model import Model, interval_lifecycle
from bollhav.postgres import write
from mock_read import read


@interval_lifecycle
def execute_interval(model: Model, interval, data_conn, state_conn=None) -> None:
    # Optional artificial delay — gives the dashboard time to show the
    # `running` spinner before the row flips to `applied`. Default: no
    # sleep. Set SLEEP_PER_INTERVAL=2 to slow each interval by 2s.
    sleep_for = float(os.environ.get("SLEEP_PER_INTERVAL", "0"))
    if sleep_for > 0:
        time.sleep(sleep_for)

    df_gen = read(since=interval.since, until=interval.until)
    write(
        conn=data_conn,
        model=model,
        df_gen=df_gen,
        since=interval.since,
        until=interval.until,
    )
