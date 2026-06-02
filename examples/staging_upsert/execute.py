"""Per-interval execute function, wrapped by `@interval_lifecycle`.

The hook gates on the state row (skip if applied), takes the
per-interval lock, marks running, runs this body, then marks applied.

The connection is passed in by the loop in `main.py`. The `write()`
call is the same shape regardless of staging mode — bollhav dispatches
on `model.target.staging` (configured in `src/models/orders.py`).
"""

from __future__ import annotations

from bollhav.model import Model, interval_lifecycle
from bollhav.postgres import write
from mock_read import read


@interval_lifecycle
def execute_interval(model: Model, interval, data_conn, state_conn=None) -> None:
    df_gen = read(since=interval.since, until=interval.until)
    write(
        conn=data_conn,
        model=model,
        df_gen=df_gen,
        since=interval.since,
        until=interval.until,
    )
