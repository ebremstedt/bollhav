"""Per-step execute, wrapped by `@interval_lifecycle`. Dispatches on
`model.target.is_view`.

For state-tracked tables the hook does the usual lifecycle (gate on
applied/blocked, take the lock, mark running, run, mark applied). For
the view-model there's no `state=State(...)`, and its single "interval"
is `None`, so `@interval_lifecycle` is a pass-through: the body just
issues a `CREATE OR REPLACE VIEW`.
"""

from __future__ import annotations

from bollhav.model import Model, interval_lifecycle
from bollhav.postgres import write
from mock_read import read


@interval_lifecycle
def execute_interval(model: Model, interval, data_conn, state_conn=None) -> None:
    if model.target.is_view:
        write(conn=data_conn, model=model)
        return
    df_gen = read(since=interval.since, until=interval.until)
    write(
        conn=data_conn,
        model=model,
        df_gen=df_gen,
        since=interval.since,
        until=interval.until,
    )
