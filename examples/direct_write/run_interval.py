"""One unit of work, wrapped by `@execute_lifecycle`.

The hook brackets this: gate on applied → take the lock → mark running → run
this body → mark applied (or record failure). There's **no** staging here, so
`write()` goes straight to the target (APPEND, or DELETE+COPY for
RECREATE_PARTITION). We pass `since`/`until` so RECREATE_PARTITION knows the
window to overwrite; APPEND ignores them.
"""

from __future__ import annotations

from bollhav.model import ModelRun, execute_lifecycle
from bollhav.postgres import write
from mock_read import read


@execute_lifecycle
def run_interval(run: ModelRun, interval, data_conn, state_conn=None) -> None:
    df_gen = read(run, interval)
    write(
        conn=data_conn,
        run=run,
        df_gen=df_gen,
        since=interval.since,
        until=interval.until,
    )
