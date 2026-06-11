"""One unit of work, wrapped by `@execute_lifecycle`.

This is where the **curfew gate** runs: before the state machine, the hook
checks `model.curfew.blocks(now)`. If the curfew is active, this body never
runs — no read, no write, no state transition — and the interval stays
`pending`. Otherwise the normal flow happens: gate on applied → lock → mark
running → run this body → mark applied.
"""

from __future__ import annotations

from bollhav.model import ModelRun, execute_lifecycle
from bollhav.postgres import write
from mock_read import read


@execute_lifecycle
def run_interval(run: ModelRun, interval, data_conn, state_conn=None) -> None:
    df_gen = read(run, interval)
    write(conn=data_conn, run=run, df_gen=df_gen)
