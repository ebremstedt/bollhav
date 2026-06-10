"""One unit of work, wrapped by `@execute_lifecycle`.

The hook brackets this: gate on applied → take the lock → check the
model's upstream **contracts** → mark running → run this body → mark
applied (or record failure). For staging models it also creates the
staging table, then merges it into the target and drops it around the
body — so this body just `read()`s and `write()`s.

`interval` is a time window for a batched model, or `None` for the
monolith. A view has nothing to write (its definition was created by
`@model_lifecycle`), so the body returns immediately and the hook just
flips its existence row to `applied`.
"""

from __future__ import annotations

from bollhav.model import ModelRun, execute_lifecycle
from bollhav.postgres import write
from mock_read import read


@execute_lifecycle
def run_interval(run: ModelRun, interval, data_conn, state_conn=None) -> None:
    if run.model.is_view:
        return
    df_gen = read(run, interval)
    write(conn=data_conn, run=run, df_gen=df_gen)
