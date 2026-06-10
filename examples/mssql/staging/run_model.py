"""One model's run, wrapped by `@model_lifecycle`.

The hook does the per-model setup before this body runs: resolve the
data backend from `model.target.database` (here `MssqlData`), build the
target assets (schema + table + PK), and — because this model stages —
create the staging schema and GC any orphan staging tables left by a
crashed run. MSSQL is never stateful, so there's no state bootstrap.

This body just loops the units and hands each to `run_interval`.
`model.intervals` holds the daily windows (computed in `main` for this
stateless model).
"""

from __future__ import annotations

from bollhav.model import ModelRun, model_lifecycle
from run_interval import run_interval


@model_lifecycle
def run_model(run: ModelRun, data_conn, state_conn=None) -> None:
    units = run.intervals
    print(f"\n{run.model.target.full_name}  ({run.model.kind})  {len(units)} unit(s)")
    for interval in units:
        print(f"  {interval.since.date()} → {interval.until.date()}", flush=True)
        run_interval(run, interval, data_conn, state_conn)
