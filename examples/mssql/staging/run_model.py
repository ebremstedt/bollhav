"""One model's run, wrapped by `@model_lifecycle`.

The hook does the per-model setup before this body runs: resolve the data
backend from `model.target.database` (here `MssqlData`), build the target
assets (schema + table + PK), create the staging schema + GC orphan staging
tables, and — because this model is state-tracked — run the state bootstrap
on `state_conn` (Postgres) and narrow `run.intervals` to the actionable set.

This body just loops those intervals and hands each to `run_interval`,
threading both connections through.
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
