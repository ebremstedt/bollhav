"""One model's run, wrapped by `@model_lifecycle`.

The hook builds the target table and (since the model is stateful) runs the
state bootstrap, then narrows `run.intervals` to the still-actionable set. This
body loops those and hands each to `run_interval`. The curfew gate lives one
level down, in `@execute_lifecycle` — so it's evaluated per interval.
"""

from __future__ import annotations

from bollhav.model import ModelRun, model_lifecycle
from run_interval import run_interval


@model_lifecycle
def run_model(run: ModelRun, data_conn, state_conn=None) -> None:
    for interval in run.intervals:
        run_interval(run, interval, data_conn, state_conn)
