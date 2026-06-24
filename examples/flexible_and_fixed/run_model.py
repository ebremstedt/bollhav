"""One model's run, wrapped by `@model_lifecycle`.

The hook does the per-model setup before this body runs: build the target
assets, and (when stateful) ensure the state tables, register in the library
(this is where `fixed_intervals` is recorded), prefill, and narrow
`run.intervals` to the still-actionable units. This body just loops them.
"""

from __future__ import annotations

from bollhav.model import ModelRun, model_lifecycle
from run_interval import run_interval


@model_lifecycle
def run_model(run: ModelRun, data_conn, state_conn=None) -> None:
    for interval in run.intervals:
        run_interval(run, interval, data_conn, state_conn)
