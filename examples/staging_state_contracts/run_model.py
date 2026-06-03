"""One model's run, wrapped by `@model_lifecycle`.

The hook does the per-model setup before this body runs: build the target
assets (the table, or — for a view — `CREATE OR REPLACE VIEW`), and (when
stateful) ensure the state tables, register in the library, prefill, and
narrow `model.intervals` down to the still-actionable units.

This body just loops those units and hands each to `run_interval`.
`model.intervals` yields time windows for a batched model, or a single
`None` for the view / monolith (their unit of work is the whole thing).
"""

from __future__ import annotations

from bollhav.model import Model, model_lifecycle
from run_interval import run_interval


@model_lifecycle
def run_model(model: Model, data_conn, state_conn=None) -> None:
    units = model.intervals
    print(f"\n{model.target.full_name}  ({model.kind})  {len(units)} unit(s)")
    for interval in units:
        run_interval(model, interval, data_conn, state_conn)
