"""Entry point.

`@load_models` discovers models in `src/models/`. This main opens the
connection and threads it through the lifecycle hooks. `@model_lifecycle`
registers the view-model in the library (so the downstream
`high_value_sums` sees it as a satisfied upstream — no
`STATE_001: not registered` or `STATE_002: no applied row` block) and
bootstraps the state-tracked tables.

The loop iterates models in topological order (no upstream → first):
  1. `warehouse.orders`              — 3 daily intervals, state+staging
  2. `warehouse.v_high_value_orders` — single CREATE OR REPLACE VIEW
  3. `warehouse.high_value_sums`     — 3 daily intervals, state+staging

For the view, `model.intervals` is `(None,)` — the loop runs it once,
and `@interval_lifecycle` passes through (no state) to the CREATE.
"""

import logging
import os

import psycopg

from bollhav.model import Model, load_models, model_lifecycle
from execute import execute_interval


def setup_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@model_lifecycle
def execute_model(model: Model, data_conn, state_conn=None) -> None:
    intervals = model.intervals
    print(f"\n{model.target.full_name}  {len(intervals)} step(s) to process")
    for interval in intervals:
        label = (
            f"{interval.since.date()} → {interval.until.date()}"
            if interval
            else "(single shot — view)"
        )
        print(f"  {label}", flush=True)
        execute_interval(model, interval, data_conn, state_conn)
    print()


@load_models
def main(models: list[Model], debug: bool) -> None:
    setup_logging(debug=debug)

    for model in models:
        dsn = os.environ[model.target.dsn_env_var]
        with psycopg.connect(dsn, autocommit=True) as conn:
            execute_model(model, conn)


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
