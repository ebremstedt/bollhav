"""Entry point — the loop only sees intervals that still need to run.

`@load_models` is discovery only now: it reads env, applies overrides,
matches models, and hands them to this function. The connection is
opened *here* and threaded through `execute_model` / `execute_interval`;
the lifecycle hooks do the rest:

  @model_lifecycle (execute_model)
    1. Ensure the target assets (schema/table) on `data_conn`.
    2. For stateful models: ensure state tables + pre-fill
       (`STATE_MODE` controls respect/disrespect on conflict), then
       filter `model.intervals` to the still-actionable ones.
  @interval_lifecycle (execute_interval)
    Gate on applied, take the per-interval lock, mark running, run the
    write, mark applied.

Result: on a second run with everything already applied,
`model.intervals` is empty and the loop exits without doing any work.

**Connections must be autocommit** — the non-atomic data→state model
commits each step independently. Verification queries are in README.md.
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
    # @model_lifecycle has (when stateful) ensured the state table,
    # prefilled, and filtered `model.intervals` to the actionable ones.
    intervals = model.intervals
    print(f"\n{model.target.full_name}  {len(intervals)} interval(s) to process")
    for interval in intervals:
        print(f"  {interval.since.date()} → {interval.until.date()}", flush=True)
        execute_interval(model, interval, data_conn, state_conn)
    print()


@load_models
def main(models: list[Model], debug: bool) -> None:
    setup_logging(debug=debug)

    for model in models:
        # One autocommit connection per model (state co-locates with the
        # target DB here, so data_conn doubles as state_conn).
        dsn = os.environ[model.target.dsn_env_var]
        with psycopg.connect(dsn, autocommit=True) as conn:
            execute_model(model, conn)


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
