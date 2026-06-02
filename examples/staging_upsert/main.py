"""Entry point — drives the orders model through its pending intervals.

`@load_models` is discovery only: it matches models in `src/models/`
and hands them back. This main opens the connection and threads it
through `execute_model` / `execute_interval`; `@model_lifecycle` does
the state bootstrap (ensure tables, pre-fill, filter `model.intervals`
to the still-pending rows), and `@interval_lifecycle` runs each one.

Connections must be autocommit. Verification queries are in README.md.
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
    print(f"\n{model.target.full_name}  {len(intervals)} interval(s) to process")
    for interval in intervals:
        print(f"  {interval.since.date()} → {interval.until.date()}", flush=True)
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
