"""Entry point — discovery + the per-model loop.

    @load_models  →  main(models)              # match models by TAGS
        for model in models:                   # already topologically sorted
            run_model(model, conn)             # @model_lifecycle  (run_model.py)
                for unit in model.intervals:
                    run_interval(model, unit, conn)  # @execute_lifecycle (run_interval.py)

Run order matters: the view's SQL reads the orders table, and
`daily_summary`'s contracts require the other three to be applied first.
`@load_models` hands the models back **topologically sorted** (producers →
view → contract-bearing model), so we just iterate them — no hand-sort.
Ordering follows every declared producer→consumer edge (including the
view's ungated `SELECT … FROM orders` source); runtime gating is separate
and contract-only

**Connections must be autocommit** — the data write commits, then the
state row flips (non-atomic data → state).
"""

import logging
import os

import psycopg
from roskarl import env_var_dsn

from bollhav.model import ModelRun, load_models
from run_model import run_model


def setup_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@load_models
def main(runs: list[ModelRun], debug: bool) -> None:
    setup_logging(debug=debug)

    dsn = env_var_dsn("TARGET_DSN").connection_string
    with (
        psycopg.connect(dsn, autocommit=True) as data_conn,
        psycopg.connect(dsn, autocommit=True) as state_conn,
    ):
        for run in runs:
            run_model(run, data_conn, state_conn)
    print()


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
