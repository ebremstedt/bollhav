"""Entry point — discovery + the per-model loop.

    @load_models  →  main(models)              # match models by TAGS
        for model in ordered(models):
            run_model(model, conn)             # @model_lifecycle  (run_model.py)
                for unit in model.intervals:
                    run_interval(model, unit, conn)  # @execute_lifecycle (run_interval.py)

Run order matters: the view's SQL reads the orders table, and
`daily_summary`'s contracts require the other three to be applied first.
We sort producers → view → contract-bearing model so a single run
satisfies everything. (A scrambled order would just block `daily_summary`
on the first run and catch up on the next.)

**Connections must be autocommit** — the data write commits, then the
state row flips (non-atomic data → state).
"""

import logging
import os

import psycopg
from roskarl import env_var_dsn

from bollhav.model import Model, load_models
from run_model import run_model


def setup_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@load_models
def main(models: list[Model], debug: bool) -> None:
    setup_logging(debug=debug)

    ordered = sorted(models, key=lambda m: (bool(m.upstream), m.is_view))
    dsn = env_var_dsn("TARGET_DSN").connection_string
    with (
        psycopg.connect(dsn, autocommit=True) as data_conn,
        psycopg.connect(dsn, autocommit=True) as state_conn,
    ):
        for model in ordered:
            run_model(model, data_conn, state_conn)
    print()


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
