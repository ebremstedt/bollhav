"""Entry point — discovery + the per-model loop.

    @load_models  →  main(runs)                # match models by TAGS
        for run in runs:                       # already topologically sorted
            run_model(run, data_conn, state_conn)   # @model_lifecycle (run_model.py)
                for unit in run.intervals:
                    run_interval(run, unit, ...)     # @execute_lifecycle (run_interval.py)

`@load_models` hands the models back **topologically sorted** (producers →
consumers), so we just iterate — every declared upstream is applied before its
downstream gates on it.

**Connections must be autocommit** — the data write commits, then the state
row flips (non-atomic data → state).
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
