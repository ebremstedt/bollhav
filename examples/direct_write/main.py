"""Entry point — discovery + the per-model loop (direct write, no staging).

    @load_models  →  main(runs)                # match models by TAGS
        for run in runs:
            run_model(run, data_conn, state_conn)   # @model_lifecycle
                for interval in run.intervals:
                    run_interval(run, interval, ...)  # @execute_lifecycle

Two models, both written DIRECTLY to the target (no staging):
  * events  — APPEND
  * metrics — RECREATE_PARTITION (overwrite each interval's window)

**Connections must be autocommit** — each interval's write commits, then its
state row flips; the framework brackets every unit in its own transaction.
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
