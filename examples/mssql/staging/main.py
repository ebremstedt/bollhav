"""Entry point — stage the `sales` model (MSSQL data) with state in Postgres.

Self-contained (no @load_models / TAGS needed):

  1. ensure the MSSQL `bollhav` database exists,
  2. reset: drop the MSSQL target table AND the Postgres state (`z_bollhav`),
     so the run starts clean and every interval is pending,
  3. resolve the daily window (reload mode → bounds.begin..bounds.end),
  4. run through `@model_lifecycle` / `@execute_lifecycle`, passing **two**
     connections — MSSQL `data_conn` (pyodbc) for data + staging, and
     Postgres `state_conn` (psycopg) for the state machine.

The MSSQL connection is `autocommit=False` (the staging apply commits its
DELETE/INSERT/MERGE + DROP as one transaction); the Postgres state
connection is `autocommit=True` (each state transition commits on its own).

Run:  python main.py   (needs BOLLHAV_MSSQL_DSN + STATE_DSN — see README)
"""

from __future__ import annotations

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src", "models"))

from sales import sales  # noqa: E402

from bollhav.mssql import ensure_schema  # noqa: E402
from bollhav.model import ModelRun  # noqa: E402
from bollhav.model.window import resolve_window  # noqa: E402

from db import connect, ensure_database, state_connect  # noqa: E402
from run_model import run_model  # noqa: E402


def _reset_target(model) -> None:
    """Fresh schema + dropped MSSQL target so the printed row count is clean."""
    schema = model.target.schema_resolved
    table = model.target.name
    with connect(autocommit=True) as conn:
        ensure_schema(conn, schema)
        conn.cursor().execute(f"DROP TABLE IF EXISTS [{schema}].[{table}]")


def _reset_state() -> None:
    """Drop the Postgres state/library schema so every interval is pending
    again (otherwise the applied gate skips a re-run)."""
    with state_connect() as conn:
        conn.execute("DROP SCHEMA IF EXISTS z_bollhav CASCADE")


def _count_rows(model) -> int:
    schema = model.target.schema_resolved
    table = model.target.name
    with connect() as conn:
        row = (
            conn.cursor()
            .execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            .fetchone()
        )
        return row[0]


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    ensure_database("bollhav")
    _reset_target(sales)
    _reset_state()

    # Reload mode → window spans bounds.begin..bounds.end. The @model_lifecycle
    # state bootstrap (on state_conn) splits this window into pending interval
    # rows in Postgres and narrows run.intervals to the actionable set.
    run = ModelRun(
        model=sales,
        window=resolve_window(sales.batching, sales.bounds, reload=True),
    )

    # Two connections: MSSQL for data/staging, Postgres for state.
    with connect() as data_conn, state_connect() as state_conn:
        run_model(run, data_conn, state_conn)

    print(f"\n✓ {sales.target.full_name} now holds {_count_rows(sales)} rows\n")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
