"""Entry point — stage the `sales` model through its daily intervals.

Self-contained (no @load_models / TAGS needed):

  1. ensure the `bollhav` database exists,
  2. drop the target table so each run starts clean to count rows
     (the MERGE makes reruns idempotent anyway — this just keeps the
     printed count obvious),
  3. compute the daily intervals (reload mode → bounds.begin..bounds.end),
  4. run the model through `@model_lifecycle` / `@execute_lifecycle`,
     which own the asset DDL and the per-interval staging lifecycle.

One connection drives the whole run. `autocommit=False` so the
per-interval apply (MERGE staging → target + DROP staging) commits as
one atomic transaction.

Run:  python main.py
"""

from __future__ import annotations

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src", "models"))

from sales import sales  # noqa: E402

from bollhav.mssql import ensure_schema  # noqa: E402

from db import connect, ensure_database  # noqa: E402
from run_model import run_model  # noqa: E402


def _reset_target(model) -> None:
    """Fresh schema + dropped target so the printed row count is clean."""
    schema = model.target.schema.resolved
    table = model.target.name
    with connect(autocommit=True) as conn:
        ensure_schema(conn, schema)
        conn.cursor().execute(f"DROP TABLE IF EXISTS [{schema}].[{table}]")


def _count_rows(model) -> int:
    schema = model.target.schema.resolved
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

    # Reload mode → intervals span bounds.begin..bounds.end (the three
    # days). A stateless model has no state bootstrap to fill them in, so
    # compute them here and hand them to the lifecycle.
    sales.directives.reload = True
    sales.intervals = sales.compute_intervals()

    with connect() as conn:
        run_model(sales, conn)

    print(f"\n✓ {sales.target.full_name} now holds {_count_rows(sales)} rows\n")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
