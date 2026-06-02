"""Entry point — drive the events model through its daily intervals.

Self-contained (no @load_models / TAGS needed):

  1. ensure the `bollhav` database exists,
  2. drop the target table so each run starts clean (no applied-gate —
     this model has no State(), so a rerun would otherwise re-APPEND),
  3. loop `model.intervals` — one (since, until) per day — and for each,
     stream the day's rows into the target in `batching.size` chunks.

Run:  python main.py
"""

from __future__ import annotations

import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src", "models"))

from events import events  # noqa: E402

from bollhav.mssql import ensure_schema  # noqa: E402

from db import connect, ensure_database  # noqa: E402
from execute import execute  # noqa: E402


def _reset_target(model) -> None:
    """Fresh schema + dropped table so the demo is rerunnable."""
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
    _reset_target(events)

    # Reload mode → intervals span bounds.begin .. bounds.end (the three
    # days). Without this the model defaults to backfill mode, which wants
    # an explicit --until rather than reading bounds.end.
    events.directives.reload = True

    intervals = events.compute_intervals()
    print(
        f"\n{events.target.full_name}  "
        f"{len(intervals)} interval(s), size={events.batching.size}"
    )
    for interval in intervals:
        print(f"  {interval.since.date()} → {interval.until.date()}", flush=True)
        execute(model=events, since=interval.since, until=interval.until)

    print(f"\n✓ {events.target.full_name} now holds {_count_rows(events)} rows\n")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
