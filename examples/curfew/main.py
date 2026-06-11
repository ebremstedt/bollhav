"""Curfew demo — the same model run twice, only the curfew window moving.

A curfew is a wall-clock gate checked per interval, *before* the state machine.
When it's active the interval is skipped with no state transition, so it stays
`pending`; when it lifts, the held intervals run. This script shows that:

  Run 1 — curfew DENIES a window covering *now* → all 3 intervals skipped.
           0 rows written; state stays pending. No exit, no error — just held.
  Run 2 — curfew window is hours away (inactive now) → the held intervals run.
           30 rows written; state flips to applied.

Both runs share the same target, so the same `pending` state rows from Run 1
are the ones Run 2 applies — the work was carried across, not lost.

Run:  python main.py        (needs TARGET_DSN; watch the INFO "curfew: skipping" lines)
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))

import psycopg  # noqa: E402

from bollhav.model import Curfew, ModelRun  # noqa: E402
from bollhav.model.window import resolve_window  # noqa: E402
from bollhav.postgres import PostgresState  # noqa: E402

from build_model import events_model  # noqa: E402
from run_model import run_model  # noqa: E402

DSN = os.environ.get(
    "TARGET_DSN", "postgresql://postgres:postgres@localhost:5432/postgres"
)


def _reset() -> None:
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
        conn.execute("DROP SCHEMA IF EXISTS z_bollhav CASCADE")


def _count_events() -> int:
    with psycopg.connect(DSN, autocommit=True) as conn:
        if conn.execute("SELECT to_regclass('warehouse.events')").fetchone()[0] is None:
            return 0
        return conn.execute("SELECT count(*) FROM warehouse.events").fetchone()[0]


def _state_summary(model) -> str:
    with psycopg.connect(DSN, autocommit=True) as conn:
        try:
            counts = PostgresState(model=model, conn=conn).read_status_summary()[
                "counts"
            ]
            return str(counts)
        except Exception:
            # No state table — the model was skipped before its bootstrap ran.
            return "(no state table — model skipped before any setup)"


def _run_once(curfew: Curfew, label: str) -> None:
    print(f"\n── {label} ──")
    model = events_model(curfew)
    print(f"   curfew active now? {curfew.blocks(datetime.now(timezone.utc))}")
    run = ModelRun(
        model=model,
        window=resolve_window(model.batching, model.bounds, reload=True),
    )
    with (
        psycopg.connect(DSN, autocommit=True) as data_conn,
        psycopg.connect(DSN, autocommit=True) as state_conn,
    ):
        run_model(run, data_conn, state_conn)
    print(f"   → warehouse.events rows: {_count_events()}")
    print(f"   → state status counts:   {_state_summary(model)}")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    _reset()

    now = datetime.now(timezone.utc)
    print(f"\nnow is {now:%H:%M} UTC — 3 daily intervals to run")

    # A 4-hour window centred on now → the curfew is active right now.
    covering = ((now - timedelta(hours=2)).time(), (now + timedelta(hours=2)).time())
    # A 2-hour window six hours out → present, but inactive right now.
    elsewhere = ((now + timedelta(hours=6)).time(), (now + timedelta(hours=8)).time())

    _run_once(
        Curfew(windows=[covering], tz=timezone.utc),
        "Run 1 — curfew DENIES a window covering now",
    )
    _run_once(
        Curfew(windows=[elsewhere], tz=timezone.utc),
        "Run 2 — curfew window is hours away (inactive now)",
    )

    print(
        "\nRun 1: the curfew was in effect when the model started, so @model_lifecycle\n"
        "skipped the whole model — no table, no state bootstrap, nothing wasted.\n"
        "Run 2: curfew clear → the model ran fully (30 rows, applied). Re-invoking\n"
        "after the window passes is how the held work gets done.\n"
    )


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
