"""A tiny runnable pipeline for trying DRY_STATE.

Two state-tracked interval models over 2024-01-01 .. 2024-01-04 (@daily → 3
intervals each):

    orders                       — no upstream
    summary                      — gated on orders (IntervalContract)

Needs a Postgres at $TARGET_DSN (default localhost:5432/postgres).

Usage:

    # see the plan against current state — no assets, no data, no execution:
    DRY_STATE=true python examples/dry_state_demo.py

    # …or list every interval individually (window + would-run / blocked):
    DRY_STATE_EXTRA=true python examples/dry_state_demo.py

    # actually run (bootstrap + execute + mark applied) — optionally a subset:
    python examples/dry_state_demo.py              # runs orders then summary
    python examples/dry_state_demo.py orders       # runs just orders
    python examples/dry_state_demo.py reset        # drop this demo's schemas + state

Walk the scenarios:

    python examples/dry_state_demo.py reset
    DRY_STATE=true python examples/dry_state_demo.py     # A: orders would-run, summary blocked
    python examples/dry_state_demo.py orders            # apply orders only
    DRY_STATE=true python examples/dry_state_demo.py     # B: orders applied, summary would-run
    python examples/dry_state_demo.py                   # apply the rest
    DRY_STATE=true python examples/dry_state_demo.py     # C: everything applied, nothing to run
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

import polars as pl
import psycopg

from bollhav.model import (
    Batch,
    Bounds,
    Database,
    IntervalChunks,
    IntervalContract,
    Kind,
    Model,
    Source,
    SourceModel,
    State,
    Target,
)
from bollhav.model.lifecycle import execute_lifecycle, model_lifecycle
from bollhav.postgres import PostgresColumn, PostgresType, write

DSN = os.environ.setdefault(
    "TARGET_DSN", "postgresql://postgres:postgres@localhost:5432/postgres"
)
CAT = "demo"
SCHEMA = "dry_state_demo"
BEGIN = datetime(2024, 1, 1, tzinfo=timezone.utc)
END = datetime(2024, 1, 4, tzinfo=timezone.utc)


def _cols():
    return [PostgresColumn(name="id", data_type=PostgresType.BIGINT, nullable=False)]


def _model(name: str, *, upstream=None) -> Model:
    m = Model(
        target=Target(
            name=name,
            schema=SCHEMA,
            catalog=CAT,
            database=Database.POSTGRES,
            dsn_env_var="TARGET_DSN",
            schema_suffix_appendix=None,
            columns=_cols(),
        ),
        kind=Kind.INTERVAL,
        state=State(),
        batching=Batch(interval=IntervalChunks(expression="@daily")),
        bounds=Bounds(begin=BEGIN, end=END),
        upstream=upstream or [],
    )
    # @load_models normally sets these from BACKFILL_* env vars; pin them here.
    m.directives.since = BEGIN
    m.directives.until = END
    return m


orders = _model("orders")
summary = _model(
    "summary",
    upstream=[
        Source(
            f"{CAT}.{SCHEMA}.orders", type=SourceModel(), contract=IntervalContract()
        )
    ],
)
MODELS = {"orders": orders, "summary": summary}


@execute_lifecycle
def run_interval(model, interval, data_conn, state_conn=None):
    # Trivial "work": write one row so the interval has data and applies.
    write(conn=data_conn, model=model, df_gen=iter([pl.DataFrame({"id": [1]})]))


@model_lifecycle
def run_model(model, data_conn, state_conn=None):
    # Under DRY_STATE this body never runs — bollhav prints the plan and skips it.
    for interval in model.intervals:
        run_interval(model, interval, data_conn, state_conn)


def _reset():
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
        # drop this demo's state tables + library rows from the bollhav schema
        from bollhav.postgres.state import LIBRARY_TABLE, ERRORS_TABLE

        tables = c.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'z_bollhav'"
        ).fetchall()
        for (t,) in tables:
            if t in (LIBRARY_TABLE, ERRORS_TABLE):
                continue
            owner = c.execute(
                f'SELECT model_name FROM z_bollhav."{t}" LIMIT 1'
            ).fetchone()
            if owner and owner[0] and SCHEMA in str(owner[0]).split("."):
                c.execute(f'DROP TABLE IF EXISTS z_bollhav."{t}"')
        c.execute(
            "DELETE FROM z_bollhav.library WHERE full_name LIKE %s", [f"%{SCHEMA}.%"]
        )
    print(f"reset: dropped {SCHEMA!r} + its state")


def main(argv: list[str]) -> None:
    if argv and argv[0] == "reset":
        _reset()
        return

    which = [a for a in argv if a in MODELS] or ["orders", "summary"]
    dry = os.environ.get("DRY_STATE") or os.environ.get("DRY_STATE_EXTRA")
    mode = "DRY_STATE plan" if dry else "running"
    print(f"=== {mode} ({', '.join(which)}) ===")
    with psycopg.connect(DSN, autocommit=True) as conn:
        for name in which:
            run_model(MODELS[name], conn, conn)


if __name__ == "__main__":
    main(sys.argv[1:])
