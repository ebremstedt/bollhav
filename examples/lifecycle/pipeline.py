"""End-to-end shape of the new lifecycle decorators.

The user creates the connections in `main()`, threads them through
`execute_model` / `execute_interval` as parameters, and the
`@model_lifecycle` / `@interval_lifecycle` hooks bracket those functions
with all the DDL + state machinery.

    main()                              ← creates target_conn + state_conn
      └─ execute_model(model, ...)      ← @model_lifecycle: assets, prefill, lock
           └─ execute_interval(...)     ← @interval_lifecycle: staging, state row

`data_conn` is required; `state_conn` is optional and defaults to
`data_conn` (co-located state). Pass a separate `state_conn` for the
cross-DB case (e.g. data in MSSQL, state in Postgres).

Run:
    docker compose up -d
    export TARGET_DSN='postgresql://bollhav:pw@localhost:5433/bollhav'
    python pipeline.py            # friendly progress
    DEBUG=1 python pipeline.py    # + the framework's internal log lines
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import polars as pl
import psycopg

from bollhav.model import (
    Batch,
    Bounds,
    Database,
    IntervalChunks,
    Model,
    State,
    Target,
    TargetSchema,
    WriteMode,
)
from bollhav.model.lifecycle import interval_lifecycle, model_lifecycle
from bollhav.postgres import PostgresColumn, PostgresType, write


# ── the model ────────────────────────────────────────────────────────

orders = Model(
    target=Target(
        name="orders",
        schema=TargetSchema(name="lifecycle_demo"),  # isolated; won't collide
        database=Database.POSTGRES,
        write_mode=WriteMode.UPSERT_NO_DELETE,
        dsn_env_var="TARGET_DSN",
        columns=[
            PostgresColumn(
                name="id",
                data_type=PostgresType.BIGINT,
                nullable=False,
                primary_key=True,
                unique=True,
            ),
            PostgresColumn(
                name="total", data_type=PostgresType.NUMERIC, nullable=False
            ),
            PostgresColumn(
                name="day", data_type=PostgresType.TIMESTAMPTZ, nullable=False
            ),
        ],
    ),
    state=State(),  # opt-in; drop → no state writes
    batching=Batch(interval=IntervalChunks(expression="@daily"), size=5000),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
)


# ── the user's read step ─────────────────────────────────────────────

ROWS_PER_DAY = 3


def read(since: datetime, until: datetime):
    """A few rows per day (stand-in for a real source read)."""
    base = int(since.timestamp())
    yield pl.DataFrame(
        {
            "id": [base + i for i in range(ROWS_PER_DAY)],
            "total": [float(i) for i in range(ROWS_PER_DAY)],
            "day": [since for _ in range(ROWS_PER_DAY)],
        }
    )


# ── execute functions, wrapped by the lifecycle hooks ────────────────


@model_lifecycle
def execute_model(model: Model, data_conn, state_conn=None) -> None:
    # @model_lifecycle has (when stateful) ensured the state table,
    # prefilled, and filtered `model.intervals` to the actionable ones.
    intervals = model.intervals
    print(f"\n▶ {model.target.full_name}: {len(intervals)} interval(s) to process")
    if not intervals:
        print("  (nothing actionable — everything already applied)")
    for interval in intervals:
        execute_interval(model, interval, data_conn, state_conn)


@interval_lifecycle
def execute_interval(model: Model, interval, data_conn, state_conn=None) -> None:
    # @interval_lifecycle has gated on applied, taken the lock, and
    # marked running. We do the data work; the hook marks applied after.
    print(f"  ▸ {interval.since.date()} → {interval.until.date()} ", end="", flush=True)
    df_gen = read(interval.since, interval.until)
    write(
        conn=data_conn,
        model=model,
        df_gen=df_gen,
        since=interval.since,
        until=interval.until,
    )
    print("→ written, marked applied ✓")


# ── logging + entrypoint ─────────────────────────────────────────────


def _setup_logging() -> None:
    """INFO by default; `DEBUG=1` surfaces the framework's per-step log
    lines (schema_created, prefilled, marked running, Writing N rows,
    marked applied, …) so you can see exactly what the hooks do."""
    debug = bool(os.environ.get("DEBUG"))
    logging.basicConfig(
        level=logging.INFO,
        format="    %(levelname)-5s %(name)s: %(message)s",
    )
    logging.getLogger("bollhav").setLevel(logging.DEBUG if debug else logging.WARNING)


def _summary(conn) -> None:
    rows = conn.execute("SELECT count(*) FROM lifecycle_demo.orders").fetchone()[0]
    states = conn.execute(
        "SELECT status, count(*) FROM z_lifecycle_demo.orders_state "
        "GROUP BY status ORDER BY status"
    ).fetchall()
    breakdown = ", ".join(f"{n} {s}" for s, n in states) or "—"
    print(f"\n✓ {orders.target.full_name} holds {rows} rows   |   state: {breakdown}\n")


def main() -> None:
    _setup_logging()

    # BULLDOZER so every run re-processes all intervals (a repeatable demo).
    # Drop this / set STATE_MODE=discover to see resume instead — DISCOVER
    # preserves applied rows, so a 2nd run finds nothing actionable.
    os.environ.setdefault("STATE_MODE", "bulldozer")

    # autocommit: the non-atomic data→state model commits each step on
    # its own. `with conn.transaction()` inside staging still gives
    # per-interval atomicity. On a non-autocommit conn the work opens a
    # dangling transaction that rolls back on close — nothing persists.
    target_conn = psycopg.connect(os.environ["TARGET_DSN"], autocommit=True)
    state_conn = target_conn  # co-located; pass a 2nd conn for cross-DB

    # Reload mode → intervals span bounds.begin..end. (Normally @load_models
    # sets this from tags/pipe args; here we drive the loop ourselves.)
    orders.directives.reload = True

    print("══ lifecycle pipeline ═══════════════════════════════════════")
    print(
        f"   state: {'on (co-located)' if orders.state else 'off'}   "
        f"target: {orders.target.full_name}"
    )
    try:
        for model in [orders]:
            execute_model(model, target_conn, state_conn)
        _summary(target_conn)
    finally:
        target_conn.close()


if __name__ == "__main__":
    main()
