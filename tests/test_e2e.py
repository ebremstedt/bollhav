"""End-to-end tests against a real Postgres.

These tests cover the features introduced on the `state2` branch:
state lifecycle, mutations one-shot setup, staging (fresh table per
interval, dropped on flush) — with and without state, library registration
(state-tracked, VIEW with library=True, library-only TABLE),
satisfaction-by-presence vs satisfaction-by-applied-row, auto orphan
staging GC at bootstrap, and additive library schema migration.

Each test uses a unique `warehouse_e2e_<n>` schema so multiple tests
can run in parallel safely. The state schema (`z_warehouse_e2e_<n>`)
and the library schema (`z_bollhav`) are cleaned before each test.

Skip when `E2E_DSN` is unset and the local default isn't reachable.
"""

from __future__ import annotations

import logging
import os
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from typing import Generator

import polars as pl
import psycopg
import pytest

from bollhav.model import (
    Batch,
    Bounds,
    Database,
    IntervalChunks,
    Model,
    ModelType,
    SourceTable,
    Staging,
    State,
    Target,
    TargetSchema,
    WriteMode,
)
from bollhav.model.state import StateMode
from bollhav.postgres import (
    PostgresColumn,
    PostgresType,
    create_replace_view,
    write,
)


DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/postgres"
SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
UNTIL = datetime(2024, 1, 4, tzinfo=timezone.utc)


# ── infra ────────────────────────────────────────────────────────────


def _dsn() -> str:
    return os.environ.get("E2E_DSN", DEFAULT_DSN)


def _can_connect() -> bool:
    try:
        with psycopg.connect(_dsn(), connect_timeout=2):
            return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _can_connect(),
    reason="Postgres unavailable (set E2E_DSN; default postgres:postgres@localhost:5432/postgres)",
)


@pytest.fixture(autouse=True)
def _set_dsn_env():
    os.environ["TARGET_DSN"] = _dsn()
    yield


@pytest.fixture
def schema_name(request):
    """A unique target schema per test, dropped before and after."""
    name = "warehouse_e2e_" + uuid.uuid4().hex[:8]
    _drop_schemas(name)
    yield name
    _drop_schemas(name)


def _drop_schemas(target_schema: str) -> None:
    with psycopg.connect(_dsn(), autocommit=True) as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {target_schema} CASCADE")
        conn.execute(f"DROP SCHEMA IF EXISTS z_{target_schema} CASCADE")


def _drop_library() -> None:
    with psycopg.connect(_dsn(), autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS z_bollhav CASCADE")


@contextmanager
def _conn():
    with psycopg.connect(_dsn()) as c:
        yield c


# ── model factory ────────────────────────────────────────────────────


def _orders_columns():
    return [
        PostgresColumn(name="id", data_type=PostgresType.BIGINT, nullable=False),
        PostgresColumn(
            name="customer_id", data_type=PostgresType.BIGINT, nullable=False
        ),
        PostgresColumn(name="total", data_type=PostgresType.NUMERIC, nullable=False),
        PostgresColumn(
            name="order_date", data_type=PostgresType.TIMESTAMPTZ, nullable=False
        ),
    ]


def _orders_model(
    schema_name: str,
    *,
    name: str = "orders",
    state: State | None = None,
    staging: Staging | None = None,
    bounds_end: datetime = UNTIL,
    upstream: list[str] | None = None,
    library: bool = False,
) -> Model:
    m = Model(
        target=Target(
            name=name,
            schema=TargetSchema(name=schema_name),
            database=Database.POSTGRES,
            write_mode=WriteMode.APPEND,
            dsn_env_var="TARGET_DSN",
            staging=staging,
            columns=_orders_columns(),
        ),
        state=state,
        library=library,
        batching=Batch(interval=IntervalChunks(expression="@daily")),
        bounds=Bounds(begin=SINCE, end=bounds_end),
        upstream=upstream or [],
    )
    # `@load_models` normally sets backfill directives from env vars
    # before invoking the bootstrap. E2E tests call the bootstrap
    # directly, so we pin them by hand to the test's fixed window —
    # otherwise the model defaults to backfilling all the way to
    # "now" (today), generating ~hundreds of intervals.
    m.directives.since = SINCE
    m.directives.until = bounds_end
    return m


def _gen_rows(
    since: datetime, until: datetime, n: int = 5
) -> Generator[pl.DataFrame, None, None]:
    """Yield a single small DataFrame for the interval."""
    base = int(since.timestamp())
    yield pl.DataFrame(
        [
            {
                "id": base + i,
                "customer_id": 1,
                "total": 1.0 * (i + 1),
                "order_date": since + timedelta(seconds=i),
            }
            for i in range(n)
        ],
        schema={
            "id": pl.Int64,
            "customer_id": pl.Int64,
            "total": pl.Float64,
            "order_date": pl.Datetime("us", "UTC"),
        },
    )


def _run_intervals(model: Model, *, error_on_interval: int | None = None):
    """Drive a model's intervals through the staged write path, the
    way a user's loop would after the @load_models bootstrap.

    Bootstrap is invoked directly via `_bootstrap_state_for_staged_models`
    so we don't need env-var plumbing for `@load_models`."""
    from bollhav.model.load_models import _bootstrap_state_for_staged_models

    # Mint the contract before bootstrap so we know the full interval set.
    model._intervals_cached = None  # ensure recompute
    contract = list(model.intervals)
    _bootstrap_state_for_staged_models([model], state_mode=StateMode.DISCOVER)

    # `model.intervals` is the bootstrap's filtered set (only
    # non-applied rows when state is set). An empty list is
    # meaningful — "everything's already done" — so don't fall back
    # to the contract here; treat empty as no work.
    if model.state is not None:
        intervals = list(model.intervals)
    else:
        intervals = contract
    for idx, interval in enumerate(intervals):
        since = interval.since if interval else None
        until = interval.until if interval else None
        if error_on_interval == idx:
            raise RuntimeError("simulated mid-stream crash")
        with psycopg.connect(_dsn()) as conn:
            if model.target.is_view:
                create_replace_view(conn=conn, model=model)
            else:
                df_gen = _gen_rows(since, until)
                write(conn=conn, model=model, df_gen=df_gen, since=since, until=until)


# ── helpers / queries ────────────────────────────────────────────────


def _list_tables(schema: str) -> list[str]:
    with _conn() as c:
        rows = c.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = %s ORDER BY tablename",
            [schema],
        ).fetchall()
    return [r[0] for r in rows]


def _row_count(schema: str, table: str) -> int:
    with _conn() as c:
        row = c.execute(f'SELECT count(*) FROM "{schema}"."{table}"').fetchone()
    return row[0]


def _state_rows(state_schema: str, state_table: str) -> list[tuple]:
    with _conn() as c:
        rows = c.execute(
            f'SELECT status, since, until FROM "{state_schema}"."{state_table}" '
            "ORDER BY since"
        ).fetchall()
    return rows


def _library_rows() -> list[tuple]:
    with _conn() as c:
        rows = c.execute(
            "SELECT full_name, model_type, state_schema, state_table "
            "FROM z_bollhav.model_library "
            "ORDER BY full_name"
        ).fetchall()
    return rows


# ── 1. state lifecycle: pending → applied ────────────────────────────


def test_e2e_state_lifecycle(schema_name):
    """3 daily intervals: bootstrap prefills pending; loop drives
    them through to applied via the staged flush."""
    m = _orders_model(schema_name, state=State(), staging=Staging())
    _run_intervals(m)

    state_schema = f"z_{schema_name}"
    rows = _state_rows(state_schema, "orders_state")
    assert len(rows) == 3
    assert all(status == "applied" for status, _, _ in rows)
    assert _row_count(schema_name, "orders") == 15  # 3 intervals × 5 rows


# ── 2. mutations one-shot: setup runs once per pipeline run ──────────


def test_e2e_actions_one_shot_setup(schema_name, caplog):
    """Across 3 intervals: target table is CREATEd once; subsequent
    intervals short-circuit at `target.setup_complete`. The action
    debug logs should appear exactly once per applicable PRE action."""
    caplog.set_level(logging.DEBUG, logger="bollhav.postgres.actions")
    m = _orders_model(schema_name, state=State(), staging=Staging())
    _run_intervals(m)

    flags = [r.message for r in caplog.records if "action:" in r.message]
    # Exactly one log per applicable model-level PRE action across the run.
    assert sum(f.endswith(".table_created done") for f in flags) == 1
    assert sum(f.endswith(".schema_created done") for f in flags) == 1
    assert sum(f.endswith(".staging_schema_created done") for f in flags) == 1
    # The staging table is created per-interval inside stage(), not via a
    # model-level PRE action — so there is no `staging_table_created` flag.


# ── 3. staging: fresh table per interval, dropped on flush ───────────


def test_e2e_staging_drops_each_interval(schema_name):
    """Staging creates a fresh table per interval and drops it inside
    each flush. After 3 intervals: target has 15 rows and NO staging
    tables remain."""
    m = _orders_model(schema_name, state=State(), staging=Staging())
    _run_intervals(m)

    assert _row_count(schema_name, "orders") == 15

    state_schema = f"z_{schema_name}"
    staging_tables = [
        t for t in _list_tables(state_schema) if t.startswith("orders_staging_")
    ]
    assert staging_tables == []


# ── 4. staging without state — same self-cleaning behavior ───────────


def test_e2e_staging_without_state(schema_name):
    """Staging works without state: rows land in target via atomic
    flush, no state row exists, staging self-cleans. Re-run reloads
    every interval (no `applied` gate)."""
    m = _orders_model(schema_name, state=None, staging=Staging())
    _run_intervals(m)

    assert _row_count(schema_name, "orders") == 15
    state_schema = f"z_{schema_name}"
    tables = _list_tables(state_schema)
    assert "orders_state" not in tables
    staging_tables = [t for t in tables if t.startswith("orders_staging_")]
    assert staging_tables == []


# ── 7. library: state-tracked TABLE registers ────────────────────────


def test_e2e_library_state_tracked_table_registers(schema_name):
    m = _orders_model(schema_name, state=State(), staging=Staging())
    _run_intervals(m)

    rows = _library_rows()
    fq = f"{schema_name}.orders"
    matches = [r for r in rows if r[0] == fq]
    assert len(matches) == 1
    _, model_type, st_sch, st_tbl = matches[0]
    assert model_type == "TABLE"
    assert st_sch == f"z_{schema_name}"
    assert st_tbl == "orders_state"


# ── 8. library-only TABLE: `library=True`, no state ──────────────────


def test_e2e_library_only_table_registers_with_null_state(schema_name):
    m = Model(
        target=Target(
            name="countries",
            schema=TargetSchema(name=schema_name),
            dsn_env_var="TARGET_DSN",
        ),
        library=True,
    )
    from bollhav.model.load_models import _bootstrap_state_for_staged_models

    _bootstrap_state_for_staged_models([m], state_mode=StateMode.DISCOVER)

    rows = _library_rows()
    fq = f"{schema_name}.countries"
    matches = [r for r in rows if r[0] == fq]
    assert len(matches) == 1
    _, model_type, st_sch, st_tbl = matches[0]
    assert model_type == "TABLE"
    assert st_sch is None
    assert st_tbl is None


# ── 9. view-as-upstream end-to-end ───────────────────────────────────


def test_e2e_view_as_upstream_satisfies_downstream(schema_name):
    """orders (state+staging) → v_high_value (VIEW, library=True) →
    enriched (state+staging, upstream=[v_high_value]). Verify
    enriched intervals all come up as `applied` after a full run."""
    from bollhav.model.load_models import _bootstrap_state_for_staged_models

    orders = _orders_model(schema_name, state=State(), staging=Staging())

    view = Model(
        source=SourceTable(
            name="v_high_value",
            query=f"SELECT * FROM {schema_name}.orders WHERE total >= 0",
        ),
        target=Target(
            name="v_high_value",
            schema=TargetSchema(name=schema_name),
            model_type=ModelType.VIEW,
            write_mode=WriteMode.VIEW,
            dsn_env_var="TARGET_DSN",
        ),
        library=True,
    )

    enriched = _orders_model(
        schema_name,
        name="enriched",
        state=State(),
        staging=Staging(),
        upstream=[f"{schema_name}.v_high_value"],
    )

    # Bootstrap all three at once so the library has the view row
    # before enriched's upstream check.
    _bootstrap_state_for_staged_models(
        [orders, view, enriched], state_mode=StateMode.DISCOVER
    )

    # Run in topo order.
    _run_intervals(orders)
    _run_intervals(view)
    _run_intervals(enriched)

    state_schema = f"z_{schema_name}"
    enriched_rows = _state_rows(state_schema, "enriched_state")
    assert len(enriched_rows) == 3
    assert all(status == "applied" for status, _, _ in enriched_rows)

    lib = {r[0]: r for r in _library_rows()}
    assert lib[f"{schema_name}.v_high_value"][1] == "VIEW"
    assert lib[f"{schema_name}.v_high_value"][2] is None  # state_schema NULL


# ── 10. view WITHOUT library=True does not satisfy downstream ────────


def test_e2e_view_without_library_true_blocks_downstream(schema_name):
    """A VIEW declared without `library=True` doesn't register —
    downstream intervals come up `blocked` with STATE_001."""
    from bollhav.model.load_models import _bootstrap_state_for_staged_models

    view = Model(  # noqa: F841 — constructed but not in matched set on purpose; see test docstring
        source=SourceTable(
            name="v_orphan",
            query="SELECT 1 AS x",
        ),
        target=Target(
            name="v_orphan",
            schema=TargetSchema(name=schema_name),
            model_type=ModelType.VIEW,
            write_mode=WriteMode.VIEW,
            dsn_env_var="TARGET_DSN",
        ),
        # library=False (default)
    )

    enriched = _orders_model(
        schema_name,
        name="enriched",
        state=State(),
        staging=Staging(),
        upstream=[f"{schema_name}.v_orphan"],
    )

    # Bootstrap only enriched — putting the (unregistered) view in
    # the matched set would make the bootstrap skip the upstream
    # check (in-pipeline upstreams are trusted to come up via topo
    # order). The whole point of this test is that the *missing*
    # library entry should cause STATE_001.
    _bootstrap_state_for_staged_models([enriched], state_mode=StateMode.DISCOVER)

    state_schema = f"z_{schema_name}"
    # Enriched should have 3 blocked rows.
    with _conn() as c:
        rows = c.execute(
            f'SELECT status, blocked_reason FROM "{state_schema}"."enriched_state" '
            "ORDER BY since"
        ).fetchall()
    assert len(rows) == 3
    assert all(status == "blocked" for status, _ in rows)
    assert all(reason.startswith("STATE_001:") for _, reason in rows)
    assert all("v_orphan" in reason for _, reason in rows)


# ── 11. orphan staging GC at bootstrap ───────────────────────────────


def test_e2e_orphan_staging_gc_at_bootstrap(schema_name):
    """Seed an orphan staging table from a prior fake run, then
    bootstrap a new run on the same model. The orphan should be
    dropped by the auto-GC at bootstrap."""
    from bollhav.model.load_models import _bootstrap_state_for_staged_models

    state_schema = f"z_{schema_name}"
    # Make the schema and a fake orphan staging table.
    with psycopg.connect(_dsn(), autocommit=True) as c:
        c.execute(f"CREATE SCHEMA IF NOT EXISTS {state_schema}")
        c.execute(
            f'CREATE UNLOGGED TABLE "{state_schema}"."orders_staging_dead0000" '
            "(id BIGINT)"
        )

    before = [t for t in _list_tables(state_schema) if "staging_" in t]
    assert "orders_staging_dead0000" in before

    m = _orders_model(schema_name, state=State(), staging=Staging())
    _bootstrap_state_for_staged_models([m], state_mode=StateMode.DISCOVER)

    after = [t for t in _list_tables(state_schema) if "staging_" in t]
    assert "orders_staging_dead0000" not in after


# ── 12. additive library migration ───────────────────────────────────


def test_e2e_library_migration_is_additive(schema_name):
    """Seed an old-shape `model_library` (no model_type column,
    state_schema/table NOT NULL). Bootstrap should ALTER it in place
    — no DROP — and the existing row should survive."""
    from bollhav.postgres.library import ensure_library

    _drop_library()
    with psycopg.connect(_dsn(), autocommit=True) as c:
        c.execute("CREATE SCHEMA z_bollhav")
        c.execute(
            "CREATE TABLE z_bollhav.model_library ("
            "  full_name TEXT PRIMARY KEY,"
            "  upstream TEXT[] NOT NULL,"
            "  state_schema TEXT NOT NULL,"
            "  state_table TEXT NOT NULL,"
            "  last_seen TIMESTAMPTZ NOT NULL DEFAULT now()"
            ")"
        )
        c.execute(
            "INSERT INTO z_bollhav.model_library "
            "(full_name, upstream, state_schema, state_table) "
            "VALUES ('legacy.model', '{}', 'z_legacy', 'model_state')"
        )

    with psycopg.connect(_dsn()) as c:
        ensure_library(c)
        c.commit()

    # New column present
    with _conn() as c:
        cols = [
            r[0]
            for r in c.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='z_bollhav' AND table_name='model_library'"
            ).fetchall()
        ]
    assert "model_type" in cols
    # state_schema and state_table now nullable
    with _conn() as c:
        nullable = dict(
            c.execute(
                "SELECT column_name, is_nullable FROM information_schema.columns "
                "WHERE table_schema='z_bollhav' AND table_name='model_library' "
                "  AND column_name IN ('state_schema','state_table')"
            ).fetchall()
        )
    assert nullable["state_schema"] == "YES"
    assert nullable["state_table"] == "YES"
    # Legacy row survived, model_type defaulted to 'TABLE'
    rows = _library_rows()
    assert ("legacy.model", "TABLE", "z_legacy", "model_state") in rows


# ── 13. state re-run is a no-op once everything is applied ───────────


def test_e2e_state_rerun_is_noop_for_applied_intervals(schema_name):
    m = _orders_model(schema_name, state=State(), staging=Staging())
    _run_intervals(m)
    assert _row_count(schema_name, "orders") == 15

    # Second run on a fresh Model instance. apply_runtime_overrides
    # would normally do this; we mimic by building a new Model.
    m2 = _orders_model(schema_name, state=State(), staging=Staging())
    _run_intervals(m2)
    # No new rows landed — every interval was already applied.
    assert _row_count(schema_name, "orders") == 15


# ── 14. write_mode combinations: big source → interval-paced apply ──
#
# The pattern these tests follow:
#   1. Plant a "source" table in Postgres with a known number of rows
#      across the 3-interval window (and known dup patterns for upsert).
#   2. Drive `stage()` interval by interval — each interval reads its
#      own window from source via SELECT and pipes the rows into the
#      target via the staged path.
#   3. Verify final target state and (via caplog) confirm the right
#      SQL pattern fired at each step.
#
# Run with `-s` or capture DEBUG via caplog to inspect the SQL.

DEBUG_LOGGERS = (
    "bollhav.postgres.staging",
    "bollhav.postgres.actions",
    "bollhav.model.state",
)


def _enable_debug_logging(caplog):
    """Turn on DEBUG capture for the bollhav loggers that emit
    interesting SQL/action-level events. Tests can then assert against
    log messages and the user can read the captured output on failure."""
    for name in DEBUG_LOGGERS:
        caplog.set_level(logging.DEBUG, logger=name)


def _plant_source_table(schema: str, table: str, rows: list[dict]) -> None:
    """Materialize a source table from a row list. Used by the
    interval-paced apply tests as the input to read from."""
    with psycopg.connect(_dsn()) as conn:
        conn.execute(
            f'CREATE SCHEMA IF NOT EXISTS "{schema}"; '
            f'DROP TABLE IF EXISTS "{schema}"."{table}"; '
            f'CREATE TABLE "{schema}"."{table}" ('
            f"  id BIGINT NOT NULL,"
            f"  customer_id BIGINT NOT NULL,"
            f"  total NUMERIC NOT NULL,"
            f"  order_date TIMESTAMPTZ NOT NULL,"
            f"  status TEXT"
            f")"
        )
        with conn.cursor().copy(
            f'COPY "{schema}"."{table}" '
            f"(id, customer_id, total, order_date, status) FROM STDIN"
        ) as copy:
            for r in rows:
                copy.write_row(
                    (
                        r["id"],
                        r["customer_id"],
                        r["total"],
                        r["order_date"],
                        r.get("status"),
                    )
                )
        conn.commit()


def _read_source_for_interval(
    source_schema: str,
    source_table: str,
    since: datetime,
    until: datetime,
    chunk_size: int = 100,
) -> Generator[pl.DataFrame, None, None]:
    """Yield chunks from the source table for the given window.

    Reads everything for the window into memory FIRST and closes the
    connection, then yields slices. The connection-per-generator
    pattern (open conn, cursor with `fetchmany`, yield) leaves the
    connection in "idle in transaction" if the consumer doesn't fully
    exhaust the generator — which would block the subsequent
    `DROP SCHEMA CASCADE` cleanup on the source table. Fully reading
    up front avoids that footgun and is fine for test-sized data."""
    with psycopg.connect(_dsn()) as conn:
        rows = conn.execute(
            f"SELECT id, customer_id, total, order_date, status "
            f'FROM "{source_schema}"."{source_table}" '
            f"WHERE order_date >= %s AND order_date < %s "
            f"ORDER BY order_date",
            [since, until],
        ).fetchall()

    for start in range(0, len(rows), chunk_size):
        batch = rows[start : start + chunk_size]
        yield pl.DataFrame(
            {
                "id": [r[0] for r in batch],
                "customer_id": [r[1] for r in batch],
                "total": [float(r[2]) for r in batch],
                "order_date": [r[3] for r in batch],
                "status": [r[4] for r in batch],
            },
            schema={
                "id": pl.Int64,
                "customer_id": pl.Int64,
                "total": pl.Float64,
                "order_date": pl.Datetime("us", "UTC"),
                "status": pl.Utf8,
            },
        )


def _orders_columns_with_status(*, unique_id: bool = False):
    """Variant of `_orders_columns` that adds `status` and optionally
    flags `id` as primary key for upsert tests."""
    return [
        PostgresColumn(
            name="id",
            data_type=PostgresType.BIGINT,
            nullable=False,
            primary_key=unique_id,
            unique=unique_id,
        ),
        PostgresColumn(
            name="customer_id", data_type=PostgresType.BIGINT, nullable=False
        ),
        PostgresColumn(name="total", data_type=PostgresType.NUMERIC, nullable=False),
        PostgresColumn(
            name="order_date", data_type=PostgresType.TIMESTAMPTZ, nullable=False
        ),
        PostgresColumn(name="status", data_type=PostgresType.TEXT, nullable=True),
    ]


def _drive_through_intervals(
    model: Model,
    source_schema: str,
    source_table: str,
    chunk_size: int = 100,
):
    """Drive `model` through its intervals, reading each from the
    planted source table. Mirrors `_run_intervals` but uses real
    SELECT-from-source as the data feed."""
    from bollhav.model.load_models import _bootstrap_state_for_staged_models

    model._intervals_cached = None
    contract = list(model.intervals)
    _bootstrap_state_for_staged_models([model], state_mode=StateMode.DISCOVER)
    intervals = list(model.intervals) if model.state is not None else contract
    for interval in intervals:
        df_gen = _read_source_for_interval(
            source_schema,
            source_table,
            interval.since,
            interval.until,
            chunk_size=chunk_size,
        )
        with psycopg.connect(_dsn()) as conn:
            write(
                conn=conn,
                model=model,
                df_gen=df_gen,
                since=interval.since,
                until=interval.until,
            )


def test_e2e_staging_append_target_append(schema_name, caplog):
    """staging.write_mode=APPEND + target.write_mode=APPEND.

    600 source rows across 3 daily intervals. Each interval COPYs
    chunks into staging (no dedup), then INSERTs staging → target."""
    _enable_debug_logging(caplog)
    source_schema = f"src_{schema_name}"
    _plant_source_table(
        source_schema,
        "orders_source",
        [
            {
                "id": i,
                "customer_id": (i % 50) + 1,
                "total": float(i),
                "order_date": SINCE + timedelta(hours=i),
                "status": None,
            }
            # 200 rows per day, 3 days = 600 rows
            for i in range(72)
        ],
    )
    try:
        m = _orders_model(schema_name, state=State(), staging=Staging())
        # Switch the columns to the status-aware ones — source carries it.
        m.target.columns = _orders_columns_with_status(unique_id=False)
        m.target.write_mode = WriteMode.APPEND
        _drive_through_intervals(m, source_schema, "orders_source")

        # All 72 rows landed.
        assert _row_count(schema_name, "orders") == 72
        # State table has 3 applied rows.
        state_rows = _state_rows(f"z_{schema_name}", "orders_state")
        assert len(state_rows) == 3
        assert all(status == "applied" for status, _, _ in state_rows)
        # Debug logs show write_to_staging in APPEND mode (3 intervals × N chunks).
        write_logs = [
            r.message
            for r in caplog.records
            if "stage: wrote" in r.message and "(APPEND)" in r.message
        ]
        assert len(write_logs) >= 3  # at least one chunk per interval
        # Apply logs show APPEND target mode.
        apply_logs = [
            r.message
            for r in caplog.records
            if "stage: applied" in r.message and "(APPEND)" in r.message
        ]
        assert len(apply_logs) == 3
    finally:
        _drop_schemas(source_schema)


def test_e2e_staging_append_target_upsert(schema_name, caplog):
    """staging.write_mode=APPEND + target.write_mode=UPSERT_NO_DELETE.

    Source has UNIQUE keys (no in-stream dupes); staging COPYs raw
    rows; the final apply MERGEs staging → target ON CONFLICT DO UPDATE.

    Note: Postgres `INSERT ... ON CONFLICT DO UPDATE` cannot handle
    multiple source rows for the same conflict key in one statement,
    so APPEND-staging + UPSERT-target is only well-defined when the
    source itself is deduped. For a stream WITH duplicates, see
    `test_e2e_staging_upsert_target_upsert` which dedupes via the
    staging-side UPSERT."""
    _enable_debug_logging(caplog)
    source_schema = f"src_{schema_name}"

    # 100 distinct ids per day, no duplicates within the source.
    rows = [
        {
            "id": 1000 * day + order_idx,
            "customer_id": (order_idx % 20) + 1,
            "total": float(order_idx),
            "order_date": SINCE + timedelta(days=day, minutes=order_idx),
            "status": "pending",
        }
        for day in range(3)
        for order_idx in range(100)
    ]
    _plant_source_table(source_schema, "orders_source", rows)
    try:
        m = _orders_model(schema_name, state=State(), staging=Staging())
        m.target.columns = _orders_columns_with_status(unique_id=True)
        m.target.write_mode = WriteMode.UPSERT_NO_DELETE
        m.target.unique_columns = [m.target.columns[0]]  # id
        _drive_through_intervals(m, source_schema, "orders_source")

        # 100 distinct ids × 3 days = 300 rows in target.
        assert _row_count(schema_name, "orders") == 300
        # No duplicate ids.
        with _conn() as c:
            dupes = c.execute(
                f'SELECT id, count(*) FROM "{schema_name}"."orders" '
                f"GROUP BY id HAVING count(*) > 1"
            ).fetchall()
        assert dupes == []
        # Apply logs show UPSERT_NO_DELETE target mode.
        apply_logs = [
            r.message
            for r in caplog.records
            if "stage: applied" in r.message and "UPSERT_NO_DELETE" in r.message
        ]
        assert len(apply_logs) == 3
    finally:
        _drop_schemas(source_schema)


def test_e2e_staging_upsert_target_upsert(schema_name, caplog):
    """staging.write_mode=UPSERT_NO_DELETE + target.write_mode=UPSERT_NO_DELETE.

    Source has the same id appearing in multiple chunks with status
    progression — exactly the CDC-stream pattern where staging UPSERT
    is the right shape. Each chunk has unique ids (one status round)
    but the SAME ids appear across chunks. Staging MERGEs each chunk,
    keeping one row per id; the final apply MERGEs that deduped
    staging into the target."""
    _enable_debug_logging(caplog)
    source_schema = f"src_{schema_name}"

    # 4 rounds of 50 orders each. Each round is one chunk's worth and
    # contains unique ids (no within-chunk dupes), but the same id
    # appears in all 4 rounds with progressing status. Timestamps space
    # rounds an hour apart so ORDER BY order_date groups by round.
    statuses = ["pending", "processing", "shipped", "delivered"]
    rows = []
    for day in range(3):
        for step, status in enumerate(statuses):
            for order_idx in range(50):
                rows.append(
                    {
                        "id": 1000 * day + order_idx,
                        "customer_id": (order_idx % 20) + 1,
                        "total": float(order_idx),
                        "order_date": SINCE
                        + timedelta(days=day, hours=step, seconds=order_idx),
                        "status": status,
                    }
                )
    _plant_source_table(source_schema, "orders_source", rows)
    try:
        m = _orders_model(
            schema_name,
            state=State(),
            staging=Staging(write_mode=WriteMode.UPSERT_NO_DELETE),
        )
        m.target.columns = _orders_columns_with_status(unique_id=True)
        m.target.write_mode = WriteMode.UPSERT_NO_DELETE
        m.target.unique_columns = [m.target.columns[0]]
        # chunk_size=50 aligns each chunk to a single status round → each
        # chunk has unique ids, so chunk-level `INSERT ... ON CONFLICT`
        # into staging works. Dupes (different statuses for the same id)
        # span across chunks; staging UPSERT collapses them.
        _drive_through_intervals(m, source_schema, "orders_source", chunk_size=50)

        # 50 distinct ids × 3 days = 150 rows.
        assert _row_count(schema_name, "orders") == 150
        # Confirm staging-side write logs show UPSERT mode (not append).
        write_logs = [
            r.message
            for r in caplog.records
            if "stage: wrote" in r.message and "(UPSERT_NO_DELETE)" in r.message
        ]
        assert len(write_logs) >= 3  # at least one chunk per interval
        # And apply-side logs show UPSERT mode too.
        apply_logs = [
            r.message
            for r in caplog.records
            if "stage: applied" in r.message and "UPSERT_NO_DELETE" in r.message
        ]
        assert len(apply_logs) == 3
    finally:
        _drop_schemas(source_schema)


def test_e2e_staging_append_target_recreate_partition(schema_name, caplog):
    """staging.write_mode=APPEND + target.write_mode=RECREATE_PARTITION.

    Source has pre-existing rows in the target's window (planted
    directly). When the apply runs, it DELETEs the target window then
    INSERTs from staging — net effect: target's contents for that
    window are completely replaced by what was in staging."""
    _enable_debug_logging(caplog)
    source_schema = f"src_{schema_name}"

    # Plant the SOURCE table with 600 rows we want to land via staging.
    _plant_source_table(
        source_schema,
        "orders_source",
        [
            {
                "id": i,
                "customer_id": (i % 50) + 1,
                "total": float(i) + 100.0,  # distinguishable from pre-existing
                "order_date": SINCE + timedelta(hours=i),
                "status": None,
            }
            for i in range(72)
        ],
    )
    try:
        # Pre-populate the target with junk rows in the window — these
        # MUST get overwritten by the RECREATE_PARTITION apply.
        with psycopg.connect(_dsn()) as conn:
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
            conn.execute(
                f'CREATE TABLE "{schema_name}"."orders" ('
                f"  id BIGINT NOT NULL,"
                f"  customer_id BIGINT NOT NULL,"
                f"  total NUMERIC NOT NULL,"
                f"  order_date TIMESTAMPTZ NOT NULL,"
                f"  status TEXT"
                f")"
            )
            conn.execute(
                f'INSERT INTO "{schema_name}"."orders" VALUES '
                f"(99999, 1, -1.0, '2024-01-01T00:00:00Z', 'junk'),"
                f"(99998, 1, -1.0, '2024-01-02T00:00:00Z', 'junk'),"
                f"(99997, 1, -1.0, '2024-01-03T00:00:00Z', 'junk')"
            )
            conn.commit()

        m = _orders_model(schema_name, state=State(), staging=Staging())
        m.target.columns = [
            PostgresColumn(name="id", data_type=PostgresType.BIGINT, nullable=False),
            PostgresColumn(
                name="customer_id", data_type=PostgresType.BIGINT, nullable=False
            ),
            PostgresColumn(
                name="total", data_type=PostgresType.NUMERIC, nullable=False
            ),
            PostgresColumn(
                name="order_date",
                data_type=PostgresType.TIMESTAMPTZ,
                nullable=False,
                partition_on=True,  # required for RECREATE_PARTITION
            ),
            PostgresColumn(name="status", data_type=PostgresType.TEXT, nullable=True),
        ]
        m.target.write_mode = WriteMode.RECREATE_PARTITION
        # `partitioned_by` is a @property derived from columns; setting
        # `partition_on=True` on the order_date column above is enough.
        _drive_through_intervals(m, source_schema, "orders_source")

        # 72 fresh rows, the 3 junk rows wiped.
        assert _row_count(schema_name, "orders") == 72
        with _conn() as c:
            junk = c.execute(
                f'SELECT count(*) FROM "{schema_name}"."orders" WHERE status = \'junk\''
            ).fetchone()
        assert junk[0] == 0
        # Apply logs show RECREATE_PARTITION mode.
        apply_logs = [
            r.message
            for r in caplog.records
            if "stage: applied" in r.message and "RECREATE_PARTITION" in r.message
        ]
        assert len(apply_logs) == 3
    finally:
        _drop_schemas(source_schema)
