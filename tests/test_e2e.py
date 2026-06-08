"""End-to-end tests against a real Postgres.

These tests cover the features introduced on the `state2` branch:
state lifecycle, mutations one-shot setup, staging (fresh table per
interval, dropped on flush) — with and without state, library
registration (state-tracked models only), enforced upstreams vs
unregistered-upstream-as-documentation, auto orphan staging GC at
bootstrap, and additive library schema migration.

Each test uses a deterministic schema derived from its node id (unique
per test, stable across runs — no random suffix) so multiple tests can
run in parallel safely. The per-test target + staging schemas are dropped
CASCADE; this test's state tables in `z_bollhav_state` and its rows in the
shared `z_bollhav.errors` / library (`z_bollhav`) are cleaned around each
test.

Skip when `E2E_DSN` is unset and the local default isn't reachable.
"""

from __future__ import annotations

import logging
import os
import re
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
    IntervalContract,
    Kind,
    Model,
    Source,
    SourceApi,
    SourceModel,
    Staging,
    State,
    Target,
    WriteMode,
)
from bollhav.model.state import StateMode
from bollhav.postgres import (
    PostgresColumn,
    PostgresType,
    create_replace_view,
    write,
)
from bollhav.postgres.data import PostgresData
from bollhav.postgres.state import LIBRARY_SCHEMA, PostgresState, state_table_name


DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/postgres"
SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
CAT = "e2e"  # catalog for e2e models (full identity catalog.schema.table)
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
    """A deterministic target schema per test, derived from the test's
    node id (no random suffix) and dropped before and after.

    Each test still gets a unique schema (node names are unique, including
    parametrized variants), so parallel runs don't collide — but the name
    is stable across runs, so there's nothing random in the table names.
    Cleanup also removes this schema's state tables from `z_bollhav_state`
    and its rows from the shared `z_bollhav.errors`, so a rerun can't inherit
    stale `applied` rows."""
    name = re.sub(r"[^a-z0-9]+", "_", request.node.name.lower()).strip("_")[:54]
    _drop_schemas(name)
    yield name
    _drop_schemas(name)


def _drop_schemas(target_schema: str) -> None:
    with psycopg.connect(_dsn(), autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE')
        conn.execute(f'DROP SCHEMA IF EXISTS "z_{target_schema}" CASCADE')
        _drop_central_state_tables(conn, target_schema)


def _drop_central_state_tables(conn, target_schema: str) -> None:
    """Drop this schema's per-model state tables from `z_bollhav_state`, and
    delete its rows from the shared `z_bollhav.errors`.

    State tables for every model live in the shared `z_bollhav_state`, named
    by a hash — so they aren't swept by the per-schema `DROP SCHEMA CASCADE`
    above. Each self-identifies via its `model_name` column; match
    `target_schema.%` and drop it. Errors are one shared table keyed by
    `full_name`, so we just delete this schema's rows. Only this test's data
    is touched, so it's safe under parallel runs."""
    from bollhav.postgres.state import ERRORS_TABLE, LIBRARY_SCHEMA, LIBRARY_TABLE

    have_state = conn.execute(
        "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
        [LIBRARY_SCHEMA],
    ).fetchone()
    if have_state:
        tables = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = %s", [LIBRARY_SCHEMA]
        ).fetchall()
        for (table,) in tables:
            # State tables now share the schema with the fixed library/errors
            # tables (which have no model_name column) — skip those.
            if table in (LIBRARY_TABLE, ERRORS_TABLE):
                continue
            owner = conn.execute(
                f'SELECT model_name FROM "{LIBRARY_SCHEMA}"."{table}" LIMIT 1'
            ).fetchone()
            # full_name is catalog.schema.table now, so match the schema as a
            # dot-delimited component (not a prefix).
            if owner and owner[0] and target_schema in str(owner[0]).split("."):
                conn.execute(f'DROP TABLE IF EXISTS "{LIBRARY_SCHEMA}"."{table}"')

    have_errors = conn.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        [LIBRARY_SCHEMA, ERRORS_TABLE],
    ).fetchone()
    if have_errors:
        conn.execute(
            f'DELETE FROM "{LIBRARY_SCHEMA}"."{ERRORS_TABLE}" WHERE full_name LIKE %s',
            [f"%{target_schema}.%"],
        )


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
    upstream: list[Source] | None = None,
    sources: list[Source] | None = None,
) -> Model:
    m = Model(
        target=Target(
            name=name,
            schema=schema_name,
            catalog=CAT,
            database=Database.POSTGRES,
            write_mode=WriteMode.APPEND,
            dsn_env_var="TARGET_DSN",
            staging=staging,
            columns=_orders_columns(),
        ),
        state=state,
        batching=Batch(interval=IntervalChunks(expression="@daily")),
        kind=Kind.INTERVAL,
        bounds=Bounds(begin=SINCE, end=bounds_end),
        # Gated upstreams and ungated sources are one list now.
        upstream=(upstream or []) + (sources or []),
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


def _bootstrap(models, *, state_mode: StateMode = StateMode.DISCOVER) -> None:
    """Run the `@model_lifecycle` setup for each model directly, on a
    single autocommit connection — what the user's `execute_model` would
    trigger via the lifecycle hook.

    This mirrors the Postgres-target branch of `@model_lifecycle`: asset
    DDL via `PostgresData` (schema, view-or-table, indexes, unique
    constraint, staging schema + orphan GC), then — for stateful models —
    the state bootstrap via `PostgresState` (ensure_library /
    register_model / ensure_tables, seed the interval or singleton rows,
    and filter `model.intervals` down to the actionable subset)."""
    with psycopg.connect(_dsn(), autocommit=True) as conn:
        for model in models:
            # `model.state.mode` drives DISCOVER vs BULLDOZER prefill.
            if model.state is not None:
                model.state.mode = state_mode

            data = PostgresData(model=model, conn=conn)
            data.create_schema()
            if model.is_view:
                data.create_or_replace_view()
            else:
                if model.target.recreate_table:
                    data.recreate_table()
                data.create_table()
                if model.target.truncate_table:
                    data.truncate_table()
                if model.target.partitioned_by is not None:
                    data.create_indexes()
                if model.target.unique_columns:
                    data.add_unique_constraint()
                if model.target.stage:
                    data.create_staging_schema()
                    data.gc_orphan_staging_tables()

            if model.stateful:
                state = PostgresState(model=model, conn=conn)
                state.ensure_library()
                state.register_model()
                state.ensure_tables()
                if model.is_kind_monolithic or model.is_view:
                    state.insert_singleton(run_id=model.run_id)
                else:
                    state.insert_intervals(
                        run_id=model.run_id,
                        intervals=model.compute_intervals(),
                    )
                model.intervals = state.get_actionable_intervals()


def _run_intervals(model: Model, *, error_on_interval: int | None = None):
    """Drive a model's intervals through the staged write path, the way a
    user's `@model_lifecycle`-wrapped loop would after the bootstrap.

    Mirrors `@execute_lifecycle`'s staged-execute flow per interval:
    `PostgresData.create_staging_table` → `write` (lands chunks into
    staging) → `apply_staging_to_target` → `drop_staging_table`, then the
    decoupled state flip via `PostgresState.mark_applied` on the same
    connection."""
    # Mint the contract before bootstrap so we know the full interval set.
    contract = list(model.compute_intervals())
    _bootstrap([model])

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
            if model.is_view:
                create_replace_view(conn=conn, model=model)
            else:
                df_gen = _gen_rows(since, until)
                if model.target.stage:
                    data = PostgresData(model=model, conn=conn)
                    data.create_staging_table(model.run_id)
                    write(
                        conn=conn,
                        model=model,
                        df_gen=df_gen,
                        since=since,
                        until=until,
                    )
                    data.apply_staging_to_target(model.run_id, interval)
                    data.drop_staging_table(model.run_id)
                else:
                    write(
                        conn=conn,
                        model=model,
                        df_gen=df_gen,
                        since=since,
                        until=until,
                    )
                # State flip is now a separate step (staging no longer flips
                # it). Mark applied on the SAME connection — what
                # @execute_lifecycle does — no new connection opened.
                if model.state is not None:
                    PostgresState(model=model, conn=conn).mark_applied(
                        run_id=model.run_id,
                        interval=interval,
                    )


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
            "FROM z_bollhav.library "
            "ORDER BY full_name"
        ).fetchall()
    return rows


# ── 1. state lifecycle: pending → applied ────────────────────────────


def test_e2e_state_lifecycle(schema_name):
    """3 daily intervals: bootstrap prefills pending; loop drives
    them through to applied via the staged flush."""
    m = _orders_model(schema_name, state=State(), staging=Staging())
    _run_intervals(m)

    rows = _state_rows(LIBRARY_SCHEMA, state_table_name(f"{CAT}.{schema_name}.orders"))
    assert len(rows) == 3
    assert all(status == "applied" for status, _, _ in rows)
    assert _row_count(schema_name, "orders") == 15  # 3 intervals × 5 rows


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
    fq = f"{CAT}.{schema_name}.orders"
    matches = [r for r in rows if r[0] == fq]
    assert len(matches) == 1
    _, model_type, st_sch, st_tbl = matches[0]
    assert model_type == "TABLE"
    assert st_sch == LIBRARY_SCHEMA
    assert st_tbl == state_table_name(f"{CAT}.{schema_name}.orders")


# ── 8. view-as-upstream: unregistered upstream is documentation ──────


def test_e2e_view_as_upstream_does_not_block_downstream(schema_name):
    """orders (state+staging) → v_high_value (VIEW) → enriched
    (state+staging, upstream=[v_high_value]).

    Only state-tracked models register in the library, so the view is
    NOT registered. A state downstream referencing it finds no library
    entry and treats the upstream as documentation (not enforced) — so
    enriched's intervals all come up `applied`, not blocked."""

    orders = _orders_model(schema_name, state=State(), staging=Staging())

    view = Model(
        upstream=[
            Source(
                "v_high_value",
                type=SourceModel(
                    query=f"SELECT * FROM {schema_name}.orders WHERE total >= 0"
                ),
            )
        ],
        target=Target(
            name="v_high_value",
            schema=schema_name,
            catalog=CAT,
            dsn_env_var="TARGET_DSN",
        ),
        kind=Kind.VIEW,
    )

    # An ungated source (no contract) is never enforced — the unregistered
    # view is documentation, so enriched's intervals come up applied.
    enriched = _orders_model(
        schema_name,
        name="enriched",
        state=State(),
        staging=Staging(),
        upstream=[Source(f"{CAT}.{schema_name}.v_high_value", type=SourceModel())],
    )

    _bootstrap([orders, view, enriched])

    # Run in topo order.
    _run_intervals(orders)
    _run_intervals(view)
    _run_intervals(enriched)

    enriched_rows = _state_rows(
        LIBRARY_SCHEMA, state_table_name(f"{CAT}.{schema_name}.enriched")
    )
    assert len(enriched_rows) == 3
    assert all(status == "applied" for status, _, _ in enriched_rows)

    # The view is state-less → it does not register in the library.
    lib = {r[0] for r in _library_rows()}
    assert f"{CAT}.{schema_name}.v_high_value" not in lib


# ── 9. orphan staging GC at bootstrap ────────────────────────────────


def test_e2e_orphan_staging_gc_at_bootstrap(schema_name):
    """Seed an orphan staging table from a prior fake run, then
    bootstrap a new run on the same model. The orphan should be
    dropped by the auto-GC at bootstrap."""

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
    _bootstrap([m])

    after = [t for t in _list_tables(state_schema) if "staging_" in t]
    assert "orders_staging_dead0000" not in after


# ── 12. additive library migration ───────────────────────────────────


def test_e2e_library_migration_is_additive(schema_name):
    """Seed an old-shape `library` (no model_type column,
    state_schema/table NOT NULL). Bootstrap should ALTER it in place
    — no DROP — and the existing row should survive."""
    _drop_library()
    with psycopg.connect(_dsn(), autocommit=True) as c:
        c.execute("CREATE SCHEMA z_bollhav")
        c.execute(
            "CREATE TABLE z_bollhav.library ("
            "  full_name TEXT PRIMARY KEY,"
            "  upstream TEXT[] NOT NULL,"
            "  state_schema TEXT NOT NULL,"
            "  state_table TEXT NOT NULL,"
            "  last_seen TIMESTAMPTZ NOT NULL DEFAULT now()"
            ")"
        )
        c.execute(
            "INSERT INTO z_bollhav.library "
            "(full_name, upstream, state_schema, state_table) "
            "VALUES ('legacy.model', '{}', 'z_legacy', 'model_state')"
        )

    # `ensure_library` is now a `PostgresState` method; the library DDL /
    # additive migration doesn't depend on the model, so any state-tracked
    # model serves to drive it.
    m = _orders_model(schema_name, state=State(), staging=Staging())
    with psycopg.connect(_dsn()) as c:
        PostgresState(model=m, conn=c).ensure_library()
        c.commit()

    # New column present
    with _conn() as c:
        cols = [
            r[0]
            for r in c.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='z_bollhav' AND table_name='library'"
            ).fetchall()
        ]
    assert "model_type" in cols
    # state_schema and state_table now nullable
    with _conn() as c:
        nullable = dict(
            c.execute(
                "SELECT column_name, is_nullable FROM information_schema.columns "
                "WHERE table_schema='z_bollhav' AND table_name='library' "
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
    "bollhav.postgres.state",
    "bollhav.model.lifecycle",
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

    contract = list(model.compute_intervals())
    _bootstrap([model])
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
            # Staged-execute flow: fresh staging table → land chunks →
            # atomic apply → drop, then the decoupled state flip.
            data = PostgresData(model=model, conn=conn)
            data.create_staging_table(model.run_id)
            write(
                conn=conn,
                model=model,
                df_gen=df_gen,
                since=interval.since,
                until=interval.until,
            )
            data.apply_staging_to_target(model.run_id, interval)
            data.drop_staging_table(model.run_id)
            # Staging no longer flips state — mark applied on the same
            # connection, the way @execute_lifecycle does.
            if model.state is not None:
                PostgresState(model=model, conn=conn).mark_applied(
                    run_id=model.run_id,
                    interval=interval,
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
        state_rows = _state_rows(
            LIBRARY_SCHEMA, state_table_name(f"{CAT}.{schema_name}.orders")
        )
        assert len(state_rows) == 3
        assert all(status == "applied" for status, _, _ in state_rows)
        # Debug logs show write_to_staging in APPEND mode (3 intervals × N chunks).
        write_logs = [
            r.message
            for r in caplog.records
            if "wrote" in r.message and "(APPEND)" in r.message
        ]
        assert len(write_logs) >= 3  # at least one chunk per interval
        # Apply logs show APPEND target mode.
        apply_logs = [
            r.message
            for r in caplog.records
            if "moved data" in r.message and "(APPEND)" in r.message
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
            if "moved data" in r.message and "UPSERT_NO_DELETE" in r.message
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
            if "wrote" in r.message and "(UPSERT_NO_DELETE)" in r.message
        ]
        assert len(write_logs) >= 3  # at least one chunk per interval
        # And apply-side logs show UPSERT mode too.
        apply_logs = [
            r.message
            for r in caplog.records
            if "moved data" in r.message and "UPSERT_NO_DELETE" in r.message
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
            if "moved data" in r.message and "RECREATE_PARTITION" in r.message
        ]
        assert len(apply_logs) == 3
    finally:
        _drop_schemas(source_schema)


# ── 13. registry read API (lineage) ──────────────────────────────────


def _registry_models(schema_name):
    """A producer `orders` and a downstream `summary` that declares an
    upstream contract on it plus two external sources."""
    orders = _orders_model(schema_name, name="orders", state=State(), staging=Staging())
    summary = _orders_model(
        schema_name,
        name="summary",
        state=State(),
        staging=Staging(),
        upstream=[
            Source(
                f"{CAT}.{schema_name}.orders",
                type=SourceModel(),
                contract=IntervalContract(),
            )
        ],
        sources=[
            Source("raw.landing", type=SourceModel()),
            Source("vendor.api_orders", type=SourceApi()),
        ],
    )
    return orders, summary


def test_e2e_registry_get_lineage_matches_model_lineage(schema_name):
    from bollhav.postgres import registry

    orders, summary = _registry_models(schema_name)
    _bootstrap([orders, summary])

    summary_fqn = f"{CAT}.{schema_name}.summary"
    with _conn() as c:
        lineage = registry.get_lineage(c, summary_fqn)

    # The DB-read lineage matches the in-code Model.lineage() shape exactly:
    # upstream typed by the upstream's registered kind (= the contract kind
    # here), sources typed by their source type (model / file / api).
    assert lineage == summary.lineage()
    assert lineage["upstream"] == [
        {"name": f"{CAT}.{schema_name}.orders", "kind": "interval"}
    ]
    assert {"name": "vendor.api_orders", "kind": "api"} in lineage["sources"]
    assert lineage["inputs_known"] is True


def test_e2e_registry_list_and_downstreams_and_graph(schema_name):
    from bollhav.postgres import registry

    orders, summary = _registry_models(schema_name)
    _bootstrap([orders, summary])
    orders_fqn = f"{CAT}.{schema_name}.orders"
    summary_fqn = f"{CAT}.{schema_name}.summary"

    with _conn() as c:
        names = {m["full_name"] for m in registry.list_models(c)}
        downstreams = registry.get_downstreams(c, orders_fqn)
        graph = registry.get_graph(c)

    assert {orders_fqn, summary_fqn} <= names
    # who depends on orders → summary
    assert summary_fqn in downstreams
    # graph has both model nodes + the external source boundary nodes
    node_names = {n["name"] for n in graph["nodes"]}
    assert {orders_fqn, summary_fqn, "raw.landing", "vendor.api_orders"} <= node_names
    assert {
        "name": "vendor.api_orders",
        "type": "external",
        "kind": "api",
    } in graph["nodes"]
    # edges: upstream (orders→summary) and source (raw.landing→summary)
    assert {"from": orders_fqn, "to": summary_fqn, "relation": "upstream"} in graph[
        "edges"
    ]
    assert {
        "from": "raw.landing",
        "to": summary_fqn,
        "relation": "source",
        "kind": "model",
    } in graph["edges"]


def test_e2e_registry_get_upstream_tree_nests(schema_name):
    from bollhav.postgres import registry

    # orders -> summary -> rollup : a 3-deep chain.
    orders = _orders_model(schema_name, name="orders", state=State(), staging=Staging())
    summary = _orders_model(
        schema_name,
        name="summary",
        state=State(),
        staging=Staging(),
        upstream=[
            Source(
                f"{CAT}.{schema_name}.orders",
                type=SourceModel(),
                contract=IntervalContract(),
            )
        ],
        sources=[Source("raw.landing", type=SourceApi())],
    )
    rollup = _orders_model(
        schema_name,
        name="rollup",
        state=State(),
        staging=Staging(),
        upstream=[
            Source(
                f"{CAT}.{schema_name}.summary",
                type=SourceModel(),
                contract=IntervalContract(),
            )
        ],
    )
    _bootstrap([orders, summary, rollup])

    with _conn() as c:
        tree = registry.get_upstream_tree(c, f"{CAT}.{schema_name}.rollup")

    assert tree["model"] == f"{CAT}.{schema_name}.rollup"
    child = tree["upstream"][0]
    assert child["model"] == f"{CAT}.{schema_name}.summary"
    assert child["sources"] == [{"name": "raw.landing", "kind": "api"}]
    grandchild = child["upstream"][0]
    assert grandchild["model"] == f"{CAT}.{schema_name}.orders"


def test_e2e_registry_get_recent_state(schema_name):
    from bollhav.postgres import registry

    m = _orders_model(schema_name, name="orders", state=State(), staging=Staging())
    _run_intervals(m)  # bootstrap + run all intervals -> applied state rows

    with _conn() as c:
        rows = registry.get_recent_state(c, f"{CAT}.{schema_name}.orders")

    assert len(rows) >= 1
    assert all(r["status"] == "applied" for r in rows)
    assert rows[0]["applied_at"]  # ISO string
    assert rows[0]["run_id"]


def test_e2e_registry_get_errors(schema_name):
    from bollhav.postgres import registry

    m = _orders_model(schema_name, name="orders", state=State(), staging=Staging())
    _bootstrap([m])
    fqn = f"{CAT}.{schema_name}.orders"
    # Log a failure the way @execute_lifecycle would, into z_bollhav.errors.
    with psycopg.connect(_dsn(), autocommit=True) as conn:
        PostgresState(model=m, conn=conn).record_failure(
            run_id=m.run_id,
            interval=None,
            error_type="RuntimeError",
            error_message="boom",
            traceback_text="Traceback ...",
            update_state=False,
        )

    with _conn() as c:
        errs = registry.get_errors(c, fqn)

    assert len(errs) >= 1
    assert errs[0]["full_name"] == fqn
    assert errs[0]["error_type"] == "RuntimeError"
    assert errs[0]["created_at"]  # ISO string


# ── 14. SCHEMA_SUFFIX env isolation ──────────────────────────────────


def test_e2e_schema_suffix_isolates_state_and_library(schema_name):
    """A SCHEMA_SUFFIX run registers + gates in its OWN bollhav schemas
    (`z_bollhav_state_<suffix>` / `z_bollhav_<suffix>`), isolated from prod's
    `z_bollhav_state` / `z_bollhav` — dev and prod state never mix."""
    from bollhav.model import Source, SourceModel
    from bollhav.model.upstream import MonolithicContract
    from bollhav.postgres.state import LIBRARY_SCHEMA

    suf = "iso"  # appendix disabled below → stable env-schema name
    env_lib = f"z_bollhav_{suf}"  # dev consolidates state + library + errors here
    env_state = f"z_bollhav_state_{suf}"  # must NOT be created (prod-only split)

    def mk(name, upstream=None):
        return Model(
            target=Target(
                name=name,
                schema=schema_name,
                catalog=CAT,
                schema_suffix=suf,
                schema_suffix_appendix=None,
                database=Database.POSTGRES,
                dsn_env_var="TARGET_DSN",
                columns=_orders_columns(),
            ),
            kind=Kind.MONOLITHIC,
            state=State(),
            upstream=upstream or [],
        )

    orders = mk("orders")
    summary = mk(
        "summary",
        upstream=[
            Source(
                f"{CAT}.{schema_name}.orders",
                type=SourceModel(),
                contract=MonolithicContract(),
            )
        ],
    )

    def _drop_env():
        with psycopg.connect(_dsn(), autocommit=True) as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS "{env_lib}" CASCADE')
            conn.execute(f'DROP SCHEMA IF EXISTS "{env_state}" CASCADE')

    _drop_env()
    try:
        with psycopg.connect(_dsn(), autocommit=True) as conn:
            so = PostgresState(orders, conn)
            # dev consolidates: state schema == library schema == one env schema
            assert so._library_schema() == env_lib
            assert so._state_schema() == env_lib
            so.ensure_library()
            so.ensure_tables()
            so.register_model()

            # the separate prod-style state schema is NOT created for dev
            assert (
                conn.execute(
                    "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                    [env_state],
                ).fetchone()
                is None
            )

            # registered in the ENV library, absent from prod's
            assert (
                PostgresState.lookup_model(conn, orders.target.full_name, env_lib)
                is not None
            )
            assert (
                PostgresState.lookup_model(
                    conn, orders.target.full_name, LIBRARY_SCHEMA
                )
                is None
            )

            # summary's gating resolves orders WITHIN the env (suffixed lookup +
            # env library) — it finds it (no 'unregistered' raise) and is merely
            # blocked because orders has no applied row yet.
            ss = PostgresState(summary, conn)
            ss.ensure_library()
            assert ss.is_upstream_satisfied_live(None).satisfied is False
    finally:
        _drop_env()


# ── 15. DRY_STATE ────────────────────────────────────────────────────


def test_e2e_dry_state_plans_without_executing(schema_name, capsys, monkeypatch):
    """DRY_STATE=true runs the state bootstrap and prints the resolved plan,
    but creates no target assets, writes no data, and runs no model logic."""
    from bollhav.model.lifecycle import model_lifecycle

    monkeypatch.setenv("DRY_STATE", "true")
    ran = {"executed": False}

    @model_lifecycle
    def run_model(model, data_conn, state_conn=None):
        ran["executed"] = True  # must not happen under DRY_STATE

    m = _orders_model(schema_name, name="orders", state=State(), staging=Staging())

    with psycopg.connect(_dsn(), autocommit=True) as conn:
        run_model(m, conn, conn)
        out = capsys.readouterr().out

        # the plan was printed for this model
        assert f"{CAT}.{schema_name}.orders" in out
        assert "would run" in out
        # the model body never ran
        assert ran["executed"] is False
        # no target table was created (asset DDL skipped)
        assert (
            conn.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name='orders'",
                [schema_name],
            ).fetchone()
            is None
        )
        # but state WAS bootstrapped (so the plan could be resolved)
        st = PostgresState(m, conn)._state_table()
        assert (
            conn.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema='z_bollhav' AND table_name=%s",
                [st],
            ).fetchone()
            is not None
        )


def test_e2e_dry_state_cascade_shows_will_run_after(schema_name, capsys, monkeypatch):
    """DRY_STATE understands the cascade: a downstream gated on an upstream that
    would itself run this pass shows 'will run after <upstream>', not blocked."""
    from bollhav.model.lifecycle import _DRY_STATE_RUNS, model_lifecycle

    _DRY_STATE_RUNS.clear()
    monkeypatch.setenv("DRY_STATE_EXTRA", "true")

    @model_lifecycle
    def run_model(model, data_conn, state_conn=None):
        for _ in model.intervals:
            pass

    orders = _orders_model(schema_name, name="orders", state=State(), staging=Staging())
    summary = _orders_model(
        schema_name,
        name="summary",
        state=State(),
        staging=Staging(),
        upstream=[
            Source(
                f"{CAT}.{schema_name}.orders",
                type=SourceModel(),
                contract=IntervalContract(),
            )
        ],
    )

    with psycopg.connect(_dsn(), autocommit=True) as conn:
        run_model(orders, conn, conn)  # records orders' would-run windows
        run_model(summary, conn, conn)  # sees orders in the cascade overlay
        out = capsys.readouterr().out

    # orders runs now; summary cascades off it (not blocked)
    assert f"{CAT}.{schema_name}.summary" in out
    assert f"will run after {CAT}.{schema_name}.orders" in out
    # summary's per-model summary line: all 3 would run, none blocked
    assert "would run 3  ·  blocked 0" in out
    assert "blocked:" not in out  # nothing actually blocked
