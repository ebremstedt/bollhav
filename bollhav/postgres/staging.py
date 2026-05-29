"""Postgres staging: stream sub-batches into a per-interval staging
table, then move the data into the target in one transaction.

Use case: an interval produces more rows than fit in memory (or one
transaction), but state granularity stays at `(since, until)`. Sub-batches
COPY into staging; the context exit moves staging → target and flips
the state row in a single tx, so a crash mid-stream cannot leave a
half-written interval marked applied.

Phase 1 scope:
  * APPEND write mode only — other modes will route their final-move
    SQL through the same pattern once added
  * State always co-locates with target (current `bollhav.postgres.state`
    invariant); cross-DB state would break the atomic flush

API:
    with stage(conn, model, since, until) as s:
        for chunk in source:
            s.write(chunk)
    # context exit: INSERT INTO target SELECT * FROM staging
    #               DROP staging
    #               UPDATE state SET applied
    # all in one transaction.

Crash mid-stream: staging table is left in place, state row stays
pending. The next invocation's `gc_orphan_staging_tables(model)` GCs
it by name pattern.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Iterator
from uuid import UUID

import polars as pl
import psycopg
from psycopg import sql

from bollhav.model.write_modes import WriteMode
from bollhav.postgres import state as pg_state

if TYPE_CHECKING:
    from bollhav.model.model import Model

logger = logging.getLogger(__name__)


# ── naming ──────────────────────────────────────────────────────────


def _staging_schema(model: "Model") -> str:
    """Resolve the staging schema. `Staging.schema` overrides; default
    is `z_<target_schema>` (co-located with state, both bollhav-owned)."""
    if model.target.staging is not None and model.target.staging.schema:
        return model.target.staging.schema
    return pg_state._state_schema(model)


def _staging_table_prefix(model: "Model") -> str:
    """Resolve the staging-table name prefix. `Staging.table_prefix`
    overrides; default is `<target_name>_staging_`."""
    if model.target.staging is not None and model.target.staging.table_prefix:
        return model.target.staging.table_prefix
    return f"{model.target.name}_staging_"


def _staging_table(model: "Model", run_id: UUID) -> str:
    """Per-interval staging table name. First 8 hex chars of run_id
    disambiguate within a model. Full run_id is on the state row if
    needed."""
    return f"{_staging_table_prefix(model)}{str(run_id)[:8]}"


def _assert_supported(model: "Model") -> None:
    if model.batching is None:
        raise ValueError(
            f"stage() requires model.batching to be set on {model.target.full_name!r}"
        )
    if model.target.write_mode is not WriteMode.APPEND:
        raise NotImplementedError(
            f"stage() currently supports WriteMode.APPEND only; "
            f"{model.target.full_name!r} uses {model.target.write_mode.value!r}"
        )
    if model.state is not None and model.state.dsn_env_var is not None:
        raise NotImplementedError(
            f"stage() currently requires state to share a DB with the target "
            f"(leave State() without dsn_env_var) — the atomic flush moves "
            f"data and flips the state row in one transaction, which can't "
            f"span databases. Got state.dsn_env_var="
            f"{model.state.dsn_env_var!r} on {model.target.full_name!r}."
        )


# ── DDL ─────────────────────────────────────────────────────────────


def ensure_staging_schema(conn: psycopg.Connection, model: "Model") -> None:
    """Idempotently create the staging schema (`z_<target_schema>` by
    default — co-located with state). Gated by
    `target.mutations.staging_schema_created` so the `CREATE SCHEMA
    IF NOT EXISTS` only fires on the first interval of a pipeline run.

    Necessary on the staging-without-state path because nothing else
    creates the schema: state-tracked staging gets it for free as a
    side-effect of `pg_state.ensure_tables`."""
    if model.target.mutations.staging_schema_created:
        return
    schema = _staging_schema(model)
    conn.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(
            schema=sql.Identifier(schema),
        )
    )
    model.target.mutations.staging_schema_created = True


def ensure_staging_table(
    conn: psycopg.Connection, model: "Model", run_id: UUID
) -> None:
    """Create the staging table mirroring target columns.

    No indexes, no constraints — staging is write-once via COPY then
    drained by a single INSERT...SELECT. UNLOGGED by default: writes
    skip WAL (~2-3x faster COPY), and a crash truncates staging —
    fine, since the interval reruns from the top. Flip `Staging.logged`
    to opt into LOGGED for compliance/replication environments.

    The staging schema is ensured up-front via `ensure_staging_schema`,
    which is a one-shot gated by `mutations.staging_schema_created`.

    In `StagingMode.REUSED` (the default) the CREATE itself is also
    one-shot — gated by `mutations.staging_table_created` — so a
    365-interval backfill issues a single `CREATE TABLE` instead of
    365. In `StagingMode.PER_INTERVAL` the CREATE fires every interval,
    since the previous interval's flush dropped the table."""
    from bollhav.model.staging import StagingMode
    from bollhav.postgres.columns import PostgresColumn
    from bollhav.postgres.schema import _col_ddl

    ensure_staging_schema(conn, model)
    mode = _staging_mode(model)
    if mode is StagingMode.REUSED and model.target.mutations.staging_table_created:
        return

    schema = _staging_schema(model)
    table = _staging_table(model, run_id)

    logged = model.target.staging is not None and model.target.staging.logged
    table_keyword = "TABLE" if logged else "UNLOGGED TABLE"

    col_defs = sql.SQL(",\n").join(
        sql.SQL(_col_ddl(col))
        for col in model.target.columns
        if isinstance(col, PostgresColumn)
    )
    conn.execute(
        sql.SQL(
            f"CREATE {table_keyword} IF NOT EXISTS "
            "{schema}.{table} (\n{col_defs}\n)"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            col_defs=col_defs,
        )
    )
    if mode is StagingMode.REUSED:
        model.target.mutations.staging_table_created = True


def _staging_mode(model: "Model"):
    """Resolve the configured `StagingMode` for the model. Defaults
    to `StagingMode.REUSED` when staging is set without an explicit
    mode (matches the `Staging` dataclass default)."""
    from bollhav.model.staging import StagingMode

    if model.target.staging is None or model.target.staging.mode is None:
        return StagingMode.REUSED
    return model.target.staging.mode


def truncate_staging_table(
    conn: psycopg.Connection, model: "Model", run_id: UUID
) -> None:
    """Clear the reused staging table before the next interval's
    COPY. Only called in `StagingMode.REUSED`. Cheap on UNLOGGED
    tables — essentially a relfilenode swap."""
    conn.execute(
        sql.SQL("TRUNCATE TABLE {schema}.{table}").format(
            schema=sql.Identifier(_staging_schema(model)),
            table=sql.Identifier(_staging_table(model, run_id)),
        )
    )


def drop_staging_table(conn: psycopg.Connection, model: "Model", run_id: UUID) -> None:
    conn.execute(
        sql.SQL("DROP TABLE IF EXISTS {schema}.{table}").format(
            schema=sql.Identifier(_staging_schema(model)),
            table=sql.Identifier(_staging_table(model, run_id)),
        )
    )


# ── write + flush ────────────────────────────────────────────────────


def copy_to_staging(
    conn: psycopg.Connection, model: "Model", run_id: UUID, df: pl.DataFrame
) -> None:
    """COPY a single sub-batch into the staging table. Each call commits
    its own small tx — staging absorbs partial progress without touching
    target."""
    if len(df) == 0:
        return

    df = df.select([col.name for col in model.target.columns])
    schema = _staging_schema(model)
    table = _staging_table(model, run_id)
    col_names = sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
    query = sql.SQL("COPY {schema}.{table} ({cols}) FROM STDIN").format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
        cols=col_names,
    )
    with conn.transaction():
        with conn.cursor() as cursor:
            with cursor.copy(query) as copy:
                for row in df.rows():
                    copy.write_row(row)
    logger.debug("stage: copied %d rows to %s.%s", len(df), schema, table)


def flush_to_target(
    conn: psycopg.Connection,
    model: "Model",
    *,
    run_id: UUID,
    since: datetime,
    until: datetime,
) -> None:
    """Atomic flush: INSERT INTO target SELECT * FROM staging,
    DROP staging, and — when state is enabled — UPDATE the state row
    to applied. All in one transaction.

    On success: data is in target. If state is set, the state row
    also says applied (both flipped under the same commit).
    On failure: postgres rolls back; staging table remains (GC'd next
    run) and state row stays pending. No partial write visible in
    target.

    Without state (`model.state is None`) the flush still gives you
    memory-bounded chunked writes and atomic-per-interval finalization
    — INSERT and DROP commit together — just nothing to flip. Re-runs
    re-process the interval because there's no `applied` gate."""
    staging_schema_id = sql.Identifier(_staging_schema(model))
    staging_table_id = sql.Identifier(_staging_table(model, run_id))
    target_schema_id = sql.Identifier(model.target.schema.resolved)
    target_table_id = sql.Identifier(model.target.name_resolved)

    col_names = sql.SQL(", ").join(
        sql.Identifier(col.name) for col in model.target.columns
    )

    from bollhav.model.staging import StagingMode

    keep = model.target.staging is not None and model.target.staging.keep_after_flush
    mode = _staging_mode(model)
    # REUSED keeps the table for the next interval (its TRUNCATE clears
    # it). PER_INTERVAL drops unless the operator opted into keep.
    drop_after_flush = mode is StagingMode.PER_INTERVAL and not keep

    with conn.transaction():
        conn.execute(
            sql.SQL(
                "INSERT INTO {target_schema}.{target_table} ({cols}) "
                "SELECT {cols} FROM {staging_schema}.{staging_table}"
            ).format(
                target_schema=target_schema_id,
                target_table=target_table_id,
                staging_schema=staging_schema_id,
                staging_table=staging_table_id,
                cols=col_names,
            )
        )
        if drop_after_flush:
            conn.execute(
                sql.SQL("DROP TABLE {schema}.{table}").format(
                    schema=staging_schema_id,
                    table=staging_table_id,
                )
            )
        if model.state is not None:
            conn.execute(
                sql.SQL(
                    "UPDATE {schema}.{table} "
                    "SET status = 'applied', applied_at = now(), run_id = %s "
                    "WHERE since = %s AND until = %s"
                ).format(
                    schema=sql.Identifier(pg_state._state_schema(model)),
                    table=sql.Identifier(pg_state._state_table(model)),
                ),
                [str(run_id), since, until],
            )
    logger.debug(
        "stage: flushed %s..%s for %s",
        since,
        until,
        model.target.full_name,
    )


# ── orphan GC ───────────────────────────────────────────────────────


def gc_orphan_staging_tables(
    model: "Model", *, keep_run_id: UUID | None = None
) -> None:
    """Drop staging tables left behind by crashed runs.

    Matches `<prefix>%` in the staging schema (prefix from
    `Staging.table_prefix` or default). If `keep_run_id` is provided,
    the current run's staging table is preserved.

    No-op when `Staging.keep_after_flush=True` — the operator has
    declared they want kept tables to live; auto-GC would defeat that.
    Manual cleanup is on them."""
    if model.target.staging is not None and model.target.staging.keep_after_flush:
        logger.debug(
            "stage: GC skipped for %s — Staging.keep_after_flush=True",
            model.target.full_name,
        )
        return

    schema = _staging_schema(model)
    prefix = _staging_table_prefix(model)
    keep = f"{prefix}{str(keep_run_id)[:8]}" if keep_run_id is not None else None

    with pg_state._connect(model) as conn:
        rows = conn.execute(
            "SELECT tablename FROM pg_tables "
            "WHERE schemaname = %s AND tablename LIKE %s",
            [schema, f"{prefix}%"],
        ).fetchall()

        with conn.transaction():
            for (tablename,) in rows:
                if keep is not None and tablename == keep:
                    continue
                conn.execute(
                    sql.SQL("DROP TABLE IF EXISTS {schema}.{table}").format(
                        schema=sql.Identifier(schema),
                        table=sql.Identifier(tablename),
                    )
                )
                logger.debug(
                    "stage: gc'd orphan staging table %s.%s", schema, tablename
                )


# ── context manager ─────────────────────────────────────────────────


class Stage:
    """Yielded by `stage()`. `.write(df)` appends a sub-batch to the
    staging table; context exit flushes."""

    def __init__(
        self,
        conn: psycopg.Connection,
        model: "Model",
        *,
        run_id: UUID,
        since: datetime,
        until: datetime,
    ) -> None:
        self._conn = conn
        self._model = model
        self._run_id = run_id
        self._since = since
        self._until = until
        self._rows_written = 0

    def write(self, df: pl.DataFrame) -> None:
        copy_to_staging(self._conn, self._model, self._run_id, df)
        self._rows_written += len(df)

    @property
    def rows_written(self) -> int:
        return self._rows_written


@contextmanager
def stage(
    conn: psycopg.Connection,
    model: "Model",
    *,
    since: datetime,
    until: datetime,
) -> Iterator[Stage]:
    """Stream sub-batches into staging, then atomically flush to target.

    Usage inside an `@state`-wrapped execute:

        with stage(conn, model, since=since, until=until) as s:
            for chunk in source:
                s.write(chunk)

    The model is expected to have `_state_run_id` already stashed (the
    user's pipeline setup mints one per invocation). On a clean exit
    the flush sets `model._state_applied_via_staging = (since, until)`
    so `@state` skips its own (redundant) mark_applied.

    On exception inside the with-block the staging table is left in
    place for `gc_orphan_staging_tables()` to clean up next run. The
    state row stays pending; the interval reruns."""
    _assert_supported(model)

    run_id = getattr(model, "_state_run_id", None)
    if run_id is None:
        raise ValueError(
            f"stage() requires model._state_run_id to be set — normally "
            f"the pipeline mints one per invocation. Got None on "
            f"{model.target.full_name!r}."
        )

    from bollhav.model.staging import StagingMode

    # REUSED mode: track whether this interval is the first one for
    # this pipeline run before `ensure_staging_table` flips the flag.
    # If the table already existed (mutations.staging_table_created
    # was True), we need a TRUNCATE to clear the prior interval's
    # rows. PER_INTERVAL mode never needs TRUNCATE — every interval
    # gets a freshly-CREATEd empty table.
    mode = _staging_mode(model)
    needs_truncate = (
        mode is StagingMode.REUSED and model.target.mutations.staging_table_created
    )
    ensure_staging_table(conn, model, run_id)
    if needs_truncate:
        truncate_staging_table(conn, model, run_id)
    s = Stage(conn, model, run_id=run_id, since=since, until=until)
    try:
        yield s
    except Exception:
        logger.debug(
            "stage: exception in staged block for %s; leaving staging "
            "table %s.%s for GC, state row stays pending",
            model.target.full_name,
            _staging_schema(model),
            _staging_table(model, run_id),
        )
        raise

    flush_to_target(conn, model, run_id=run_id, since=since, until=until)
    model._state_applied_via_staging = (since, until)


__all__ = [
    "Stage",
    "stage",
    "ensure_staging_table",
    "copy_to_staging",
    "flush_to_target",
    "drop_staging_table",
    "gc_orphan_staging_tables",
]
