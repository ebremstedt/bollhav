"""MSSQL staging: stream sub-batches into a per-interval staging table,
then atomically apply the staged content to the target.

Use case: an interval produces more rows than fit in memory (or one
transaction), but the unit of recovery stays at `(since, until)`.
Sub-batches bulk-insert (or MERGE) into a staging table; the context
exit applies staging → target and drops staging in one transaction, so
a crash mid-stream cannot leave a half-applied interval visible in the
target.

Scope:
  * Target write modes: APPEND, UPSERT_NO_DELETE, RECREATE_PARTITION
  * Staging write modes: APPEND, UPSERT_NO_DELETE (chosen by
    `Staging.write_mode`)
  * `model.state is None` only — state coordination is a future
    addition; until then, set State() only on direct-write models.

API:
    with stage(conn, model, since=since, until=until) as s:
        for chunk in source:
            s.write(chunk)
    # context exit: apply_atomically_to_target(...)
    #   INSERT/MERGE/(DELETE+INSERT) staging -> target
    #   DROP staging (unless keep_after_apply)
    #   commit
    # all in one transaction.

Crash mid-stream: staging table is left in place. The next
invocation's `gc_orphan_staging_tables(model)` cleans it up by name
pattern.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Iterator
from uuid import UUID

import polars as pl
import pyodbc

from bollhav.model.staging import Staging
from bollhav.model.write_modes import WriteMode
from bollhav.mssql.columns import MssqlColumn
from bollhav.mssql.modes import _bulk_insert
from bollhav.mssql.schema import _b, _col_ddl

if TYPE_CHECKING:
    from bollhav.model.model import Model

logger = logging.getLogger(__name__)


# ── MSSQL-specific staging config ────────────────────────────────────


@dataclass
class MssqlStaging(Staging):
    """MSSQL-specific extension of `Staging`.

    Currently carries no MSSQL-only fields — kept as a placeholder for
    upcoming knobs like `WITH (DURABILITY = SCHEMA_ONLY)` for
    memory-optimized tables, or `WITH (DATA_COMPRESSION = ...)`.
    """

    pass


# ── naming ──────────────────────────────────────────────────────────


def _staging_schema(model: "Model") -> str:
    """Resolve the staging schema. `Staging.schema` overrides; default
    is `z_<target_schema>` so bollhav-owned tables stay out of the
    user's schemas."""
    if model.target.staging is not None and model.target.staging.schema:
        return model.target.staging.schema
    return f"z_{model.target.schema.resolved}"


def _staging_table_prefix(model: "Model") -> str:
    """Resolve the staging-table name prefix. `Staging.table_prefix`
    overrides; default is `<target_name>_staging_`."""
    if model.target.staging is not None and model.target.staging.table_prefix:
        return model.target.staging.table_prefix
    return f"{model.target.name}_staging_"


def _staging_table(model: "Model", run_id: UUID) -> str:
    """Per-interval staging table name. First 8 hex chars of run_id
    disambiguate within a model."""
    return f"{_staging_table_prefix(model)}{str(run_id)[:8]}"


def _assert_supported(model: "Model") -> None:
    if model.batching is None:
        raise ValueError(
            f"stage() requires model.batching to be set on {model.target.full_name!r}"
        )
    if model.target.write_mode is WriteMode.VIEW:
        raise NotImplementedError(
            "stage() can't operate on a VIEW target — there's no row stream "
            "to land. VIEWs are CREATE OR ALTER, not chunked writes."
        )
    if model.state is not None:
        raise NotImplementedError(
            f"stage() does not yet support state coordination on "
            f"{model.target.full_name!r}. Leave State() unset — staging "
            f"still gives you chunked atomic apply; intervals rerun on "
            f"crash because there's no applied-gate."
        )


# ── DDL ─────────────────────────────────────────────────────────────


def ensure_staging_schema(conn: pyodbc.Connection, model: "Model") -> None:
    """Idempotent CREATE SCHEMA for the staging schema. MSSQL requires
    CREATE SCHEMA to be the first statement in a batch, so it runs in
    its own dynamic-SQL block via EXEC."""
    schema = _staging_schema(model)
    cursor = conn.cursor()
    cursor.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = ?) "
        "BEGIN DECLARE @s NVARCHAR(MAX) = N'CREATE SCHEMA ' + QUOTENAME(?); EXEC(@s) END",
        schema,
        schema,
    )
    cursor.commit()


def ensure_staging_table(conn: pyodbc.Connection, model: "Model", run_id: UUID) -> None:
    """Create the per-interval staging table if missing. Each interval's
    apply drops it (unless `keep_after_apply`), so the next interval's
    call CREATEs a fresh one; the IF NOT EXISTS guard just makes it
    crash-safe to re-enter.

    Staging tables are always regular (fully logged) tables. Use
    `MssqlStaging` rather than the neutral `Staging` if/when MSSQL-
    specific knobs (compression, memory-optimized) are needed."""
    schema = _staging_schema(model)
    table = _staging_table(model, run_id)
    mssql_cols = [c for c in model.target.columns if isinstance(c, MssqlColumn)]
    col_defs = ",\n".join(_col_ddl(c) for c in mssql_cols)

    cursor = conn.cursor()
    cursor.execute(
        f"IF NOT EXISTS ("
        f"    SELECT 1 FROM INFORMATION_SCHEMA.TABLES"
        f"    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
        f") CREATE TABLE {_b(schema)}.{_b(table)} (\n{col_defs}\n)",
        schema,
        table,
    )
    cursor.commit()


def drop_staging_table(conn: pyodbc.Connection, model: "Model", run_id: UUID) -> None:
    schema = _staging_schema(model)
    table = _staging_table(model, run_id)
    cursor = conn.cursor()
    cursor.execute(
        f"IF OBJECT_ID(?, 'U') IS NOT NULL DROP TABLE {_b(schema)}.{_b(table)}",
        f"{schema}.{table}",
    )
    cursor.commit()


# ── write to staging ─────────────────────────────────────────────────


def _staging_write_mode(model: "Model") -> WriteMode:
    """Resolve the staging-side write_mode. Defaults to APPEND when
    staging is set without an explicit choice."""
    if model.target.staging is None:
        return WriteMode.APPEND
    return model.target.staging.write_mode


def _append_to_staging(
    cursor: pyodbc.Cursor, model: "Model", run_id: UUID, df: pl.DataFrame
) -> None:
    """Bulk-insert a chunk into the staging table — no dedup, dupes
    accumulate. Cheap; the cost-per-row is just `_bulk_insert`."""
    mssql_cols = [c for c in model.target.columns if isinstance(c, MssqlColumn)]
    _bulk_insert(
        cursor,
        f"{_b(_staging_schema(model))}.{_b(_staging_table(model, run_id))}",
        [c.name for c in mssql_cols],
        df,
        columns=mssql_cols,
        fast=True,
    )


def _merge_into_staging(
    cursor: pyodbc.Cursor, model: "Model", run_id: UUID, df: pl.DataFrame
) -> None:
    """MERGE a chunk into the staging table using `target.unique_columns`
    as the join key — keeps staging deduped as chunks arrive.

    The merge keys come from the target's unique constraint (same ones
    used by the eventual target-side MERGE), so each row in staging
    represents the latest version seen for its key. Internally
    delegates to `_merge_via_temp`, which uses a per-call `#tmp` table."""
    from bollhav.mssql.modes import _merge_via_temp

    staging_table = f"{_b(_staging_schema(model))}.{_b(_staging_table(model, run_id))}"
    _merge_via_temp(cursor, staging_table, model, df, fast_executemany=True)


def write_to_staging(
    conn: pyodbc.Connection, model: "Model", run_id: UUID, df: pl.DataFrame
) -> None:
    """Append or merge a single sub-batch into the staging table,
    chosen by `staging.write_mode`. Each call commits its own small
    tx so staging absorbs partial progress without touching target."""
    if len(df) == 0:
        return

    df = df.select([col.name for col in model.target.columns])
    cursor = conn.cursor()
    match _staging_write_mode(model):
        case WriteMode.APPEND:
            _append_to_staging(cursor, model, run_id, df)
        case WriteMode.UPSERT_NO_DELETE:
            _merge_into_staging(cursor, model, run_id, df)
        case _ as wm:  # pragma: no cover — guarded by Staging.__post_init__
            raise NotImplementedError(f"unsupported staging.write_mode {wm!r}")
    cursor.commit()
    logger.debug(
        "stage: wrote %d rows to %s.%s (%s)",
        len(df),
        _staging_schema(model),
        _staging_table(model, run_id),
        _staging_write_mode(model).value,
    )


# ── apply staging -> target ──────────────────────────────────────────


def _apply_append(
    cursor: pyodbc.Cursor,
    model: "Model",
    *,
    staging_schema: str,
    staging_table: str,
    target_schema: str,
    target_table: str,
) -> None:
    """target.write_mode = APPEND — INSERT INTO target SELECT FROM staging."""
    mssql_cols = [c for c in model.target.columns if isinstance(c, MssqlColumn)]
    col_list = ", ".join(_b(c.name) for c in mssql_cols)
    cursor.execute(
        f"INSERT INTO {_b(target_schema)}.{_b(target_table)} ({col_list}) "
        f"SELECT {col_list} FROM {_b(staging_schema)}.{_b(staging_table)}"
    )


def _apply_upsert(
    cursor: pyodbc.Cursor,
    model: "Model",
    *,
    staging_schema: str,
    staging_table: str,
    target_schema: str,
    target_table: str,
) -> None:
    """target.write_mode = UPSERT_NO_DELETE — MERGE target USING staging.

    Staging is already the source table — no #tmp hop needed."""
    mssql_cols = [c for c in model.target.columns if isinstance(c, MssqlColumn)]
    all_col_names = [c.name for c in mssql_cols]
    unique_col_names = [c.name for c in model.target.merge_key_columns]
    non_unique_col_names = [c for c in all_col_names if c not in unique_col_names]

    on_clause = " AND ".join(
        f"target.{_b(c)} = source.{_b(c)}" for c in unique_col_names
    )
    insert_cols = ", ".join(_b(c) for c in all_col_names)
    insert_vals = ", ".join(f"source.{_b(c)}" for c in all_col_names)

    if non_unique_col_names:
        update_set = ", ".join(
            f"target.{_b(c)} = source.{_b(c)}" for c in non_unique_col_names
        )
        matched_clause = f"WHEN MATCHED THEN UPDATE SET {update_set}"
    else:
        matched_clause = ""

    cursor.execute(
        f"MERGE INTO {_b(target_schema)}.{_b(target_table)} AS target "
        f"USING {_b(staging_schema)}.{_b(staging_table)} AS source ON {on_clause} "
        f"{matched_clause} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
    )


def _apply_recreate_partition(
    cursor: pyodbc.Cursor,
    model: "Model",
    *,
    staging_schema: str,
    staging_table: str,
    target_schema: str,
    target_table: str,
    since: datetime,
    until: datetime,
) -> None:
    """target.write_mode = RECREATE_PARTITION — DELETE window, then
    INSERT FROM staging. Same transaction, so concurrent readers never
    see a gap (only the new contents of the window)."""
    if since.tzinfo is None or until.tzinfo is None:
        raise ValueError("RECREATE_PARTITION requires since/until to be UTC-aware")
    partition_col_name = model.target.partitioned_by
    if partition_col_name is None:
        raise ValueError("RECREATE_PARTITION requires target.partitioned_by to be set")

    mssql_cols = [c for c in model.target.columns if isinstance(c, MssqlColumn)]
    col_list = ", ".join(_b(c.name) for c in mssql_cols)

    cursor.execute(
        f"DELETE FROM {_b(target_schema)}.{_b(target_table)} "
        f"WHERE {_b(partition_col_name)} >= ? AND {_b(partition_col_name)} < ?",
        since,
        until,
    )
    cursor.execute(
        f"INSERT INTO {_b(target_schema)}.{_b(target_table)} ({col_list}) "
        f"SELECT {col_list} FROM {_b(staging_schema)}.{_b(staging_table)}"
    )


def apply_atomically_to_target(
    conn: pyodbc.Connection,
    model: "Model",
    *,
    run_id: UUID,
    since: datetime,
    until: datetime,
) -> None:
    """Apply the staged content to the target using whatever operation
    `target.write_mode` describes — all in one transaction.

    "Atomically" is the contract: the INSERT/MERGE/DELETE+INSERT, the
    optional `DROP TABLE staging`, and (when state coordination lands)
    the `UPDATE state SET applied` all commit together or roll back
    together. Concurrent readers never see a partial state of the
    target. A crash mid-apply rolls everything back; the staging table
    remains intact (GC'd on next run) and the interval reruns from the
    top — no half-applied rows, no out-of-sync state row.

    On success: data is in target and the staging table is dropped
    (unless `keep_after_apply`). On failure: rollback. No partial write
    visible in target."""
    staging_schema = _staging_schema(model)
    staging_table = _staging_table(model, run_id)
    target_schema = model.target.schema.resolved
    target_table = model.target.name_resolved

    keep = model.target.staging is not None and model.target.staging.keep_after_apply
    drop_after_apply = not keep

    table_names = dict(
        staging_schema=staging_schema,
        staging_table=staging_table,
        target_schema=target_schema,
        target_table=target_table,
    )

    cursor = conn.cursor()
    try:
        match model.target.write_mode:
            case WriteMode.APPEND:
                _apply_append(cursor, model, **table_names)
            case WriteMode.UPSERT_NO_DELETE:
                _apply_upsert(cursor, model, **table_names)
            case WriteMode.RECREATE_PARTITION:
                _apply_recreate_partition(
                    cursor, model, **table_names, since=since, until=until
                )
            case _ as wm:  # pragma: no cover — guarded by _assert_supported
                raise NotImplementedError(f"unsupported target.write_mode {wm!r}")
        if drop_after_apply:
            cursor.execute(f"DROP TABLE {_b(staging_schema)}.{_b(staging_table)}")
        cursor.commit()
    except Exception:
        cursor.rollback()
        raise
    logger.debug(
        "stage: applied %s (%s -> %s.%s via %s)",
        model.target.full_name,
        f"{staging_schema}.{staging_table}",
        target_schema,
        target_table,
        model.target.write_mode.value,
    )


# ── orphan GC ───────────────────────────────────────────────────────


def gc_orphan_staging_tables(
    conn: pyodbc.Connection,
    model: "Model",
    *,
    keep_run_id: UUID | None = None,
) -> None:
    """Drop staging tables left behind by crashed runs.

    Matches `<prefix>%` in the staging schema. If `keep_run_id` is
    provided, the current run's staging table is preserved.

    No-op when `Staging.keep_after_apply=True` — the operator has
    declared they want kept tables to live; auto-GC would defeat that.
    Manual cleanup is on them.

    Connection management is the caller's responsibility — pass in an
    open `pyodbc.Connection`."""
    if model.target.staging is not None and model.target.staging.keep_after_apply:
        logger.debug(
            "stage: GC skipped for %s — Staging.keep_after_apply=True",
            model.target.full_name,
        )
        return

    schema = _staging_schema(model)
    prefix = _staging_table_prefix(model)
    keep = f"{prefix}{str(keep_run_id)[:8]}" if keep_run_id is not None else None

    cursor = conn.cursor()
    rows = cursor.execute(
        "SELECT t.name FROM sys.tables t "
        "JOIN sys.schemas s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name LIKE ?",
        schema,
        f"{prefix}%",
    ).fetchall()

    for (tablename,) in rows:
        if keep is not None and tablename == keep:
            continue
        cursor.execute(f"DROP TABLE {_b(schema)}.{_b(tablename)}")
        logger.debug("stage: gc'd orphan staging table %s.%s", schema, tablename)
    cursor.commit()


# ── context manager ─────────────────────────────────────────────────


class Stage:
    """Yielded by `stage()`. `.write(df)` adds a sub-batch to the
    staging table; context exit applies staging to target atomically."""

    def __init__(
        self,
        conn: pyodbc.Connection,
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
        write_to_staging(self._conn, self._model, self._run_id, df)
        self._rows_written += len(df)

    @property
    def rows_written(self) -> int:
        return self._rows_written


@contextmanager
def stage(
    conn: pyodbc.Connection,
    model: "Model",
    *,
    since: datetime,
    until: datetime,
) -> Iterator[Stage]:
    """Stream sub-batches into staging, then atomically apply to target.

    Usage:

        with stage(conn, model, since=since, until=until) as s:
            for chunk in source:
                s.write(chunk)

    The model is expected to have `_state_run_id` already stashed (the
    user's pipeline setup mints one per invocation, regardless of
    whether state coordination is enabled — the run_id is the staging
    table disambiguator).

    On exception inside the with-block the staging table is left in
    place for `gc_orphan_staging_tables()` to clean up on the next
    run. The interval reruns from the top."""
    _assert_supported(model)

    run_id = getattr(model, "_state_run_id", None)
    if run_id is None:
        raise ValueError(
            f"stage() requires model._state_run_id to be set — normally "
            f"the pipeline mints one per invocation. Got None on "
            f"{model.target.full_name!r}."
        )

    ensure_staging_schema(conn, model)
    ensure_staging_table(conn, model, run_id)
    s = Stage(conn, model, run_id=run_id, since=since, until=until)
    try:
        yield s
    except Exception:
        logger.debug(
            "stage: exception in staged block for %s; leaving staging "
            "table %s.%s for GC",
            model.target.full_name,
            _staging_schema(model),
            _staging_table(model, run_id),
        )
        raise

    apply_atomically_to_target(conn, model, run_id=run_id, since=since, until=until)


__all__ = [
    "MssqlStaging",
    "Stage",
    "stage",
    "ensure_staging_schema",
    "ensure_staging_table",
    "write_to_staging",
    "apply_atomically_to_target",
    "drop_staging_table",
    "gc_orphan_staging_tables",
]
