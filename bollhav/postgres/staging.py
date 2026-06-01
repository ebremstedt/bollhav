"""Postgres staging: stream sub-batches into a per-interval staging
table, then atomically apply the staged content to the target.

Use case: an interval produces more rows than fit in memory (or one
transaction), but state granularity stays at `(since, until)`.
Sub-batches COPY (or upsert) into staging; the context exit applies
staging → target and flips the state row in a single tx, so a crash
mid-stream cannot leave a half-applied interval marked applied.

Scope:
  * Target write modes: APPEND, UPSERT_NO_DELETE, RECREATE_PARTITION
  * Staging write modes: APPEND, UPSERT_NO_DELETE (chosen by
    `Staging.write_mode`)
  * State always co-locates with target — cross-DB state would break
    the atomic apply.

API:
    with stage(conn, model, since=since, until=until) as s:
        for chunk in source:
            s.write(chunk)
    # context exit: apply_atomically_to_target(...)
    #   INSERT/UPSERT/(DELETE+INSERT) staging -> target
    #   DROP staging (if mode=INTERVAL and not keep_after_apply)
    #   UPDATE state SET applied
    # all in one transaction.

Crash mid-stream: staging table is left in place, state row stays
pending. The next invocation's `gc_orphan_staging_tables(model)` GCs
it by name pattern.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Iterator
from uuid import UUID

import polars as pl
import psycopg
from psycopg import sql

from bollhav.model.staging import Staging
from bollhav.model.write_modes import WriteMode
from bollhav.postgres import state as pg_state

if TYPE_CHECKING:
    from bollhav.model.model import Model

logger = logging.getLogger(__name__)


@dataclass
class PostgresStaging(Staging):
    """Postgres-specific extension of `Staging`.

    Adds knobs only meaningful to Postgres; the neutral options
    (schema, table_prefix, mode, keep_after_apply) come from the base.

    `logged` — when False (default), staging tables are created
        UNLOGGED: writes skip the WAL, giving ~2-3x faster COPY
        throughput. A crash will truncate the staging table — fine,
        since the interval reruns from the top anyway. Set True for
        environments that mandate WAL on every write (compliance,
        replication policy).
    """

    logged: bool = False


def _logged(model: "Model") -> bool:
    """Resolve the Postgres `logged` knob from the staging config. The
    neutral `Staging` base doesn't carry it; only `PostgresStaging`
    does. A plain `Staging(...)` on a Postgres target therefore gets
    the default UNLOGGED behavior."""
    s = model.target.staging
    return isinstance(s, PostgresStaging) and s.logged


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
    if model.target.write_mode is WriteMode.VIEW:
        raise NotImplementedError(
            "stage() can't operate on a VIEW target — there's no row stream "
            "to land. VIEWs are CREATE OR REPLACE, not chunked writes."
        )
    if model.state is not None and model.state.dsn_env_var is not None:
        raise NotImplementedError(
            f"stage() currently requires state to share a DB with the target "
            f"(leave State() without dsn_env_var) — the atomic apply moves "
            f"data and flips the state row in one transaction, which can't "
            f"span databases. Got state.dsn_env_var="
            f"{model.state.dsn_env_var!r} on {model.target.full_name!r}."
        )


# ── DDL ─────────────────────────────────────────────────────────────


def ensure_staging_table_per_interval(
    conn: psycopg.Connection, model: "Model", run_id: UUID
) -> None:
    """Create the per-interval staging table in `StagingMode.INTERVAL`.

    Each interval gets its own freshly-CREATEd table; the prior
    interval's `flush` dropped it. This is NOT in the PRE action set
    because actions are one-shot per pipeline run while INTERVAL's
    CREATE is per-interval — the action runner would skip subsequent
    intervals.

    In `StagingMode.REUSED` the CREATE happens once via the
    `staging_table_created` PRE action; this function isn't called."""
    from bollhav.postgres.columns import PostgresColumn
    from bollhav.postgres.schema import _col_ddl

    schema = _staging_schema(model)
    table = _staging_table(model, run_id)

    table_keyword = "TABLE" if _logged(model) else "UNLOGGED TABLE"

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


# ── write to staging ─────────────────────────────────────────────────


def _staging_write_mode(model: "Model") -> WriteMode:
    """Resolve the staging-side write_mode. Defaults to APPEND when
    staging is set without an explicit choice."""
    if model.target.staging is None:
        return WriteMode.APPEND
    return model.target.staging.write_mode


def _copy_to_staging(
    conn: psycopg.Connection, model: "Model", run_id: UUID, df: pl.DataFrame
) -> None:
    """staging.write_mode = APPEND — COPY chunk into staging."""
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


def _upsert_to_staging(
    conn: psycopg.Connection, model: "Model", run_id: UUID, df: pl.DataFrame
) -> None:
    """staging.write_mode = UPSERT_NO_DELETE — COPY chunk into a
    `temp_<run_id>` temp table, then INSERT ... ON CONFLICT DO UPDATE
    against the staging table. Keeps staging deduped as data arrives."""
    from bollhav.postgres.columns import PostgresColumn

    staging_schema_id = sql.Identifier(_staging_schema(model))
    staging_table_id = sql.Identifier(_staging_table(model, run_id))
    pg_cols = [c for c in model.target.columns if isinstance(c, PostgresColumn)]
    unique_column_names = [c.name for c in model.target.unique_columns]
    col_names = sql.SQL(", ").join(sql.Identifier(c.name) for c in pg_cols)
    pk_cols = sql.SQL(", ").join(sql.Identifier(c) for c in unique_column_names)
    update_set = sql.SQL(", ").join(
        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c.name))
        for c in pg_cols
        if c.name not in unique_column_names
    )
    col_defs = sql.SQL(", ").join(
        sql.SQL("{name} {type}").format(
            name=sql.Identifier(c.name),
            type=sql.SQL(c.data_type.value),
        )
        for c in pg_cols
    )
    temp = sql.Identifier(f"temp_{str(run_id)[:8]}")

    with conn.transaction():
        conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(temp))
        conn.execute(
            sql.SQL("CREATE TEMP TABLE {} ({col_defs}) ON COMMIT DROP").format(
                temp, col_defs=col_defs
            )
        )
        with conn.cursor().copy(
            sql.SQL("COPY {} ({cols}) FROM STDIN").format(temp, cols=col_names)
        ) as copy:
            for row in df.rows():
                copy.write_row(row)
        conn.execute(
            sql.SQL(
                "INSERT INTO {staging_schema}.{staging_table} ({cols}) "
                "SELECT {cols} FROM {temp} "
                "ON CONFLICT ({pk_cols}) DO UPDATE SET {update_set}"
            ).format(
                staging_schema=staging_schema_id,
                staging_table=staging_table_id,
                cols=col_names,
                temp=temp,
                pk_cols=pk_cols,
                update_set=update_set,
            )
        )


def write_to_staging(
    conn: psycopg.Connection, model: "Model", run_id: UUID, df: pl.DataFrame
) -> None:
    """COPY or upsert a single sub-batch into the staging table, chosen
    by `staging.write_mode`. Each call commits its own small tx so
    staging absorbs partial progress without touching target."""
    if len(df) == 0:
        return

    df = df.select([col.name for col in model.target.columns])
    match _staging_write_mode(model):
        case WriteMode.APPEND:
            _copy_to_staging(conn, model, run_id, df)
        case WriteMode.UPSERT_NO_DELETE:
            _upsert_to_staging(conn, model, run_id, df)
        case _ as wm:  # pragma: no cover — guarded by Staging.__post_init__
            raise NotImplementedError(f"unsupported staging.write_mode {wm!r}")
    logger.debug(
        "stage: wrote %d rows to %s.%s (%s)",
        len(df),
        _staging_schema(model),
        _staging_table(model, run_id),
        _staging_write_mode(model).value,
    )


# ── apply staging -> target ──────────────────────────────────────────


def _apply_append(
    conn: psycopg.Connection,
    model: "Model",
    *,
    staging_schema: str,
    staging_table: str,
    target_schema: str,
    target_table: str,
) -> None:
    """target.write_mode = APPEND — INSERT INTO target SELECT FROM staging."""
    col_names = sql.SQL(", ").join(sql.Identifier(c.name) for c in model.target.columns)
    conn.execute(
        sql.SQL(
            "INSERT INTO {target_schema}.{target_table} ({cols}) "
            "SELECT {cols} FROM {staging_schema}.{staging_table}"
        ).format(
            target_schema=sql.Identifier(target_schema),
            target_table=sql.Identifier(target_table),
            staging_schema=sql.Identifier(staging_schema),
            staging_table=sql.Identifier(staging_table),
            cols=col_names,
        )
    )


def _apply_upsert(
    conn: psycopg.Connection,
    model: "Model",
    *,
    staging_schema: str,
    staging_table: str,
    target_schema: str,
    target_table: str,
) -> None:
    """target.write_mode = UPSERT_NO_DELETE — INSERT FROM staging
    with ON CONFLICT DO UPDATE."""
    unique_column_names = [c.name for c in model.target.unique_columns]
    col_names = sql.SQL(", ").join(sql.Identifier(c.name) for c in model.target.columns)
    pk_cols = sql.SQL(", ").join(sql.Identifier(c) for c in unique_column_names)
    update_set = sql.SQL(", ").join(
        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c.name))
        for c in model.target.columns
        if c.name not in unique_column_names
    )
    conn.execute(
        sql.SQL(
            "INSERT INTO {target_schema}.{target_table} ({cols}) "
            "SELECT {cols} FROM {staging_schema}.{staging_table} "
            "ON CONFLICT ({pk_cols}) DO UPDATE SET {update_set}"
        ).format(
            target_schema=sql.Identifier(target_schema),
            target_table=sql.Identifier(target_table),
            staging_schema=sql.Identifier(staging_schema),
            staging_table=sql.Identifier(staging_table),
            cols=col_names,
            pk_cols=pk_cols,
            update_set=update_set,
        )
    )


def _apply_recreate_partition(
    conn: psycopg.Connection,
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
    col_names = sql.SQL(", ").join(sql.Identifier(c.name) for c in model.target.columns)
    conn.execute(
        sql.SQL(
            "DELETE FROM {target_schema}.{target_table} "
            "WHERE {col} >= %s AND {col} < %s"
        ).format(
            target_schema=sql.Identifier(target_schema),
            target_table=sql.Identifier(target_table),
            col=sql.Identifier(partition_col_name),
        ),
        [since, until],
    )
    conn.execute(
        sql.SQL(
            "INSERT INTO {target_schema}.{target_table} ({cols}) "
            "SELECT {cols} FROM {staging_schema}.{staging_table}"
        ).format(
            target_schema=sql.Identifier(target_schema),
            target_table=sql.Identifier(target_table),
            staging_schema=sql.Identifier(staging_schema),
            staging_table=sql.Identifier(staging_table),
            cols=col_names,
        )
    )


def apply_atomically_to_target(
    conn: psycopg.Connection,
    model: "Model",
    *,
    run_id: UUID,
    since: datetime,
    until: datetime,
) -> None:
    """Apply the staged content to the target using whatever operation
    `target.write_mode` describes — all in one transaction.

    "Atomically" is the contract: the INSERT/UPSERT/DELETE+INSERT, the
    optional `DROP TABLE staging`, and (when state is enabled) the
    `UPDATE state SET applied` all commit together or roll back
    together. Concurrent readers never see a partial state of the
    target. A crash mid-apply rolls everything back; the staging table
    remains intact (GC'd on next run) and the interval reruns from the
    top — no half-applied rows, no out-of-sync state row.

    Without state (`model.state is None`) the apply still gives you
    memory-bounded chunked writes and atomic-per-interval finalization
    — just nothing to flip. Re-runs re-process the interval because
    there's no `applied` gate."""
    staging_schema = _staging_schema(model)
    staging_table = _staging_table(model, run_id)
    target_schema = model.target.schema.resolved
    target_table = model.target.name_resolved

    from bollhav.model.staging import StagingMode

    keep = model.target.staging is not None and model.target.staging.keep_after_apply
    mode = _staging_mode(model)
    drop_after_apply = mode is StagingMode.INTERVAL and not keep

    table_names = dict(
        staging_schema=staging_schema,
        staging_table=staging_table,
        target_schema=target_schema,
        target_table=target_table,
    )

    with conn.transaction():
        match model.target.write_mode:
            case WriteMode.APPEND:
                _apply_append(conn, model, **table_names)
            case WriteMode.UPSERT_NO_DELETE:
                _apply_upsert(conn, model, **table_names)
            case WriteMode.RECREATE_PARTITION:
                _apply_recreate_partition(
                    conn, model, **table_names, since=since, until=until
                )
            case _ as wm:  # pragma: no cover — guarded by _assert_supported
                raise NotImplementedError(f"unsupported target.write_mode {wm!r}")
        if drop_after_apply:
            conn.execute(
                sql.SQL("DROP TABLE {schema}.{table}").format(
                    schema=sql.Identifier(staging_schema),
                    table=sql.Identifier(staging_table),
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
        "stage: applied %s..%s for %s (%s)",
        since,
        until,
        model.target.full_name,
        model.target.write_mode.value,
    )


# ── orphan GC ───────────────────────────────────────────────────────


def gc_orphan_staging_tables(
    model: "Model", *, keep_run_id: UUID | None = None
) -> None:
    """Drop staging tables left behind by crashed runs.

    Matches `<prefix>%` in the staging schema (prefix from
    `Staging.table_prefix` or default). If `keep_run_id` is provided,
    the current run's staging table is preserved.

    No-op when `Staging.keep_after_apply=True` — the operator has
    declared they want kept tables to live; auto-GC would defeat that.
    Manual cleanup is on them."""
    if model.target.staging is not None and model.target.staging.keep_after_apply:
        logger.debug(
            "stage: GC skipped for %s — Staging.keep_after_apply=True",
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
        write_to_staging(self._conn, self._model, self._run_id, df)
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
    """Stream sub-batches into staging, then atomically apply to target.

    Usage inside an `@state`-wrapped execute:

        with stage(conn, model, since=since, until=until) as s:
            for chunk in source:
                s.write(chunk)

    The model is expected to have `_state_run_id` already stashed (the
    user's pipeline setup mints one per invocation). On a clean exit
    the apply sets `model._state_applied_via_staging = (since, until)`
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
    from bollhav.postgres.actions import run_pre_model_actions

    mode = _staging_mode(model)
    if mode is StagingMode.INTERVAL:
        # INTERVAL mode: fresh table per interval. The PRE
        # `staging_table_created` action skips itself in this mode
        # (one-shot wouldn't fit), so the CREATE happens here. We
        # still call run_pre_model_actions to ensure the schema-level PRE
        # actions (schema, staging schema) have fired.
        run_pre_model_actions(conn, model)
        ensure_staging_table_per_interval(conn, model, run_id)
    else:
        # REUSED mode: PRE actions create the staging schema and
        # table on the first interval. `run_pre_model_actions` is
        # idempotent (gated by `target._applied_model_actions`), so subsequent
        # interval calls short-circuit. The TRUNCATE then clears
        # any leftover from the previous interval. Harmless on the
        # first interval where the table was just CREATEd empty.
        run_pre_model_actions(conn, model)
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

    apply_atomically_to_target(conn, model, run_id=run_id, since=since, until=until)
    model._state_applied_via_staging = (since, until)


__all__ = [
    "PostgresStaging",
    "Stage",
    "stage",
    "ensure_staging_table_per_interval",
    "write_to_staging",
    "apply_atomically_to_target",
    "drop_staging_table",
    "truncate_staging_table",
    "gc_orphan_staging_tables",
]
