"""Postgres staging: land sub-batches in a per-interval staging table,
then atomically apply the staged content to the target.

Use case: an interval produces more rows than fit in memory (or one
transaction), but state granularity stays at `(since, until)`.
Sub-batches COPY (or upsert) into staging; one transaction then applies
staging → target so a crash mid-stream cannot leave a half-applied
interval marked applied.

Scope:
  * Target write modes: APPEND, UPSERT_NO_DELETE, RECREATE_PARTITION
  * Staging write modes: APPEND, UPSERT_NO_DELETE (chosen by
    `Staging.write_mode`)
  * State always co-locates with target — cross-DB state would break
    the atomic apply.

This module holds the staging primitives:
  * `write_to_staging` — COPY / upsert one chunk into the staging table.
  * `apply_atomically_to_target` — merge staging → target in one tx.
  * `drop_staging_table` — tear the staging table down.

The staging table's *lifecycle* is owned by the framework, not here:
`@execute_lifecycle` creates the table, then merges and drops it around
the user's execute, and `write()` lands the rows in between — all via
`PostgresData`, which delegates to these primitives. Both sides key the
table on `model.run_id`.

Crash mid-stream: the staging table is left in place, state row stays
pending. `PostgresData.gc_orphan_staging_tables` reaps it next run.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

import polars as pl
import psycopg
from psycopg import sql

from bollhav.model.staging import Staging
from bollhav.model.write_modes import WriteMode

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
    """Resolve the staging schema. `Staging.schema` overrides; default is
    `z_<target_schema>` — co-located with the target data (per-model), NOT
    the central `z_bollhav_state` (state/error tables live there now).
    `State.schema_prefix` still tunes the `z_` prefix."""
    if model.target.staging is not None and model.target.staging.schema:
        return model.target.staging.schema
    prefix = (
        model.state.schema_prefix
        if model.state is not None and model.state.schema_prefix is not None
        else "z_"
    )
    return f"{prefix}{model.target.schema_resolved}"


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


# ── DDL ─────────────────────────────────────────────────────────────


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
        "wrote %d rows to staging table (%s)",
        len(df),
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
    drop_after_apply: bool | None = None,
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

    By default the staging table is dropped here (unless
    `Staging.keep_after_apply`). Pass `drop_after_apply=False` to keep the
    teardown a separate step — the lifecycle hook does this so the merge
    and the drop are distinct phases (it drops via PostgresData after).

    Without state (`model.state is None`) the apply still gives you
    memory-bounded chunked writes and atomic-per-interval finalization
    — just nothing to flip. Re-runs re-process the interval because
    there's no `applied` gate."""
    staging_schema = _staging_schema(model)
    staging_table = _staging_table(model, run_id)
    target_schema = model.target.schema_resolved
    target_table = model.target.name_resolved

    if drop_after_apply is None:
        keep = (
            model.target.staging is not None and model.target.staging.keep_after_apply
        )
        drop_after_apply = not keep

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
        # State is flipped to `applied` separately, by the interval
        # lifecycle's `mark_applied` after this returns — not inside the
        # data-move transaction. The data write commits here; the state
        # flip follows (non-atomic, data → state).
    logger.debug(
        "moved data from staging to target (%s)",
        model.target.write_mode.value,
    )


# ── orphan GC ───────────────────────────────────────────────────────


# Orphan staging-table GC lives on `PostgresData.gc_orphan_staging_tables`
# (target-side asset DDL, model-scoped) — the lifecycle hook calls it there.


__all__ = [
    "PostgresStaging",
    "write_to_staging",
    "apply_atomically_to_target",
    "drop_staging_table",
]
