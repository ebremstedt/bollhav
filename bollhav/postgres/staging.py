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
from bollhav.postgres.messages.error import (
    UnsupportedStagingWriteModeError,
    RecreatePartitionRequiresAwareWindowError,
    RecreatePartitionRequiresPartitionedByError,
    StagedRecreatePartitionRequiresWindowError,
    UnsupportedTargetWriteModeError,
)

if TYPE_CHECKING:
    from bollhav.model.model import Model

logger = logging.getLogger(__name__)


@dataclass
class PostgresStaging(Staging):
    """Postgres-specific extension of `Staging`

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

# Staging tables now live in the one central bollhav schema (`z_bollhav`, or
# `z_bollhav_<suffix>` per dev branch) alongside state/library/errors — one
# schema to inspect or drop. Because that schema is shared across every model,
# the staging-table name carries a per-model identity (devowelled catalog/schema
# context + table + full-name digest), so two models with the same target name
# never collide and GC can scope by prefix. Budget leaves room for the digest +
# `_stg_` + 8-hex run_id under Postgres' 63-char identifier limit.
_STAGING_SLUG_CAP = 30


def _staging_schema(model: "Model") -> str:
    """Resolve the staging schema. `Staging.schema` overrides; default is the
    central bollhav schema (`z_bollhav`, or `z_bollhav_<suffix>` under a
    `SCHEMA_SUFFIX` run — same resolution as state/library/errors), so all
    bollhav-owned tables are centralized in one schema."""
    if model.target.staging is not None and model.target.staging.schema:
        return model.target.staging.schema
    from bollhav.model.target import resolve_schema_name
    from bollhav.postgres.state import LIBRARY_SCHEMA

    return resolve_schema_name(
        LIBRARY_SCHEMA,
        model.target.schema_suffix,
        model.target.schema_suffix_appendix,
    )


def _staging_stem(full_name: str) -> str:
    """Per-model staging stem — devowelled catalog/schema context + table name +
    full-name digest. Mirrors `state_table_name` so identity rides in the
    digest (collision-safe over the FULL name); the readable slug is capped to
    leave room for the `_stg_<run_id8>` per-interval tail under 63 chars."""
    from bollhav.postgres.state import _CONTEXT_CAP, _devowel, _name_digest

    digest = _name_digest(full_name)
    parts = full_name.lower().replace("-", "_").split(".")
    table = parts[-1]
    context = "_".join(_devowel(p) for p in parts[:-1])[:_CONTEXT_CAP]
    table_budget = (
        _STAGING_SLUG_CAP - len(context) - 1 if context else _STAGING_SLUG_CAP
    )
    table = table[:table_budget]
    slug = f"{context}_{table}" if context else table
    return f"{slug}_{digest}"


def _staging_table_prefix(model: "Model") -> str:
    """Resolve the staging-table name prefix. `Staging.table_prefix` overrides;
    default is `<per-model stem>_stg_`. The stem makes the prefix unique per
    model, so GC (`LIKE '<prefix>%'`) scopes to one model in the shared
    central schema."""
    if model.target.staging is not None and model.target.staging.table_prefix:
        return model.target.staging.table_prefix
    return f"{_staging_stem(model.target.full_name)}_stg_"


def _staging_table(model: "Model", run_id: UUID) -> str:
    """Per-interval staging table name. First 8 hex chars of run_id
    disambiguate within a model. Full run_id is on the state row if
    needed."""
    return f"{_staging_table_prefix(model)}{str(run_id)[:8]}"


# ── DDL ─────────────────────────────────────────────────────────────


def drop_staging_table(conn: psycopg.Connection, model: "Model", run_id: UUID) -> None:
    schema = _staging_schema(model)
    table = _staging_table(model, run_id)
    conn.execute(
        sql.SQL("DROP TABLE IF EXISTS {schema}.{table}").format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
        )
    )
    logger.debug("dropped staging table %s.%s", schema, table)


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
    key_column_names = [c.name for c in model.target.merge_key_columns]
    col_names = sql.SQL(", ").join(sql.Identifier(c.name) for c in pg_cols)
    pk_cols = sql.SQL(", ").join(sql.Identifier(c) for c in key_column_names)
    update_set = sql.SQL(", ").join(
        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c.name))
        for c in pg_cols
        if c.name not in key_column_names
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
            raise UnsupportedStagingWriteModeError(wm)
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
    key_column_names = [c.name for c in model.target.merge_key_columns]
    col_names = sql.SQL(", ").join(sql.Identifier(c.name) for c in model.target.columns)
    pk_cols = sql.SQL(", ").join(sql.Identifier(c) for c in key_column_names)
    update_set = sql.SQL(", ").join(
        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c.name))
        for c in model.target.columns
        if c.name not in key_column_names
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
        raise RecreatePartitionRequiresAwareWindowError()
    partition_col_name = model.target.partitioned_by
    if partition_col_name is None:
        raise RecreatePartitionRequiresPartitionedByError()
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
    since: datetime | None = None,
    until: datetime | None = None,
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
                if since is None or until is None:
                    raise StagedRecreatePartitionRequiresWindowError()
                _apply_recreate_partition(
                    conn, model, **table_names, since=since, until=until
                )
            case _ as wm:  # pragma: no cover — guarded by _assert_supported
                raise UnsupportedTargetWriteModeError(wm)
        if drop_after_apply:
            conn.execute(
                sql.SQL("DROP TABLE {schema}.{table}").format(
                    schema=sql.Identifier(staging_schema),
                    table=sql.Identifier(staging_table),
                )
            )
            logger.debug(
                "dropped staging table %s.%s", staging_schema, staging_table
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
