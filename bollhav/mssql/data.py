"""Target-side (data) MSSQL backend for one model.

`MssqlData` is the MSSQL counterpart to `PostgresData`: it runs the
target-DB asset DDL and drives the per-interval staging lifecycle for a
model. Each method is one discrete operation; the lifecycle hook decides
which to call and in what order — the same hook that drives
`PostgresData`, so the two backends share one orchestration.

MSSQL has no state coordination (`bollhav.mssql.staging._assert_supported`
rejects `State()`), so only the stateless paths of the lifecycle ever
reach this class: asset DDL + staged/direct writes, never the state
machine.

Most methods are thin wrappers over the already-tested free functions in
`bollhav.mssql.schema` and `bollhav.mssql.staging`; the discrete
table-shape steps (create / recreate / truncate / unique) are issued
here so they compose the same way the Postgres ones do.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

import pyodbc

from bollhav.mssql.columns import MssqlColumn
from bollhav.mssql.schema import (
    _b,
    _col_ddl,
    ensure_indexes,
    ensure_primary_key,
    ensure_schema,
)
from bollhav.mssql.staging import (
    apply_atomically_to_target,
    drop_staging_table,
    ensure_staging_schema,
    ensure_staging_table,
    gc_orphan_staging_tables,
    write_to_staging,
)

if TYPE_CHECKING:
    import polars as pl

    from bollhav.model.intervals import TZInterval
    from bollhav.model.model import Model

logger = logging.getLogger(__name__)


class MssqlData:
    """Target-side asset DDL + staging lifecycle for one MSSQL model.

    Construct with the model and the caller-owned data connection
    (a `pyodbc.Connection`, opened in `main()` and threaded through the
    lifecycle hooks). Each method runs one discrete piece of setup; the
    lifecycle hook calls the ones it needs, in order."""

    def __init__(self, model: "Model", conn: pyodbc.Connection) -> None:
        if model.state is not None:
            raise NotImplementedError(
                f"MSSQL has no state coordination — remove State() from "
                f"{model.target.full_name!r}. Staging still works without it: "
                f"chunked atomic apply, intervals just rerun on crash (there's "
                f"no applied-gate)."
            )
        self.model = model
        self.conn = conn

    # ── asset DDL ─────────────────────────────────────────────────────

    def create_schema(self) -> None:
        ensure_schema(self.conn, self.model.target.schema.resolved)

    def create_or_replace_view(self) -> None:
        """`CREATE OR ALTER VIEW` from the model's source query — the
        target-side asset for a VIEW model. Called instead of the table
        DDL when `model.is_view`."""
        from bollhav.mssql.modes import create_replace_view

        create_replace_view(conn=self.conn, model=self.model)

    def recreate_table(self) -> None:
        target = self.model.target
        schema, table = target.schema.resolved, target.name_resolved
        cursor = self.conn.cursor()
        cursor.execute(
            f"IF OBJECT_ID(?, 'U') IS NOT NULL DROP TABLE {_b(schema)}.{_b(table)}",
            f"{schema}.{table}",
        )
        cursor.commit()

    def create_table(self) -> None:
        """`CREATE TABLE IF NOT EXISTS` for the target, then its PRIMARY
        KEY and indexes. Non-destructive and idempotent — recreate /
        truncate are separate, lifecycle-ordered steps."""
        target = self.model.target
        schema, table = target.schema.resolved, target.name_resolved
        mssql_cols = [c for c in target.columns if isinstance(c, MssqlColumn)]
        col_defs = ",\n".join(_col_ddl(c) for c in mssql_cols)

        cursor = self.conn.cursor()
        cursor.execute(
            f"IF NOT EXISTS ("
            f"    SELECT 1 FROM INFORMATION_SCHEMA.TABLES"
            f"    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
            f") CREATE TABLE {_b(schema)}.{_b(table)} (\n{col_defs}\n)",
            schema,
            table,
        )
        cursor.commit()
        ensure_primary_key(conn=self.conn, model=self.model)
        ensure_indexes(conn=self.conn, model=self.model)

    def truncate_table(self) -> None:
        target = self.model.target
        schema, table = target.schema.resolved, target.name_resolved
        cursor = self.conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {_b(schema)}.{_b(table)}")
        cursor.commit()

    def create_indexes(self) -> None:
        ensure_indexes(conn=self.conn, model=self.model)

    def add_unique_constraint(self) -> None:
        """Add a `<table>_uq` UNIQUE constraint over `target.unique_columns`
        if missing. Idempotent — the IF NOT EXISTS guard makes re-runs a
        no-op."""
        target = self.model.target
        if not target.unique_columns:
            return
        schema, table = target.schema.resolved, target.name_resolved
        constraint_name = f"{table}_uq"
        cols = ", ".join(_b(c.name) for c in target.unique_columns)
        cursor = self.conn.cursor()
        cursor.execute(
            f"IF NOT EXISTS ("
            f"    SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
            f"    WHERE CONSTRAINT_NAME = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ?"
            f") ALTER TABLE {_b(schema)}.{_b(table)}"
            f"    ADD CONSTRAINT {_b(constraint_name)} UNIQUE ({cols})",
            constraint_name,
            schema,
            table,
        )
        cursor.commit()

    # ── staging lifecycle ─────────────────────────────────────────────
    #
    # The per-interval staging flow as discrete steps the lifecycle hook
    # drives in order: create the table, write rows into it, apply it to
    # the target, then tear it down. The heavy write-mode SQL lives in
    # `bollhav.mssql.staging`; these are the model-scoped entry points.

    def create_staging_schema(self) -> None:
        ensure_staging_schema(self.conn, self.model)

    def gc_orphan_staging_tables(self, *, keep_run_id: UUID | None = None) -> None:
        gc_orphan_staging_tables(self.conn, self.model, keep_run_id=keep_run_id)

    def create_staging_table(self, run_id: UUID) -> None:
        ensure_staging_table(self.conn, self.model, run_id)

    def write_to_staging(self, run_id: UUID, df: "pl.DataFrame") -> None:
        write_to_staging(self.conn, self.model, run_id, df)

    def apply_staging_to_target(self, run_id: UUID, interval: "TZInterval") -> None:
        """Apply this run's staging table to the target via
        `target.write_mode`, atomically. The MSSQL apply also drops the
        staging table in the same transaction (unless `keep_after_apply`),
        so the lifecycle's follow-up `drop_staging_table` is a safe
        no-op."""
        apply_atomically_to_target(
            self.conn,
            self.model,
            run_id=run_id,
            since=interval.since,
            until=interval.until,
        )

    def drop_staging_table(self, run_id: UUID) -> None:
        """Tear down this run's staging table. No-op when
        `Staging.keep_after_apply` (the operator wants kept tables to
        live; manual cleanup is on them), and already a no-op when the
        atomic apply dropped it."""
        staging = self.model.target.staging
        if staging is not None and staging.keep_after_apply:
            logger.debug(
                "teardown skipped for %s — Staging.keep_after_apply=True",
                self.model.target.full_name,
            )
            return
        drop_staging_table(self.conn, self.model, run_id)


__all__ = ["MssqlData"]
