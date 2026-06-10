from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

import pyodbc

from bollhav.mssql.columns import MssqlColumn
from bollhav.mssql.schema import (
    _bracket_quote,
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
    cleanup_orphaned_staging_tables,
    write_to_staging,
)

if TYPE_CHECKING:
    import polars as pl

    from bollhav.model.intervals import TZInterval
    from bollhav.model.model import Model

logger = logging.getLogger(__name__)


class MssqlData:
    """Target-side asset DDL + staging lifecycle for MSSQL models"""

    def __init__(self, model: "Model", conn: pyodbc.Connection) -> None:
        self.model = model
        self.conn = conn

    # ── asset DDL ─────────────────────────────────────────────────────

    def create_schema(self) -> None:
        ensure_schema(self.conn, self.model.target.schema_resolved)

    def create_or_replace_view(self) -> None:
        """`CREATE OR ALTER VIEW` from the model's source query — the
        target-side asset for a VIEW model. Called instead of the table
        DDL when `model.is_view`."""
        from bollhav.mssql.modes import create_replace_view

        create_replace_view(conn=self.conn, model=self.model)

    def recreate_table(self) -> None:
        target = self.model.target
        schema, table = target.schema_resolved, target.name_resolved
        cursor = self.conn.cursor()
        cursor.execute(
            f"IF OBJECT_ID(?, 'U') IS NOT NULL DROP TABLE {_bracket_quote(schema)}.{_bracket_quote(table)}",
            f"{schema}.{table}",
        )
        cursor.commit()

    def create_table(self) -> None:
        """`CREATE TABLE IF NOT EXISTS` for the target, then its PRIMARY
        KEY and indexes. Non-destructive and idempotent — recreate /
        truncate are separate, lifecycle-ordered steps."""
        target = self.model.target
        schema, table = target.schema_resolved, target.name_resolved
        mssql_cols = [c for c in target.columns if isinstance(c, MssqlColumn)]
        col_defs = ",\n".join(_col_ddl(c) for c in mssql_cols)

        cursor = self.conn.cursor()
        cursor.execute(
            f"IF NOT EXISTS ("
            f"    SELECT 1 FROM INFORMATION_SCHEMA.TABLES"
            f"    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
            f") CREATE TABLE {_bracket_quote(schema)}.{_bracket_quote(table)} (\n{col_defs}\n)",
            schema,
            table,
        )
        cursor.commit()
        ensure_primary_key(conn=self.conn, model=self.model)
        ensure_indexes(conn=self.conn, model=self.model)

    def truncate_table(self) -> None:
        target = self.model.target
        schema, table = target.schema_resolved, target.name_resolved
        cursor = self.conn.cursor()
        cursor.execute(
            f"TRUNCATE TABLE {_bracket_quote(schema)}.{_bracket_quote(table)}"
        )
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
        schema, table = target.schema_resolved, target.name_resolved
        constraint_name = f"{table}_uq"
        cols = ", ".join(_bracket_quote(c.name) for c in target.unique_columns)
        cursor = self.conn.cursor()
        cursor.execute(
            f"IF NOT EXISTS ("
            f"    SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
            f"    WHERE CONSTRAINT_NAME = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ?"
            f") ALTER TABLE {_bracket_quote(schema)}.{_bracket_quote(table)}"
            f"    ADD CONSTRAINT {_bracket_quote(constraint_name)} UNIQUE ({cols})",
            constraint_name,
            schema,
            table,
        )
        cursor.commit()

    # ── staging lifecycle ─────────────────────────────────────────────

    def create_staging_schema(self) -> None:
        ensure_staging_schema(self.conn, self.model)

    def gc_orphan_staging_tables(self, *, keep_run_id: UUID | None = None) -> None:
        cleanup_orphaned_staging_tables(self.conn, self.model, keep_run_id=keep_run_id)

    def create_staging_table(self, run_id: UUID) -> None:
        ensure_staging_table(self.conn, self.model, run_id)

    def write_to_staging(self, run_id: UUID, df: "pl.DataFrame") -> None:
        write_to_staging(self.conn, self.model, run_id, df)

    def apply_staging_to_target(self, run_id: UUID, interval: "TZInterval") -> None:
        apply_atomically_to_target(
            self.conn,
            self.model,
            run_id=run_id,
            since=interval.since,
            until=interval.until,
        )

    def drop_staging_table(self, run_id: UUID) -> None:
        if (
            self.model.target.staging is not None
            and self.model.target.staging.keep_after_apply
        ):
            logger.debug(
                "teardown skipped for %s — Staging.keep_after_apply=True",
                self.model.target.full_name,
            )
            return
        drop_staging_table(self.conn, self.model, run_id)


__all__ = ["MssqlData"]
