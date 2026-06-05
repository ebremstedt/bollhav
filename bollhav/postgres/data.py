"""Target-side (data) Postgres backend for one model.

`PostgresData` is the data-connection counterpart to `PostgresState`
(which owns the state connection): it runs the target-DB asset DDL for
a model. Each method is one discrete operation (create schema, create
table, truncate, …) — there is no runner that loops over an action
list. The caller (the lifecycle hook) decides which to call and in what
order.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, LiteralString, cast
from uuid import UUID

import psycopg
from psycopg import sql

if TYPE_CHECKING:
    import polars as pl

    from bollhav.model.database import DatabaseColumn
    from bollhav.model.intervals import TZInterval
    from bollhav.model.model import Model

logger = logging.getLogger(__name__)


def _col_ddl(col: "DatabaseColumn") -> LiteralString:
    """Render one column-definition line for a CREATE TABLE. Skips
    non-PostgresColumn entries with an empty string — the caller's
    `if isinstance(...)` filter normally prevents them anyway."""
    from bollhav.postgres.columns import PostgresColumn

    if not isinstance(col, PostgresColumn):
        return cast(LiteralString, "")
    pg_type = col.data_type.value
    if col.precision is not None and col.scale is not None:
        pg_type = f"{pg_type}({col.precision}, {col.scale})"
    elif col.length is not None:
        pg_type = f"{pg_type}({col.length})"
    constraints = " PRIMARY KEY" if col.primary_key else ""
    null_clause = "NOT NULL" if not col.nullable else ""
    return cast(
        LiteralString,
        f'    "{col.name}" {pg_type}{constraints} {null_clause}'.rstrip(),
    )


class PostgresData:
    """Target-side asset DDL for one model.

    Construct with the model and the caller-owned data connection
    (opened in `main()`, threaded through the lifecycle hooks). Each
    method runs one discrete piece of setup; the lifecycle hook calls
    the ones it needs, in order."""

    def __init__(self, model: "Model", conn: psycopg.Connection) -> None:
        self.model = model
        self.conn = conn

    def _staging_schema_name(self) -> str:
        model = self.model
        staging = model.target.staging
        if staging is not None and staging.schema:
            return staging.schema
        prefix = (
            model.state.schema_prefix
            if model.state is not None and model.state.schema_prefix is not None
            else "z_"
        )
        return f"{prefix}{model.target.schema.resolved}"

    def _staging_table_prefix(self) -> str:
        """Staging-table name prefix. `Staging.table_prefix` overrides
        the default `<target_name>_staging_`."""
        staging = self.model.target.staging
        if staging is not None and staging.table_prefix:
            return staging.table_prefix
        return f"{self.model.target.name}_staging_"

    def _staging_table_name(self, run_id: UUID) -> str:
        """Per-interval staging table name. The first 8 hex chars of
        `run_id` disambiguate within a model."""
        return f"{self._staging_table_prefix()}{str(run_id)[:8]}"

    def _staging_logged(self) -> bool:
        """Resolve the Postgres `logged` knob. Only `PostgresStaging`
        carries it; a plain `Staging(...)` gets the default UNLOGGED
        behavior (faster COPY; truncated on crash, which is fine since
        the interval reruns from the top)."""
        from bollhav.postgres.staging import PostgresStaging

        staging = self.model.target.staging
        return isinstance(staging, PostgresStaging) and staging.logged

    def ensure_assets(self) -> None:
        """Idempotent, NON-destructive setup of the target's assets:
        schema, table, indexes, unique constraint, and (when staging) the
        staging schema. Safe to call repeatedly — every step is guarded
        (`CREATE ... IF NOT EXISTS` / `ADD CONSTRAINT ... EXCEPTION WHEN
        duplicate`), so write paths call it per-write to self-heal a
        missing asset.

        Deliberately excludes `recreate_table` / `truncate_table`: those
        are destructive, once-per-run setup the lifecycle hook performs
        before the interval loop — never per write/interval."""
        target = self.model.target
        self.create_schema()
        self.create_table()
        if target.partitioned_by is not None:
            self.create_indexes()
        if target.unique_columns:
            self.add_unique_constraint()
        if target.stage:
            self.create_staging_schema()

    def create_schema(self) -> None:
        self.conn.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(self.model.target.schema.resolved)
            )
        )

    def create_or_replace_view(self) -> None:
        """`CREATE OR REPLACE VIEW` from the model's `SourceTable.query` —
        the target-side asset for a VIEW model. The lifecycle hook calls
        this instead of the table DDL when `model.is_view`."""
        from bollhav.postgres.modes import create_replace_view

        create_replace_view(conn=self.conn, model=self.model)

    def recreate_table(self) -> None:
        target = self.model.target
        self.conn.execute(
            sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier(target.schema.resolved),
                sql.Identifier(target.name_resolved),
            )
        )

    def create_table(self) -> None:
        from bollhav.postgres.columns import PostgresColumn

        target = self.model.target
        col_defs = sql.SQL(",\n").join(
            sql.SQL(_col_ddl(c))
            for c in target.columns
            if isinstance(c, PostgresColumn)
        )
        self.conn.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} (\n{}\n)").format(
                sql.Identifier(target.schema.resolved),
                sql.Identifier(target.name_resolved),
                col_defs,
            )
        )

    def truncate_table(self) -> None:
        target = self.model.target
        self.conn.execute(
            sql.SQL("TRUNCATE TABLE {}.{}").format(
                sql.Identifier(target.schema.resolved),
                sql.Identifier(target.name_resolved),
            )
        )

    def create_indexes(self) -> None:
        target = self.model.target
        col = target.partitioned_by
        if col is None:
            raise RuntimeError(
                f"create_indexes ran for {target.full_name!r} but partitioned_by "
                f"is None — guard the call on `target.partitioned_by is not None`"
            )
        index_name = f"{target.name_resolved}_{col}_idx"
        self.conn.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} ({})").format(
                sql.Identifier(index_name),
                sql.Identifier(target.schema.resolved),
                sql.Identifier(target.name_resolved),
                sql.Identifier(col),
            )
        )

    def add_unique_constraint(self) -> None:
        target = self.model.target
        constraint_name = f"{target.name_resolved}_uq"
        unique_col_ids = sql.SQL(", ").join(
            sql.Identifier(c.name) for c in target.unique_columns
        )
        self.conn.execute(
            sql.SQL("""
                DO $$ BEGIN
                    ALTER TABLE {}.{}
                    ADD CONSTRAINT {} UNIQUE ({});
                EXCEPTION WHEN duplicate_table THEN NULL;
                END $$
            """).format(
                sql.Identifier(target.schema.resolved),
                sql.Identifier(target.name_resolved),
                sql.Identifier(constraint_name),
                unique_col_ids,
            )
        )

    def create_staging_schema(self) -> None:
        self.conn.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(self._staging_schema_name())
            )
        )

    def create_staging_table(self, run_id: UUID) -> None:
        """Create the per-interval staging table. Each interval gets its
        own freshly-CREATEd table (the prior interval's flush dropped
        it, unless `keep_after_apply`), so this is called per-interval
        with the run's `run_id`, not once at model setup."""
        from bollhav.postgres.columns import PostgresColumn

        target = self.model.target
        schema = self._staging_schema_name()
        table = self._staging_table_name(run_id)
        table_keyword = "TABLE" if self._staging_logged() else "UNLOGGED TABLE"

        col_defs = sql.SQL(",\n").join(
            sql.SQL(_col_ddl(col))
            for col in target.columns
            if isinstance(col, PostgresColumn)
        )
        self.conn.execute(
            sql.SQL(
                f"CREATE {table_keyword} IF NOT EXISTS "
                "{schema}.{table} (\n{col_defs}\n)"
            ).format(
                schema=sql.Identifier(schema),
                table=sql.Identifier(table),
                col_defs=col_defs,
            )
        )
        logger.debug("created staging table %s.%s", schema, table)

    def gc_orphan_staging_tables(self, *, keep_run_id: UUID | None = None) -> None:
        """Drop staging tables left behind by crashed runs. Best-effort:
        any error is swallowed and logged so this never blocks the run.

        Matches `<prefix>%` in the staging schema (prefix from
        `Staging.table_prefix` or the default). If `keep_run_id` is given,
        that run's staging table is preserved.

        No-op when `Staging.keep_after_apply=True` — the operator has
        declared they want kept tables to live; auto-GC would defeat that,
        so manual cleanup is on them."""
        staging = self.model.target.staging
        if staging is not None and staging.keep_after_apply:
            logger.debug(
                "GC skipped for %s — Staging.keep_after_apply=True",
                self.model.target.full_name,
            )
            return

        try:
            schema = self._staging_schema_name()
            prefix = self._staging_table_prefix()
            keep = (
                f"{prefix}{str(keep_run_id)[:8]}" if keep_run_id is not None else None
            )

            rows = self.conn.execute(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname = %s AND tablename LIKE %s",
                [schema, f"{prefix}%"],
            ).fetchall()

            with self.conn.transaction():
                for (tablename,) in rows:
                    if keep is not None and tablename == keep:
                        continue
                    self.conn.execute(
                        sql.SQL("DROP TABLE IF EXISTS {schema}.{table}").format(
                            schema=sql.Identifier(schema),
                            table=sql.Identifier(tablename),
                        )
                    )
                    logger.debug("gc'd orphan staging table %s.%s", schema, tablename)
        except Exception as exc:  # best-effort; never blocks the run
            logger.debug(
                "staging: orphan GC skipped for %s — %s",
                self.model.target.full_name,
                exc,
            )

    # ── staging lifecycle ─────────────────────────────────────────────
    #
    # The per-interval staging flow as discrete steps the lifecycle hook
    # drives in order: create the table (`create_staging_table`), write
    # rows into it, merge it into the target, then tear it down. The heavy
    # write-mode SQL lives in `bollhav.postgres.staging`; these are the
    # model-scoped entry points onto it.

    def write_to_staging(self, run_id: UUID, df: "pl.DataFrame") -> None:
        """COPY / upsert one sub-batch into this run's staging table, per
        `Staging.write_mode`. Called by the user's execute to land rows
        before the merge."""
        from bollhav.postgres.staging import write_to_staging as _write_to_staging

        _write_to_staging(self.conn, self.model, run_id, df)

    def apply_staging_to_target(self, run_id: UUID, interval: "TZInterval") -> None:
        """Merge this run's staging table into the target using
        `target.write_mode` (APPEND / UPSERT_NO_DELETE / RECREATE_PARTITION),
        atomically. Does NOT drop the staging table — `drop_staging_table`
        does, so the merge and the teardown stay distinct steps."""
        from bollhav.postgres.staging import apply_atomically_to_target

        apply_atomically_to_target(
            self.conn,
            self.model,
            run_id=run_id,
            since=interval.since,
            until=interval.until,
            drop_after_apply=False,
        )

    def drop_staging_table(self, run_id: UUID) -> None:
        """Tear down this run's staging table after a successful merge.
        No-op when `Staging.keep_after_apply` (the operator wants kept
        tables to live; manual cleanup is on them)."""
        staging = self.model.target.staging
        if staging is not None and staging.keep_after_apply:
            logger.debug(
                "teardown skipped for %s — Staging.keep_after_apply=True",
                self.model.target.full_name,
            )
            return
        from bollhav.postgres.staging import drop_staging_table as _drop_staging_table

        _drop_staging_table(self.conn, self.model, run_id)


__all__ = ["PostgresData"]
