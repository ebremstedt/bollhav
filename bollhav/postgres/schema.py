import logging
import psycopg
from psycopg import sql
from typing import cast, LiteralString
from bollhav.model.model import Model
from bollhav.postgres.columns import PostgresColumn

logger = logging.getLogger(__name__)


def _col_ddl(col: PostgresColumn) -> LiteralString:
    pg_type = col.data_type.value
    if col.precision is not None and col.scale is not None:
        pg_type = f"{pg_type}({col.precision}, {col.scale})"
    elif col.length is not None:
        pg_type = f"{pg_type}({col.length})"
    constraints = ""
    if col.primary_key:
        constraints = " PRIMARY KEY"
    null_clause = "NOT NULL" if not col.nullable else ""
    return cast(
        LiteralString, f'    "{col.name}" {pg_type}{constraints} {null_clause}'.rstrip()
    )


def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    logger.debug("Ensuring schema: %s", schema)
    conn.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
    )


def ensure_table(conn: psycopg.Connection, model: Model) -> None:
    """First call in a pipeline run: drop (if recreate_table), create,
    truncate (if truncate_table), add indexes, add unique constraints.
    Subsequent calls in the same run: complete no-op — every step is
    gated by `target.mutations.*` flags. See `Mutations` docstring."""
    target = model.target
    schema_id = sql.Identifier(target.schema.resolved)
    table_id = sql.Identifier(target.name_resolved)
    logger.debug(
        "Ensuring table: %s.%s",
        target.schema.resolved,
        target.name_resolved,
    )

    if target.recreate_table and not target.mutations.recreated:
        conn.execute(
            sql.SQL("DROP TABLE IF EXISTS {schema}.{table}").format(
                schema=schema_id, table=table_id
            )
        )
        target.mutations.recreated = True

    if not target.mutations.table_created:
        col_defs = sql.SQL(",\n").join(
            sql.SQL(_col_ddl(col))
            for col in target.columns
            if isinstance(col, PostgresColumn)
        )
        conn.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {schema}.{table} (\n{col_defs}\n)"
            ).format(
                schema=schema_id,
                table=table_id,
                col_defs=col_defs,
            )
        )
        target.mutations.table_created = True

    if target.truncate_table and not target.mutations.truncated:
        conn.execute(
            sql.SQL("TRUNCATE TABLE {schema}.{table}").format(
                schema=schema_id, table=table_id
            )
        )
        target.mutations.truncated = True

    if target.partitioned_by is not None and not target.mutations.indexes_created:
        index_name = f"{target.name_resolved}_{target.partitioned_by}_idx"
        conn.execute(
            sql.SQL(
                "CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} ({col})"
            ).format(
                index=sql.Identifier(index_name),
                schema=schema_id,
                table=table_id,
                col=sql.Identifier(target.partitioned_by),
            )
        )
        target.mutations.indexes_created = True

    if target.unique_columns and not target.mutations.uniques_added:
        constraint_name = f"{target.name_resolved}_uq"
        unique_col_ids = sql.SQL(", ").join(
            sql.Identifier(col.name) for col in target.unique_columns
        )
        conn.execute(
            sql.SQL("""
                DO $$ BEGIN
                    ALTER TABLE {schema}.{table}
                    ADD CONSTRAINT {constraint} UNIQUE ({cols});
                EXCEPTION WHEN duplicate_table THEN NULL;
                END $$
            """).format(
                schema=schema_id,
                table=table_id,
                constraint=sql.Identifier(constraint_name),
                cols=unique_col_ids,
            )
        )
        target.mutations.uniques_added = True


def ensure_schema_and_table(conn: psycopg.Connection, model: Model) -> None:
    target = model.target
    if target.setup_complete:
        return
    with conn.transaction():
        if not target.mutations.schema_created:
            ensure_schema(conn=conn, schema=target.schema.resolved)
            target.mutations.schema_created = True
        ensure_table(conn=conn, model=model)
