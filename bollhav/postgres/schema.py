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
    schema_id = sql.Identifier(model.target.schema.resolved)
    table_id = sql.Identifier(model.target.name)
    logger.debug(
        "Ensuring table: %s.%s", model.target.schema.resolved, model.target.name
    )

    if model.target.recreate_table:
        conn.execute(
            sql.SQL("DROP TABLE IF EXISTS {schema}.{table}").format(
                schema=schema_id, table=table_id
            )
        )

    col_defs = sql.SQL(",\n").join(
        sql.SQL(_col_ddl(col))
        for col in model.target.columns
        if isinstance(col, PostgresColumn)
    )
    conn.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{table} (\n{col_defs}\n)").format(
            schema=schema_id,
            table=table_id,
            col_defs=col_defs,
        )
    )

    if model.target.truncate_table:
        conn.execute(
            sql.SQL("TRUNCATE TABLE {schema}.{table}").format(
                schema=schema_id, table=table_id
            )
        )
    if model.target.partitioned_by is not None:
        index_name = f"{model.target.name}_{model.target.partitioned_by}_idx"
        conn.execute(
            sql.SQL(
                "CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} ({col})"
            ).format(
                index=sql.Identifier(index_name),
                schema=sql.Identifier(model.target.schema.resolved),
                table=sql.Identifier(model.target.name),
                col=sql.Identifier(model.target.partitioned_by),
            )
        )

    unique_columns = [
        col
        for col in model.target.columns
        if isinstance(col, PostgresColumn) and col.unique
    ]
    if unique_columns:
        constraint_name = f"{model.target.name}_uq"
        unique_col_ids = sql.SQL(", ").join(
            sql.Identifier(col.name) for col in unique_columns
        )
        conn.execute(
            sql.SQL("""
                DO $$ BEGIN
                    ALTER TABLE {schema}.{table}
                    ADD CONSTRAINT {constraint} UNIQUE ({cols});
                EXCEPTION WHEN duplicate_table THEN NULL;
                END $$
            """).format(
                schema=sql.Identifier(model.target.schema.resolved),
                table=sql.Identifier(model.target.name),
                constraint=sql.Identifier(constraint_name),
                cols=unique_col_ids,
            )
        )


def ensure_schema_and_table(conn: psycopg.Connection, model: Model) -> None:
    with conn.transaction():
        ensure_schema(conn=conn, schema=model.target.schema.resolved)
        ensure_table(conn=conn, model=model)
