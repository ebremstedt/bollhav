import logging
import psycopg
from psycopg import sql
import polars as pl
from typing import cast, LiteralString
from datetime import datetime, timedelta
from bollhav.model.model import Model
from bollhav.postgres import PostgresColumn

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
    logger.debug(
        "Ensuring table: %s.%s", model.target.schema.resolved, model.target.name
    )
    col_defs = sql.SQL(",\n").join(
        sql.SQL(_col_ddl(col))
        for col in model.target.columns
        if isinstance(col, PostgresColumn)
    )
    conn.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{table} (\n{col_defs}\n)").format(
            schema=sql.Identifier(model.target.schema.resolved),
            table=sql.Identifier(model.target.name),
            col_defs=col_defs,
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


def append(
    conn: psycopg.Connection,
    model: Model,
    df: pl.DataFrame,
) -> None:
    col_names = sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
    query = sql.SQL("COPY {schema}.{table} ({cols}) FROM STDIN").format(
        schema=sql.Identifier(model.target.schema.resolved),
        table=sql.Identifier(model.target.name),
        cols=col_names,
    )
    with conn.transaction():
        with conn.cursor() as cursor:
            with cursor.copy(query) as copy:
                for row in df.rows():
                    copy.write_row(row)


def _assert_utc(dt: datetime, name: str) -> None:
    if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be UTC, got {dt!r}")


def overwrite_insert(
    conn: psycopg.Connection,
    model: Model,
    df: pl.DataFrame,
    since: datetime,
    until: datetime,
) -> None:
    _assert_utc(dt=since, name=since.__str__())
    _assert_utc(dt=until, name=until.__str__())

    with conn.transaction():
        conn.execute(
            sql.SQL(
                "DELETE FROM {schema}.{table} WHERE {col} >= %s AND {col} < %s"
            ).format(
                schema=sql.Identifier(model.target.schema.resolved),
                table=sql.Identifier(model.target.name),
                col=sql.Identifier(model.target.partitioned_by),
            ),
            [since, until],
        )
        col_names = sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
        copy_query = sql.SQL("COPY {schema}.{table} ({cols}) FROM STDIN").format(
            schema=sql.Identifier(model.target.schema.resolved),
            table=sql.Identifier(model.target.name),
            cols=col_names,
        )
        with conn.cursor().copy(copy_query) as copy:
            for row in df.rows():
                copy.write_row(row)


def recreate_insert(
    conn: psycopg.Connection,
    model: Model,
    df: pl.DataFrame,
) -> None:
    with conn.transaction():
        conn.execute(
            sql.SQL("DROP TABLE IF EXISTS {schema}.{table}").format(
                schema=sql.Identifier(model.target.schema.resolved),
                table=sql.Identifier(model.target.name),
            )
        )
        ensure_schema_and_table(conn=conn, model=model)
        col_names = sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
        copy_query = sql.SQL("COPY {schema}.{table} ({cols}) FROM STDIN").format(
            schema=sql.Identifier(model.target.schema.resolved),
            table=sql.Identifier(model.target.name),
            cols=col_names,
        )
        with conn.cursor().copy(copy_query) as copy:
            for row in df.rows():
                copy.write_row(row)


def truncate_insert(
    conn: psycopg.Connection,
    model: Model,
    df: pl.DataFrame,
) -> None:
    with conn.transaction():
        conn.execute(
            sql.SQL("TRUNCATE TABLE {schema}.{table}").format(
                schema=sql.Identifier(model.target.schema.resolved),
                table=sql.Identifier(model.target.name),
            )
        )
        col_names = sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
        copy_query = sql.SQL("COPY {schema}.{table} ({cols}) FROM STDIN").format(
            schema=sql.Identifier(model.target.schema.resolved),
            table=sql.Identifier(model.target.name),
            cols=col_names,
        )
        with conn.cursor().copy(copy_query) as copy:
            for row in df.rows():
                copy.write_row(row)


def update_insert(conn: psycopg.Connection, model: Model, df: pl.DataFrame) -> None:
    unique_columns = [col.name for col in model.target.unique_columns]
    temp_table = f"temp_{model.target.name}"

    col_names = sql.SQL(", ").join(
        sql.Identifier(col.name) for col in model.target.columns
    )
    pk_cols = sql.SQL(", ").join(sql.Identifier(c) for c in unique_columns)
    update_set = sql.SQL(", ").join(
        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col.name))
        for col in model.target.columns
        if col.name not in unique_columns
    )
    col_defs = sql.SQL(", ").join(
        sql.SQL("{name} {type}").format(
            name=sql.Identifier(col.name),
            type=sql.SQL(col.data_type.value),
        )
        for col in model.target.columns
    )

    with conn.transaction():
        # Cleanup leftover from a previous failed run
        conn.execute(
            sql.SQL("DROP TABLE IF EXISTS {temp}").format(
                temp=sql.Identifier(temp_table)
            )
        )
        conn.execute(
            sql.SQL("CREATE TEMP TABLE {temp} ({col_defs}) ON COMMIT DROP").format(
                temp=sql.Identifier(temp_table),
                col_defs=col_defs,
            )
        )
        with conn.cursor().copy(
            sql.SQL("COPY {temp} ({cols}) FROM STDIN").format(
                temp=sql.Identifier(temp_table),
                cols=col_names,
            )
        ) as copy:
            for row in df.rows():
                copy.write_row(row)
        conn.execute(
            sql.SQL(
                "INSERT INTO {schema}.{table} ({cols}) "
                "SELECT {cols} FROM {temp} t "
                "ON CONFLICT ({pk_cols}) DO UPDATE SET {update_set}"
            ).format(
                schema=sql.Identifier(model.target.schema.resolved),
                table=sql.Identifier(model.target.name),
                cols=col_names,
                temp=sql.Identifier(temp_table),
                pk_cols=pk_cols,
                update_set=update_set,
            )
        )


def create_replace_view(
    conn: psycopg.Connection,
    model: Model,
) -> None:
    if model.source.query is None:
        raise ValueError(
            f"model.source.query must be set for {model.target.write_mode.value}"
        )

    with conn.transaction():
        ensure_schema(conn, model.target.schema.resolved)
        conn.execute(
            sql.SQL("CREATE OR REPLACE VIEW {schema}.{view} AS {query}").format(
                schema=sql.Identifier(model.target.schema.resolved),
                view=sql.Identifier(model.target.name),
                query=sql.SQL(cast(LiteralString, model.source.query)),
            )
        )
