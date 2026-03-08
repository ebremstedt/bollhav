import psycopg
from psycopg import sql
import polars as pl
from typing import cast, LiteralString
from datetime import datetime, timedelta
from bollhav.model import Model
from bollhav.postgres import PostgresColumn


def _col_ddl(col: PostgresColumn) -> LiteralString:
    pg_type = col.data_type.value
    if col.precision is not None and col.scale is not None:
        pg_type = f"{pg_type}({col.precision}, {col.scale})"
    elif col.length is not None:
        pg_type = f"{pg_type}({col.length})"
    constraints = ""
    if col.primary_key:
        constraints = " PRIMARY KEY"
    elif col.unique:
        constraints = " UNIQUE"
    null_clause = "NOT NULL" if not col.nullable else ""
    return cast(
        LiteralString, f'    "{col.name}" {pg_type}{constraints} {null_clause}'.rstrip()
    )


def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    conn.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
    )


def ensure_table(conn: psycopg.Connection, model: Model) -> None:
    col_defs = sql.SQL(",\n").join(
        sql.SQL(_col_ddl(col))
        for col in model.model_config.columns
        if isinstance(col, PostgresColumn)
    )
    conn.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{table} (\n{col_defs}\n)").format(
            schema=sql.Identifier(model.model_config.schema),
            table=sql.Identifier(model.model_config.name),
            col_defs=col_defs,
        )
    )
    if model.model_config.partitioned_by is not None:
        index_name = (
            f"{model.model_config.name}_{model.model_config.partitioned_by}_idx"
        )
        conn.execute(
            sql.SQL(
                "CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} ({col})"
            ).format(
                index=sql.Identifier(index_name),
                schema=sql.Identifier(model.model_config.schema),
                table=sql.Identifier(model.model_config.name),
                col=sql.Identifier(model.model_config.partitioned_by),
            )
        )


def _ensure(conn: psycopg.Connection, model: Model) -> None:
    ensure_schema(conn=conn, schema=model.model_config.schema)
    ensure_table(conn=conn, model=model)


def append(
    conn: psycopg.Connection,
    model: Model,
    df: pl.DataFrame,
) -> None:
    _ensure(conn=conn, model=model)
    col_names = sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
    query = sql.SQL("COPY {schema}.{table} ({cols}) FROM STDIN").format(
        schema=sql.Identifier(model.model_config.schema),
        table=sql.Identifier(model.model_config.name),
        cols=col_names,
    )
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
    _ensure(conn=conn, model=model)

    if model.model_config.partitioned_by is None:
        raise ValueError(
            "The attribute model_config.partitioned_by must be set for OVERWRITE_INSERT"
        )

    with conn.transaction():
        conn.execute(
            sql.SQL(
                "DELETE FROM {schema}.{table} WHERE {col} >= %s AND {col} < %s"
            ).format(
                schema=sql.Identifier(model.model_config.schema),
                table=sql.Identifier(model.model_config.name),
                col=sql.Identifier(model.model_config.partitioned_by),
            ),
            [since, until],
        )
        col_names = sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
        copy_query = sql.SQL("COPY {schema}.{table} ({cols}) FROM STDIN").format(
            schema=sql.Identifier(model.model_config.schema),
            table=sql.Identifier(model.model_config.name),
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
                schema=sql.Identifier(model.model_config.schema),
                table=sql.Identifier(model.model_config.name),
            )
        )
        _ensure(conn=conn, model=model)
        col_names = sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
        copy_query = sql.SQL("COPY {schema}.{table} ({cols}) FROM STDIN").format(
            schema=sql.Identifier(model.model_config.schema),
            table=sql.Identifier(model.model_config.name),
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
    _ensure(conn=conn, model=model)
    with conn.transaction():
        conn.execute(
            sql.SQL("TRUNCATE TABLE {schema}.{table}").format(
                schema=sql.Identifier(model.model_config.schema),
                table=sql.Identifier(model.model_config.name),
            )
        )
        col_names = sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
        copy_query = sql.SQL("COPY {schema}.{table} ({cols}) FROM STDIN").format(
            schema=sql.Identifier(model.model_config.schema),
            table=sql.Identifier(model.model_config.name),
            cols=col_names,
        )
        with conn.cursor().copy(copy_query) as copy:
            for row in df.rows():
                copy.write_row(row)


def update_insert(conn: psycopg.Connection, model: Model, df: pl.DataFrame) -> None:
    _ensure(conn=conn, model=model)
    unique_columns = [col.name for col in model.model_config.unique_columns]
    with conn.transaction():
        temp_table = f"temp_{model.model_config.name}_{id(df)}"
        col_names = sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
        pk_cols = sql.SQL(", ").join(sql.Identifier(c) for c in unique_columns)
        update_set = sql.SQL(", ").join(
            sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
            for c in df.columns
            if c not in unique_columns
        )
        conn.execute(
            sql.SQL("DROP TABLE IF EXISTS {temp}").format(
                temp=sql.Identifier(temp_table)
            )
        )
        conn.execute(
            sql.SQL(
                "CREATE TEMP TABLE {temp} (LIKE {schema}.{table}) ON COMMIT DROP"
            ).format(
                temp=sql.Identifier(temp_table),
                schema=sql.Identifier(model.model_config.schema),
                table=sql.Identifier(model.model_config.name),
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
                schema=sql.Identifier(model.model_config.schema),
                table=sql.Identifier(model.model_config.name),
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
    if model.model_config.source_query is None:
        raise ValueError(
            f"The model_config.source_query attribute must be set for {model.model_config.write_mode.value}"
        )

    with conn.transaction():
        ensure_schema(conn, model.model_config.schema)
        conn.execute(
            sql.SQL("CREATE OR REPLACE VIEW {schema}.{view} AS {query}").format(
                schema=sql.Identifier(model.model_config.schema),
                view=sql.Identifier(model.model_config.name),
                query=sql.SQL(cast(LiteralString, model.model_config.source_query)),
            )
        )
