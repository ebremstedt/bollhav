import logging
import psycopg
from psycopg import sql
import polars as pl
from typing import cast, LiteralString
from datetime import datetime, timedelta
from bollhav.model.model import Model
from bollhav.postgres.schema import ensure_schema

logger = logging.getLogger(__name__)


def _assert_utc(dt: datetime, name: str) -> None:
    if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be UTC, got {dt!r}")


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


def recreate_partition(
    conn: psycopg.Connection,
    model: Model,
    df: pl.DataFrame,
    since: datetime,
    until: datetime,
) -> None:
    _assert_utc(since, "since")
    _assert_utc(until, "until")
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


def upsert_no_delete(conn: psycopg.Connection, model: Model, df: pl.DataFrame) -> None:
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
