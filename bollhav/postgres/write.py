from datetime import datetime
from typing import Generator
import psycopg
import polars as pl
from roskarl import DSN
from bollhav.model import Model
from bollhav.modes import WriteMode
from ddl import build_ddl, get_pk_columns
from modes.append import append
from modes.truncate_insert import truncate_insert
from modes.overwrite_insert import overwrite_insert
from modes.update_insert import update_insert
from modes.view import create_view


def _get_connection(dsn: DSN) -> psycopg.Connection:
    conn_string = (
        f"host={dsn.hostname} "
        f"port={dsn.port} "
        f"dbname={dsn.database} "
        f"user={dsn.username} "
        f"password={dsn.password}"
    )
    return psycopg.connect(conninfo=conn_string, autocommit=False)


def _ensure_table(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    ddl: str,
) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f'CREATE TABLE IF NOT EXISTS {schema}."{table}" ({ddl})')


def write(
    df_gen: Generator[pl.DataFrame, None, None],
    model: Model,
    since: datetime | None = None,
    until: datetime | None = None,
    filter_column: str | None = None,
) -> None:
    schema: str = model.schema
    table: str = model.name
    write_mode: WriteMode = model.write_mode

    if write_mode == WriteMode.VIEW:
        if not model.source_query:
            raise ValueError(f"VIEW mode requires source_query on model '{model.name}'")
        conn = _get_connection(dsn=model.target_dsn)
        try:
            with conn:
                create_view(
                    conn=conn,
                    schema=schema,
                    name=table,
                    query=model.source_query,
                )
        finally:
            conn.close()
        return

    ddl: str = build_ddl(columns=model.columns)

    pk_columns: list[str] = []
    if write_mode in (WriteMode.UPDATE_INSERT, WriteMode.MERGE):
        pk_columns = get_pk_columns(columns=model.columns)
        if not pk_columns:
            raise ValueError(
                f"{write_mode.value} requires at least one primary key column, "
                f"none found in model '{model.name}'"
            )

    if write_mode == WriteMode.OVERWRITE_INSERT:
        if since is None or until is None:
            raise ValueError(
                f"OVERWRITE_INSERT requires since and until, "
                f"got since={since}, until={until}"
            )
        if filter_column is None:
            raise ValueError("OVERWRITE_INSERT requires filter_column")

    conn = _get_connection(dsn=model.target_dsn)

    try:
        with conn:
            _ensure_table(conn=conn, schema=schema, table=table, ddl=ddl)

            first_batch = True

            for df in df_gen:
                if len(df) == 0:
                    continue

                if write_mode == WriteMode.APPEND:
                    append(conn=conn, schema=schema, table=table, df=df)

                elif write_mode == WriteMode.TRUNCATE_INSERT:
                    if first_batch:
                        truncate_insert(conn=conn, schema=schema, table=table, df=df)
                    else:
                        append(conn=conn, schema=schema, table=table, df=df)

                elif write_mode == WriteMode.OVERWRITE_INSERT:
                    if first_batch:
                        overwrite_insert(
                            conn=conn,
                            schema=schema,
                            table=table,
                            df=df,
                            since=since,
                            until=until,
                            filter_column=filter_column,
                        )
                    else:
                        append(conn=conn, schema=schema, table=table, df=df)

                elif write_mode == WriteMode.UPDATE_INSERT:
                    update_insert(
                        conn=conn,
                        schema=schema,
                        table=table,
                        df=df,
                        pk_columns=pk_columns,
                    )

                first_batch = False
    finally:
        conn.close()
