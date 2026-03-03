import psycopg
import polars as pl
from datetime import datetime


def append(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    df: pl.DataFrame,
) -> None:
    col_names = ", ".join(f'"{c}"' for c in df.columns)
    rows = df.rows()

    with conn.cursor() as cursor:
        with cursor.copy(f'COPY {schema}."{table}" ({col_names}) FROM STDIN') as copy:
            for row in rows:
                copy.write_row(row)


def overwrite_insert(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    df: pl.DataFrame,
    since: datetime,
    until: datetime,
    filter_column: str,
) -> None:
    with conn.transaction():
        conn.execute(
            f'DELETE FROM {schema}."{table}" '
            f'WHERE "{filter_column}" >= %s AND "{filter_column}" < %s',
            [since, until],
        )

        col_names = ", ".join(f'"{c}"' for c in df.columns)
        copy_cmd = f'COPY {schema}."{table}" ({col_names}) FROM STDIN'

        cur = conn.cursor()
        with cur.copy(copy_cmd) as copy:
            for row in df.rows():
                copy.write_row(row)


def truncate_insert(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    df: pl.DataFrame,
) -> None:
    with conn.transaction():
        conn.execute(f'TRUNCATE TABLE {schema}."{table}"')

        # Option A: manual COPY (always fastest, no extra deps)
        col_names = ", ".join(f'"{c}"' for c in df.columns)
        with conn.cursor().copy(
            f'COPY {schema}."{table}" ({col_names}) FROM STDIN'
        ) as copy:
            for row in df.rows():
                copy.write_row(row)


def update_insert(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    df: pl.DataFrame,
    pk_columns: list[str],
) -> None:
    with conn.transaction():
        temp_table = f"{schema}.temp_{table}_{id(df)}"

        col_names = ", ".join(f'"{c}"' for c in df.columns)
        pk_cols_str = ", ".join(f'"{c}"' for c in pk_columns)

        conn.execute(
            f'CREATE TEMP TABLE "{temp_table}" (LIKE {schema}."{table}") ON COMMIT DROP'
        )

        with conn.cursor().copy(
            f'COPY "{temp_table}" ({col_names}) FROM STDIN WITH (FORMAT binary)'
        ) as copy:
            copy.write_table(df.to_arrow())

        update_set = ", ".join(
            f'"{c}" = t."{c}"' for c in df.columns if c not in pk_columns
        )

        conn.execute(
            f'INSERT INTO {schema}."{table}" ({col_names}) '
            f'SELECT {col_names} FROM "{temp_table}" t '
            f"ON CONFLICT ({pk_cols_str}) DO UPDATE SET {update_set}"
        )


def create_view(
    conn: psycopg.Connection,
    schema: str,
    name: str,
    query: str,
) -> None:
    with conn.transaction():
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(f'CREATE OR REPLACE VIEW {schema}."{name}" AS {query}')
