from __future__ import annotations

import psycopg
import polars as pl


def truncate_insert(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    df: pl.DataFrame,
) -> None:
    conn.execute(f'TRUNCATE TABLE {schema}."{table}"')

    col_names = ", ".join(f'"{c}"' for c in df.columns)
    rows = df.rows()

    with conn.cursor() as cursor:
        with cursor.copy(f'COPY {schema}."{table}" ({col_names}) FROM STDIN') as copy:
            for row in rows:
                copy.write_row(row)
