from __future__ import annotations

from datetime import datetime
import psycopg
import polars as pl


def overwrite_insert(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    df: pl.DataFrame,
    since: datetime,
    until: datetime,
    filter_column: str,
) -> None:
    conn.execute(
        f'DELETE FROM {schema}."{table}" '
        f'WHERE "{filter_column}" >= %s AND "{filter_column}" < %s',
        [since, until],
    )

    col_names = ", ".join(f'"{c}"' for c in df.columns)
    rows = df.rows()

    with conn.cursor() as cursor:
        with cursor.copy(f'COPY {schema}."{table}" ({col_names}) FROM STDIN') as copy:
            for row in rows:
                copy.write_row(row)
