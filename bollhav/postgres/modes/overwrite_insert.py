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
