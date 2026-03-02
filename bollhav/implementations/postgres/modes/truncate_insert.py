import psycopg
import polars as pl


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
