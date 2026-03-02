from __future__ import annotations

import psycopg
import polars as pl


def update_insert(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    df: pl.DataFrame,
    pk_columns: list[str],
) -> None:
    df_columns = df.columns
    col_names = ", ".join(f'"{c}"' for c in df_columns)
    placeholders = ", ".join(["%s"] * len(df_columns))
    conflict_cols = ", ".join(f'"{c}"' for c in pk_columns)
    update_cols = [c for c in df_columns if c not in pk_columns]
    update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)

    sql = (
        f'INSERT INTO {schema}."{table}" ({col_names}) '
        f"VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_cols}) "
        f"DO UPDATE SET {update_set}"
    )

    rows = df.rows()

    with conn.cursor() as cursor:
        cursor.executemany(query=sql, params_seq=rows)
