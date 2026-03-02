from __future__ import annotations

import psycopg
import polars as pl


def merge(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    df: pl.DataFrame,
    pk_columns: list[str],
) -> None:
    df_columns = df.columns
    update_cols = [c for c in df_columns if c not in pk_columns]

    temp_table = f"_merge_staging_{table}"

    col_names = ", ".join(f'"{c}"' for c in df_columns)

    conn.execute(
        f'CREATE TEMP TABLE "{temp_table}" '
        f'(LIKE {schema}."{table}" INCLUDING ALL) '
        f"ON COMMIT DROP"
    )

    with conn.cursor() as cursor:
        with cursor.copy(f'COPY "{temp_table}" ({col_names}) FROM STDIN') as copy:
            for row in df.rows():
                copy.write_row(row)

    join_condition = " AND ".join(f'target."{c}" = source."{c}"' for c in pk_columns)
    update_set = ", ".join(f'"{c}" = source."{c}"' for c in update_cols)
    insert_cols = ", ".join(f'"{c}"' for c in df_columns)
    source_cols = ", ".join(f'source."{c}"' for c in df_columns)

    sql = (
        f'MERGE INTO {schema}."{table}" AS target '
        f'USING "{temp_table}" AS source '
        f"ON {join_condition} "
        f"WHEN MATCHED THEN UPDATE SET {update_set} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({source_cols})"
    )

    conn.execute(sql)
