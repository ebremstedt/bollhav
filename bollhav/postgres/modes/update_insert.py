import psycopg
import polars as pl


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
