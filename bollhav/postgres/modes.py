import psycopg
import polars as pl
from datetime import datetime, timedelta
from bollhav.model import Model
from bollhav.postgres import PostgresColumn


def _col_ddl(col: PostgresColumn) -> str:
    pg_type = col.data_type.value
    if col.precision is not None and col.scale is not None:
        pg_type = f"{pg_type}({col.precision}, {col.scale})"
    elif col.length is not None:
        pg_type = f"{pg_type}({col.length})"

    constraints = ""
    if col.primary_key:
        constraints = " PRIMARY KEY"
    elif col.unique:
        constraints = " UNIQUE"

    null_clause = "NOT NULL" if not col.nullable else ""
    return f'    "{col.name}" {pg_type}{constraints} {null_clause}'.rstrip()


def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def ensure_table(conn: psycopg.Connection, model: Model) -> None:
    col_defs = ",\n".join(
        _col_ddl(col) for col in model.columns if isinstance(col, PostgresColumn)
    )
    conn.execute(
        f'CREATE TABLE IF NOT EXISTS {model.schema}."{model.name}" (\n{col_defs}\n)'
    )
    if model.partitioned_by_index:
        index_name = f"{model.name}_{model.partitioned_by}_idx"
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS "{index_name}" '
            f'ON {model.schema}."{model.name}" ("{model.partitioned_by}")'
        )


def _ensure(conn: psycopg.Connection, model: Model) -> None:
    ensure_schema(conn, model.schema)
    ensure_table(conn, model)


def append(
    conn: psycopg.Connection,
    model: Model,
    df: pl.DataFrame,
) -> None:
    _ensure(conn, model)
    col_names = ", ".join(f'"{c}"' for c in df.columns)
    with conn.cursor() as cursor:
        with cursor.copy(
            f'COPY {model.schema}."{model.name}" ({col_names}) FROM STDIN'
        ) as copy:
            for row in df.rows():
                copy.write_row(row)


def _assert_utc(dt: datetime, name: str) -> None:
    if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be UTC, got {dt!r}")


def overwrite_insert(
    conn: psycopg.Connection,
    model: Model,
    df: pl.DataFrame,
    since: datetime,
    until: datetime,
) -> None:
    _assert_utc(dt=since, name=since.__str__())
    _assert_utc(dt=until, name=until.__str__())

    _ensure(conn, model)
    with conn.transaction():
        conn.execute(
            f'DELETE FROM {model.schema}."{model.name}" '
            f'WHERE "{model.partitioned_by}" >= %s AND "{model.partitioned_by}" < %s',
            [since, until],
        )
        col_names = ", ".join(f'"{c}"' for c in df.columns)
        with conn.cursor().copy(
            f'COPY {model.schema}."{model.name}" ({col_names}) FROM STDIN'
        ) as copy:
            for row in df.rows():
                copy.write_row(row)


def truncate_insert(
    conn: psycopg.Connection,
    model: Model,
    df: pl.DataFrame,
) -> None:
    _ensure(conn, model)
    with conn.transaction():
        conn.execute(f'TRUNCATE TABLE {model.schema}."{model.name}"')
        col_names = ", ".join(f'"{c}"' for c in df.columns)
        with conn.cursor().copy(
            f'COPY {model.schema}."{model.name}" ({col_names}) FROM STDIN'
        ) as copy:
            for row in df.rows():
                copy.write_row(row)


def update_insert(conn: psycopg.Connection, model: Model, df: pl.DataFrame) -> None:
    _ensure(conn, model)
    unique_columns = [col.name for col in model.unique_columns]
    with conn.transaction():
        temp_table = f"temp_{model.name}_{id(df)}"
        col_names = ", ".join(f'"{c}"' for c in df.columns)
        pk_cols_str = ", ".join(f'"{c}"' for c in unique_columns)

        conn.execute(
            f'CREATE TEMP TABLE "{temp_table}" (LIKE {model.schema}."{model.name}") ON COMMIT DROP'
        )
        with conn.cursor().copy(
            f'COPY "{temp_table}" ({col_names}) FROM STDIN'
        ) as copy:
            for row in df.rows():
                copy.write_row(row)

        update_set = ", ".join(
            f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c not in unique_columns
        )
        conn.execute(
            f'INSERT INTO {model.schema}."{model.name}" ({col_names}) '
            f'SELECT {col_names} FROM "{temp_table}" t '
            f"ON CONFLICT ({pk_cols_str}) DO UPDATE SET {update_set}"
        )


def create_view(
    conn: psycopg.Connection,
    model: Model,
    query: str,
) -> None:
    with conn.transaction():
        ensure_schema(conn, model.schema)
        conn.execute(f'CREATE OR REPLACE VIEW {model.schema}."{model.name}" AS {query}')
