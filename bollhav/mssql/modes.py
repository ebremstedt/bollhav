import logging
import pyodbc
import polars as pl
from typing import cast, LiteralString
from bollhav.model.model import Model
from bollhav.mssql.columns import MssqlColumn, MssqlType
from bollhav.mssql.schema import _b, _col_type

logger = logging.getLogger(__name__)

_MSSQL_TO_ODBC: dict[MssqlType, tuple[int, int, int]] = {
    MssqlType.BIGINT: (pyodbc.SQL_BIGINT, 0, 0),
    MssqlType.BIT: (pyodbc.SQL_BIT, 0, 0),
    MssqlType.CHAR: (pyodbc.SQL_CHAR, 1, 0),
    MssqlType.DATE: (pyodbc.SQL_TYPE_DATE, 0, 0),
    MssqlType.DATETIME: (pyodbc.SQL_TYPE_TIMESTAMP, 23, 3),
    MssqlType.DATETIME2: (pyodbc.SQL_TYPE_TIMESTAMP, 26, 6),
    MssqlType.DATETIMEOFFSET: (pyodbc.SQL_TYPE_TIMESTAMP, 26, 6),
    MssqlType.DECIMAL: (pyodbc.SQL_DECIMAL, 18, 4),
    MssqlType.FLOAT: (pyodbc.SQL_FLOAT, 53, 0),
    MssqlType.INT: (pyodbc.SQL_INTEGER, 0, 0),
    MssqlType.NVARCHAR: (pyodbc.SQL_WVARCHAR, 0, 0),
    MssqlType.NUMERIC: (pyodbc.SQL_NUMERIC, 18, 4),
    MssqlType.REAL: (pyodbc.SQL_REAL, 0, 0),
    MssqlType.SMALLINT: (pyodbc.SQL_SMALLINT, 0, 0),
    MssqlType.TIME: (pyodbc.SQL_TYPE_TIME, 0, 0),
    MssqlType.TINYINT: (pyodbc.SQL_TINYINT, 0, 0),
    MssqlType.UNIQUEIDENTIFIER: (pyodbc.SQL_WVARCHAR, 36, 0),
    MssqlType.VARBINARY_MAX: (pyodbc.SQL_VARBINARY, 0, 0),
    MssqlType.VARCHAR: (pyodbc.SQL_VARCHAR, 0, 0),
}


def _odbc_type(col: MssqlColumn) -> tuple[int, int, int]:
    sql_type, default_prec, default_scale = _MSSQL_TO_ODBC[col.data_type]
    precision = col.precision if col.precision is not None else default_prec
    scale = col.scale if col.scale is not None else default_scale
    if col.length is not None and col.data_type in (
        MssqlType.NVARCHAR,
        MssqlType.VARCHAR,
        MssqlType.CHAR,
    ):
        precision = col.length
    return (sql_type, precision, scale)


def _bulk_insert(
    cursor: pyodbc.Cursor,
    target: str,
    col_names: list[str],
    df: pl.DataFrame,
    mssql_cols: list[MssqlColumn] | None = None,
) -> None:
    cols = ", ".join(_b(c) for c in col_names)
    placeholders = ", ".join(["?"] * len(col_names))
    cursor.fast_executemany = True
    if mssql_cols:
        cursor.setinputsizes([_odbc_type(c) for c in mssql_cols])
    cursor.executemany(
        f"INSERT INTO {target} ({cols}) VALUES ({placeholders})", df.rows()
    )


def merge(conn: pyodbc.Connection, model: Model, df: pl.DataFrame) -> None:
    """Upsert into target using MSSQL MERGE via a temp staging table."""
    schema = model.target.schema.resolved
    table = model.target.name
    temp = f"#tmp_{table}"

    mssql_cols = [c for c in model.target.columns if isinstance(c, MssqlColumn)]
    all_col_names = [c.name for c in mssql_cols]
    unique_col_names = [c.name for c in model.target.unique_columns]
    non_unique_col_names = [c for c in all_col_names if c not in unique_col_names]

    col_defs = ", ".join(f"{_b(c.name)} {_col_type(c)}" for c in mssql_cols)
    on_clause = " AND ".join(
        f"target.{_b(c)} = source.{_b(c)}" for c in unique_col_names
    )
    insert_cols = ", ".join(_b(c) for c in all_col_names)
    insert_vals = ", ".join(f"source.{_b(c)}" for c in all_col_names)

    cursor = conn.cursor()
    cursor.execute(f"IF OBJECT_ID('tempdb..{temp}') IS NOT NULL DROP TABLE {temp}")
    cursor.execute(f"CREATE TABLE {temp} ({col_defs})")
    _bulk_insert(cursor, temp, all_col_names, df, mssql_cols)

    if non_unique_col_names:
        update_set = ", ".join(
            f"target.{_b(c)} = source.{_b(c)}" for c in non_unique_col_names
        )
        matched_clause = f"WHEN MATCHED THEN UPDATE SET {update_set}"
    else:
        matched_clause = ""

    cursor.execute(
        f"MERGE INTO {_b(schema)}.{_b(table)} AS target "
        f"USING {temp} AS source ON {on_clause} "
        f"{matched_clause} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
    )
    cursor.commit()


def truncate_write(conn: pyodbc.Connection, model: Model, df: pl.DataFrame) -> None:
    """Truncate target table then bulk insert."""
    schema = model.target.schema.resolved
    table = model.target.name
    mssql_cols = [c for c in model.target.columns if isinstance(c, MssqlColumn)]
    all_col_names = [c.name for c in mssql_cols]

    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {_b(schema)}.{_b(table)}")
    _bulk_insert(cursor, f"{_b(schema)}.{_b(table)}", all_col_names, df, mssql_cols)
    cursor.commit()


def create_replace_view(conn: pyodbc.Connection, model: Model) -> None:
    """Create or alter a view using the query defined on model.source."""
    if model.source is None or model.source.query is None:
        raise ValueError(
            f"model.source.query must be set for {model.target.write_mode.value}"
        )
    schema = model.target.schema.resolved
    view = model.target.name
    query = cast(LiteralString, model.source.query)

    cursor = conn.cursor()
    cursor.execute(f"CREATE OR ALTER VIEW {_b(schema)}.{_b(view)} AS {query}")
    cursor.commit()
