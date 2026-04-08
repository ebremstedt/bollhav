import logging
import pyodbc
import polars as pl
from typing import cast, LiteralString
from bollhav.model.model import Model
from bollhav.mssql.columns import MssqlColumn
from bollhav.mssql.schema import _b, _col_type

logger = logging.getLogger(__name__)


def _bulk_insert(
    cursor: pyodbc.Cursor, target: str, col_names: list[str], df: pl.DataFrame
) -> None:
    cols = ", ".join(_b(c) for c in col_names)
    placeholders = ", ".join(["?"] * len(col_names))
    cursor.fast_executemany = True
    sizes = []
    for col in col_names:
        if df[col].dtype == pl.String:
            max_len = df[col].str.len_bytes().max()
            sizes.append((pyodbc.SQL_WVARCHAR, max(max_len or 0, 1), 0))
        else:
            sizes.append(None)
    cursor.setinputsizes(sizes)
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
    _bulk_insert(cursor, temp, all_col_names, df)

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
    all_col_names = [c.name for c in model.target.columns]

    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {_b(schema)}.{_b(table)}")
    _bulk_insert(cursor, f"{_b(schema)}.{_b(table)}", all_col_names, df)
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
