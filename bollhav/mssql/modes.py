import logging
import pyodbc
import polars as pl
from typing import cast, LiteralString
from bollhav.model.model import Model
from bollhav.mssql.columns import MssqlColumn, MssqlType
from bollhav.mssql.schema import _bracket_quote, _col_type

logger = logging.getLogger(__name__)


def _bulk_insert(
    cursor: pyodbc.Cursor,
    target: str,
    col_names: list[str],
    df: pl.DataFrame,
    columns: list[MssqlColumn] | None = None,
    fast: bool = True,
) -> None:
    cols = ", ".join(_bracket_quote(c) for c in col_names)
    placeholders = ", ".join(["?"] * len(col_names))
    cursor.fast_executemany = fast
    if fast and columns:
        _set_input_sizes(cursor, columns)
    cursor.executemany(
        f"INSERT INTO {target} ({cols}) VALUES ({placeholders})", df.rows()
    )


def _input_size_for(col: MssqlColumn):
    """Return the pyodbc.setinputsizes tuple for one column.

    The point of being exhaustive here: any column position left as a bare `0`
    becomes "I don't know, driver autodetect" — and for at least
    variable-length string types, that lets pyodbc fall through to ODBC's
    Data-At-Execution streaming path (SQLParamData/SQLPutData), which is
    fragile for large batches and prone to mid-stream connection resets.
    Giving every column an explicit (sql_type, length, scale) tuple removes
    that uncertainty across the board.
    """
    t = col.data_type

    # Variable-length string types
    if t == MssqlType.NVARCHAR:
        # MAX (length=None) has to use streaming — value can be up to 2GB.
        return (pyodbc.SQL_WVARCHAR, col.length or 0, 0)
    if t == MssqlType.VARCHAR:
        return (pyodbc.SQL_VARCHAR, col.length or 0, 0)
    if t == MssqlType.CHAR:
        return (pyodbc.SQL_CHAR, col.length or 1, 0)

    # Date / time types — scale = fractional-second digits, drives both buffer
    # sizing and truncation behavior on the driver side.
    if t == MssqlType.DATE:
        return (pyodbc.SQL_TYPE_DATE, 10, 0)
    if t == MssqlType.TIME:
        scale = col.scale if col.scale is not None else 7
        return (pyodbc.SQL_TYPE_TIME, 8 + (scale + 1 if scale else 0), scale)
    if t == MssqlType.DATETIME:
        # DATETIME has fixed 3-digit fractional seconds; scale not user-settable.
        return (pyodbc.SQL_TYPE_TIMESTAMP, 23, 3)
    if t in (MssqlType.DATETIME2, MssqlType.DATETIMEOFFSET):
        scale = col.scale if col.scale is not None else 7
        return (
            pyodbc.SQL_TYPE_TIMESTAMP,
            19 + (scale + 1 if scale else 0),
            scale,
        )

    # Integer types
    if t == MssqlType.BIGINT:
        return (pyodbc.SQL_BIGINT, 19, 0)
    if t == MssqlType.INT:
        return (pyodbc.SQL_INTEGER, 10, 0)
    if t == MssqlType.SMALLINT:
        return (pyodbc.SQL_SMALLINT, 5, 0)
    if t == MssqlType.TINYINT:
        return (pyodbc.SQL_TINYINT, 3, 0)
    if t == MssqlType.BIT:
        return (pyodbc.SQL_BIT, 1, 0)

    # Floating point
    if t == MssqlType.FLOAT:
        return (pyodbc.SQL_DOUBLE, 53, 0)
    if t == MssqlType.REAL:
        return (pyodbc.SQL_REAL, 24, 0)

    # Decimal / numeric
    if t == MssqlType.DECIMAL:
        return (pyodbc.SQL_DECIMAL, col.precision or 18, col.scale or 0)
    if t == MssqlType.NUMERIC:
        return (pyodbc.SQL_NUMERIC, col.precision or 18, col.scale or 0)

    # GUID
    if t == MssqlType.UNIQUEIDENTIFIER:
        return (pyodbc.SQL_GUID, 36, 0)

    # Variable-length binary (MAX)
    if t == MssqlType.VARBINARY_MAX:
        return (pyodbc.SQL_VARBINARY, 0, 0)

    # If we get here, MssqlType has gained a value this function doesn't
    # cover yet — fail loudly rather than silently fall back to autodetect.
    raise ValueError(f"_input_size_for: unhandled MssqlType {t!r}")


def _set_input_sizes(cursor: pyodbc.Cursor, columns: list[MssqlColumn]) -> None:
    cursor.setinputsizes([_input_size_for(c) for c in columns])


def _merge_via_temp(
    cursor: pyodbc.Cursor,
    target_table: str,
    model: Model,
    df: pl.DataFrame,
    *,
    fast_executemany: bool = True,
) -> None:
    """MERGE one DataFrame into `target_table` via a session-scoped
    `#tmp` table.

    `target_table` is the fully-bracketed `[schema].[table]` string of
    whatever table this MERGE lands in — the real target for direct
    upserts, or a staging table for staged upserts. The merge keys come
    from `model.target.merge_key_columns` either way (staging mirrors
    the target's PK/unique constraint).

    Does not commit — the caller decides when to flush, so multiple
    operations can share a transaction."""
    mssql_cols = [c for c in model.target.columns if isinstance(c, MssqlColumn)]
    all_col_names = [c.name for c in mssql_cols]
    unique_col_names = [c.name for c in model.target.merge_key_columns]
    non_unique_col_names = [c for c in all_col_names if c not in unique_col_names]

    # `#tmp_` names are session-scoped in tempdb. Hash the target_table
    # so concurrent merges into different tables in the same session
    # don't collide on the temp name.
    temp = f"#tmp_merge_{abs(hash(target_table)) % 10_000_000:07d}"

    col_defs = ", ".join(f"{_bracket_quote(c.name)} {_col_type(c)}" for c in mssql_cols)
    on_clause = " AND ".join(
        f"target.{_bracket_quote(c)} = source.{_bracket_quote(c)}"
        for c in unique_col_names
    )
    insert_cols = ", ".join(_bracket_quote(c) for c in all_col_names)
    insert_vals = ", ".join(f"source.{_bracket_quote(c)}" for c in all_col_names)

    cursor.execute(f"IF OBJECT_ID('tempdb..{temp}') IS NOT NULL DROP TABLE {temp}")
    cursor.execute(f"CREATE TABLE {temp} ({col_defs})")
    _bulk_insert(
        cursor, temp, all_col_names, df, columns=mssql_cols, fast=fast_executemany
    )

    if non_unique_col_names:
        update_set = ", ".join(
            f"target.{_bracket_quote(c)} = source.{_bracket_quote(c)}"
            for c in non_unique_col_names
        )
        matched_clause = f"WHEN MATCHED THEN UPDATE SET {update_set}"
    else:
        matched_clause = ""

    cursor.execute(
        f"MERGE INTO {target_table} AS target "
        f"USING {temp} AS source ON {on_clause} "
        f"{matched_clause} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
    )


def merge(
    conn: pyodbc.Connection,
    model: Model,
    df: pl.DataFrame,
    fast_executemany: bool = True,
) -> None:
    """Upsert directly into the target table using MSSQL MERGE.

    Thin wrapper over `_merge_via_temp` that opens a cursor, runs the
    merge against the target table, and commits."""
    target_table = f"{_bracket_quote(model.target.schema_resolved)}.{_bracket_quote(model.target.name)}"
    cursor = conn.cursor()
    _merge_via_temp(cursor, target_table, model, df, fast_executemany=fast_executemany)
    cursor.commit()


def append(
    conn: pyodbc.Connection,
    model: Model,
    df: pl.DataFrame,
    fast_executemany: bool = True,
) -> None:
    """Bulk insert rows into target without clearing existing data."""
    schema = model.target.schema_resolved
    table = model.target.name
    all_col_names = [c.name for c in model.target.columns]

    cursor = conn.cursor()
    mssql_cols = [c for c in model.target.columns if isinstance(c, MssqlColumn)]
    _bulk_insert(
        cursor,
        f"{_bracket_quote(schema)}.{_bracket_quote(table)}",
        all_col_names,
        df,
        columns=mssql_cols,
        fast=fast_executemany,
    )
    cursor.commit()


def create_replace_view(conn: pyodbc.Connection, model: Model) -> None:
    """Create or alter a view using the query defined on its SourceModel."""
    from bollhav.model.source import SourceModel

    src = next(
        (
            s
            for s in model.upstream
            if isinstance(s.type, SourceModel) and s.type.query is not None
        ),
        None,
    )
    if src is None:
        raise ValueError(
            f"create_replace_view requires a Source with a SourceModel type "
            f"whose .query is set, in upstream=[...] on "
            f"{model.target.full_name!r}"
        )
    schema = model.target.schema_resolved
    view = model.target.name
    # The filter above guarantees this, but it doesn't narrow `src.type`.
    assert isinstance(src.type, SourceModel) and src.type.query is not None
    query = cast(LiteralString, src.type.query)

    cursor = conn.cursor()
    cursor.execute(
        f"CREATE OR ALTER VIEW {_bracket_quote(schema)}.{_bracket_quote(view)} AS {query}"
    )
    cursor.commit()
