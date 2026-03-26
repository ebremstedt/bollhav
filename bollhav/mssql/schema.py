from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyodbc

from bollhav.model.model import Model
from bollhav.mssql.columns import MssqlColumn

logger = logging.getLogger(__name__)


def _b(name: str) -> str:
    """Bracket-quote an MSSQL identifier."""
    return "[" + name.replace("]", "]]") + "]"


def _col_type(col: MssqlColumn) -> str:
    t = col.data_type.value
    if t in ("DECIMAL", "NUMERIC"):
        if col.precision is not None and col.scale is not None:
            return f"{t}({col.precision}, {col.scale})"
        if col.precision is not None:
            return f"{t}({col.precision})"
    elif t in ("NVARCHAR", "VARCHAR", "CHAR"):
        length = col.length if col.length is not None else "MAX"
        return f"{t}({length})"
    elif t == "DATETIME2" and col.scale is not None:
        return f"{t}({col.scale})"
    return t


def _col_ddl(col: MssqlColumn) -> str:
    pk = " PRIMARY KEY" if col.primary_key else ""
    null = " NOT NULL" if not col.nullable else ""
    return f"    {_b(col.name)} {_col_type(col)}{pk}{null}"


def ensure_schema(conn: pyodbc.Connection, schema: str) -> None:
    logger.debug("Ensuring schema: %s", schema)
    cursor = conn.cursor()
    cursor.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = ?) "
        "BEGIN DECLARE @s NVARCHAR(MAX) = N'CREATE SCHEMA ' + QUOTENAME(?); EXEC(@s) END",
        schema,
        schema,
    )
    cursor.commit()


def ensure_table(conn: pyodbc.Connection, model: Model) -> None:
    schema = model.target.schema.resolved
    table = model.target.name
    logger.debug("Ensuring table: %s.%s", schema, table)

    mssql_cols = [c for c in model.target.columns if isinstance(c, MssqlColumn)]
    col_defs = ",\n".join(_col_ddl(c) for c in mssql_cols)

    cursor = conn.cursor()
    cursor.execute(
        f"IF NOT EXISTS ("
        f"    SELECT 1 FROM INFORMATION_SCHEMA.TABLES"
        f"    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
        f") CREATE TABLE {_b(schema)}.{_b(table)} (\n{col_defs}\n)",
        schema,
        table,
    )

    unique_cols = [c for c in mssql_cols if c.unique]
    if unique_cols:
        constraint_name = f"{table}_uq"
        cols = ", ".join(_b(c.name) for c in unique_cols)
        cursor.execute(
            f"IF NOT EXISTS ("
            f"    SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
            f"    WHERE CONSTRAINT_NAME = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ?"
            f") ALTER TABLE {_b(schema)}.{_b(table)}"
            f"    ADD CONSTRAINT {_b(constraint_name)} UNIQUE ({cols})",
            constraint_name,
            schema,
            table,
        )

    cursor.commit()


def ensure_schema_and_table(conn: pyodbc.Connection, model: Model) -> None:
    ensure_schema(conn=conn, schema=model.target.schema.resolved)
    ensure_table(conn=conn, model=model)
