import logging
import pyodbc
from bollhav.model.model import Model
from bollhav.mssql.columns import MssqlColumn
from bollhav.mssql.indexes import MssqlIndex

logger = logging.getLogger(__name__)


def _bracket_quote(name: str) -> str:
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
    # PRIMARY KEY is added separately by ensure_primary_key so existing tables
    # also get the constraint and the constraint name is deterministic.
    null = " NOT NULL" if not col.nullable else ""
    return f"    {_bracket_quote(col.name)} {_col_type(col)}{null}"


def _index_ddl(schema: str, table: str, idx: MssqlIndex) -> str:
    unique = "UNIQUE " if idx.unique else ""
    cols = ", ".join(_bracket_quote(c) for c in idx.columns)
    include = (
        f" INCLUDE ({', '.join(_bracket_quote(c) for c in idx.included)})"
        if idx.included
        else ""
    )
    where = f" WHERE {idx.filter}" if idx.filter else ""
    return (
        f"CREATE {unique}NONCLUSTERED INDEX {_bracket_quote(idx.name)} "
        f"ON {_bracket_quote(schema)}.{_bracket_quote(table)} ({cols}){include}{where}"
    )


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
    schema = model.target.schema_resolved
    table = model.target.name_resolved
    logger.debug("Ensuring table: %s.%s", schema, table)

    mssql_cols = [c for c in model.target.columns if isinstance(c, MssqlColumn)]
    col_defs = ",\n".join(_col_ddl(c) for c in mssql_cols)

    cursor = conn.cursor()

    if model.target.recreate_table:
        logger.debug("Dropping table (recreate_table=True): %s.%s", schema, table)
        cursor.execute(
            f"IF OBJECT_ID(?, 'U') IS NOT NULL DROP TABLE {_bracket_quote(schema)}.{_bracket_quote(table)}",
            f"{schema}.{table}",
        )

    cursor.execute(
        f"IF NOT EXISTS ("
        f"    SELECT 1 FROM INFORMATION_SCHEMA.TABLES"
        f"    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
        f") CREATE TABLE {_bracket_quote(schema)}.{_bracket_quote(table)} (\n{col_defs}\n)",
        schema,
        table,
    )

    if model.target.truncate_table:
        logger.debug("Truncating table (truncate_table=True): %s.%s", schema, table)
        cursor.execute(
            f"TRUNCATE TABLE {_bracket_quote(schema)}.{_bracket_quote(table)}"
        )

    # Skip UQ for columns already covered by the PK — PRIMARY KEY enforces
    # uniqueness, so a parallel UQ on the same columns is redundant.
    pk_col_set = {c.name for c in mssql_cols if c.primary_key}
    unique_cols = [c for c in mssql_cols if c.unique and c.name not in pk_col_set]
    if unique_cols:
        constraint_name = f"{table}_uq"
        cols = ", ".join(_bracket_quote(c.name) for c in unique_cols)
        cursor.execute(
            f"IF NOT EXISTS ("
            f"    SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
            f"    WHERE CONSTRAINT_NAME = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ?"
            f") ALTER TABLE {_bracket_quote(schema)}.{_bracket_quote(table)}"
            f"    ADD CONSTRAINT {_bracket_quote(constraint_name)} UNIQUE ({cols})",
            constraint_name,
            schema,
            table,
        )

    cursor.commit()


def ensure_primary_key(conn: pyodbc.Connection, model: Model) -> None:
    """Add a CLUSTERED PRIMARY KEY named `<table>_pk` if any columns are flagged
    `primary_key=True` and the table doesn't already have a PK.

    Idempotent and safe on existing tables — re-running is a no-op once the PK
    is in place. Pairs with `_col_ddl` (which no longer emits inline PRIMARY
    KEY) so new and existing tables both get a deterministically named, clustered
    PK from this single code path.
    """
    schema = model.target.schema_resolved
    table = model.target.name_resolved
    mssql_cols = [c for c in model.target.columns if isinstance(c, MssqlColumn)]
    pk_cols = [c for c in mssql_cols if c.primary_key]
    if not pk_cols:
        return

    constraint_name = f"{table}_pk"
    cols = ", ".join(_bracket_quote(c.name) for c in pk_cols)
    obj = f"{_bracket_quote(schema)}.{_bracket_quote(table)}"
    logger.debug("Ensuring primary key on: %s.%s (%s)", schema, table, cols)

    cursor = conn.cursor()
    cursor.execute(
        f"IF NOT EXISTS ("
        f"    SELECT 1 FROM sys.key_constraints"
        f"    WHERE parent_object_id = OBJECT_ID(?)"
        f"      AND type = 'PK'"
        f") ALTER TABLE {obj}"
        f"    ADD CONSTRAINT {_bracket_quote(constraint_name)} PRIMARY KEY CLUSTERED ({cols})",
        f"{schema}.{table}",
    )
    cursor.commit()


def ensure_indexes(conn: pyodbc.Connection, model: Model) -> None:
    schema = model.target.schema_resolved
    table = model.target.name_resolved
    mssql_indexes = [i for i in model.target.indexes if isinstance(i, MssqlIndex)]
    if not mssql_indexes:
        return
    logger.debug("Ensuring %d index(es) on: %s.%s", len(mssql_indexes), schema, table)

    cursor = conn.cursor()
    for idx in mssql_indexes:
        cursor.execute(
            f"IF NOT EXISTS ("
            f"    SELECT 1 FROM sys.indexes"
            f"    WHERE name = ? AND object_id = OBJECT_ID(?)"
            f") {_index_ddl(schema, table, idx)}",
            idx.name,
            f"{schema}.{table}",
        )
    cursor.commit()


def ensure_schema_and_table(conn: pyodbc.Connection, model: Model) -> None:
    ensure_schema(conn=conn, schema=model.target.schema_resolved)
    ensure_table(conn=conn, model=model)
    ensure_primary_key(conn=conn, model=model)


def ensure_schema_table_and_indexes(conn: pyodbc.Connection, model: Model) -> None:
    ensure_schema(conn=conn, schema=model.target.schema_resolved)
    ensure_table(conn=conn, model=model)
    ensure_primary_key(conn=conn, model=model)
    ensure_indexes(conn=conn, model=model)
