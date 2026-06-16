from __future__ import annotations

import logging
from typing import cast, LiteralString

import psycopg
from psycopg import sql

from bollhav.model.model import Model
from bollhav.postgres.columns import PostgresColumn

logger = logging.getLogger(__name__)


def _col_ddl(col: PostgresColumn) -> LiteralString:
    pg_type = col.data_type.value
    if col.precision is not None and col.scale is not None:
        pg_type = f"{pg_type}({col.precision}, {col.scale})"
    elif col.length is not None:
        pg_type = f"{pg_type}({col.length})"
    constraints = ""
    if col.primary_key:
        constraints = " PRIMARY KEY"
    null_clause = "NOT NULL" if not col.nullable else ""
    return cast(
        LiteralString, f'    "{col.name}" {pg_type}{constraints} {null_clause}'.rstrip()
    )


def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    """Idempotent `CREATE SCHEMA IF NOT EXISTS`. Used by the state
    bootstrap (which needs its `z_<schema>` ahead of any actions),
    not gated by the action system."""
    logger.debug("Ensuring schema: %s", schema)
    conn.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
    )


def ensure_schema_and_table(conn: psycopg.Connection, model: Model) -> None:
    """Public façade — idempotently ensure the model's target assets
    (schema, table, indexes, unique constraint, staging schema). A
    verb-style entrypoint over `PostgresData.ensure_assets()`."""
    from bollhav.postgres.data import PostgresData

    PostgresData(model=model, conn=conn).ensure_assets()


def ensure_table(conn: psycopg.Connection, model: Model) -> None:
    """Synonym for `ensure_schema_and_table` — both ensure the same
    target assets via `PostgresData.ensure_assets()`."""
    from bollhav.postgres.data import PostgresData

    PostgresData(model=model, conn=conn).ensure_assets()
