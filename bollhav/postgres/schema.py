from __future__ import annotations

import logging

import psycopg
from psycopg import sql

from bollhav.model.model import Model

logger = logging.getLogger(__name__)


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
