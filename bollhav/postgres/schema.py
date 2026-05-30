"""Schema helpers shared between the action system and other modules.

The full PRE_MODEL setup (CREATE SCHEMA / CREATE TABLE / DROP /
TRUNCATE / CREATE INDEX / ADD UNIQUE) lives as `Action` entries in
`bollhav.postgres.actions.default_actions()`. Call
`run_pre_model_actions(conn, model)` to fire them.

This module exposes:
  * `ensure_schema` — idempotent CREATE SCHEMA used by the state-
    table bootstrap (which has to create `z_<schema>` ahead of any
    actions, since the actions need it).
  * `_col_ddl` — renders one column-definition line, used by both
    the framework's CREATE TABLE action and the staging table DDL.
  * `ensure_schema_and_table` and `ensure_table` — public façades
    over `run_pre_model_actions`. Same effect: run every applicable
    PRE_MODEL action against this model's target. The two names are
    synonyms; both exist as friendly aliases for users who want a
    verb-style entrypoint rather than reaching into the action
    runner directly.
"""

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
    """Public façade — run every applicable PRE_MODEL action for the
    given model. Equivalent to calling `run_pre_model_actions` directly;
    kept as a verb-style entrypoint for callers who don't want to reach
    into the action-runner module."""
    from bollhav.postgres.actions import run_pre_model_actions

    run_pre_model_actions(conn, model)


def ensure_table(conn: psycopg.Connection, model: Model) -> None:
    """Synonym for `ensure_schema_and_table`. Both run the same PRE
    action set; the split between them stopped being meaningful once
    schema-creation became part of the action list rather than a
    separate ensure step."""
    from bollhav.postgres.actions import run_pre_model_actions

    run_pre_model_actions(conn, model)
