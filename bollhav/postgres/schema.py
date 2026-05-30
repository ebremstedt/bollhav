"""Thin backwards-compatibility shim over `bollhav.postgres.actions`.

Historically `ensure_schema_and_table(conn, model)` did the full
schema + table + indexes + uniques setup inline, gated by a
`Mutations` struct on the target. That now lives in the action
system: each step is an `Action` in `default_actions()`, the
runner manages "did this fire?" state, and users plug in their own
DDL by appending Actions.

This module keeps two helpers used by other parts of the codebase
(`ensure_schema` for the state-table bootstrap, `_col_ddl` for the
staging table DDL), plus an `ensure_schema_and_table` wrapper that
forwards to `run_pre_model_actions` so the old import path still works.
New code should call `run_pre_model_actions` directly.
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
    """Backwards-compatible entry point — forwards to `run_pre_model_actions`.
    New code should import and call `run_pre_model_actions` directly."""
    from bollhav.postgres.actions import run_pre_model_actions

    run_pre_model_actions(conn, model)


def ensure_table(conn: psycopg.Connection, model: Model) -> None:
    """Backwards-compatible entry point — forwards to `run_pre_model_actions`.
    The old split between `ensure_table` and `ensure_schema_and_table`
    is gone; both now run the same PRE action set."""
    from bollhav.postgres.actions import run_pre_model_actions

    run_pre_model_actions(conn, model)
