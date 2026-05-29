"""Postgres backend for bollhav state tracking.

Each state-enabled model gets one table in the target DB:

    z_<target_schema>.<target_name>_state

The `z_` prefix keeps bollhav-owned tables out of the user's schemas
and sorts them to the bottom of a DB editor's schema list.

State lives in the target DB so the staging-flush transaction can flip
the state row atomically with the data move (`bollhav.postgres.staging`).
A separate state DB would break that guarantee — `State.dsn_env_var`
is reserved for a future cross-DB option but is not honored here.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

import psycopg
from psycopg import sql

if TYPE_CHECKING:
    from bollhav.model.model import Model
    from bollhav.model.state import StateMode

logger = logging.getLogger(__name__)


# ── connection / naming ──────────────────────────────────────────────


def _resolve_dsn(model: "Model") -> str:
    """State.dsn_env_var wins when set; otherwise fall back to the
    target's DSN env var (state shares the target's database)."""
    env_var = (
        model.state.dsn_env_var if model.state is not None else None
    ) or model.target.dsn_env_var
    if not env_var:
        raise ValueError(
            f"state tracking on model {model.target.full_name!r} requires "
            f"state.dsn_env_var or target.dsn_env_var to be set"
        )
    dsn = os.environ.get(env_var)
    if not dsn:
        raise ValueError(
            f"state DSN env var {env_var!r} is unset for model "
            f"{model.target.full_name!r}"
        )
    return dsn


def _state_schema(model: "Model") -> str:
    """State schema name. `State.schema_prefix` overrides the default
    `"z_"` prefix on the target's schema name."""
    prefix = (
        model.state.schema_prefix
        if model.state is not None and model.state.schema_prefix is not None
        else "z_"
    )
    return f"{prefix}{model.target.schema.resolved}"


def _state_table(model: "Model") -> str:
    """State table name. `State.table_suffix` overrides the default
    `"_state"` suffix on the target's table name."""
    suffix = (
        model.state.table_suffix
        if model.state is not None and model.state.table_suffix is not None
        else "_state"
    )
    return f"{model.target.name}{suffix}"


def _connect(model: "Model") -> psycopg.Connection:
    """Connect to the state DB. On failure, raise a wrapped error that
    names the model and the env var used, so the operator can tell at
    a glance whether the issue is the state DSN, the target DSN, or
    the database itself."""
    env_var = (
        model.state.dsn_env_var if model.state is not None else None
    ) or model.target.dsn_env_var
    source = (
        "state.dsn_env_var"
        if model.state is not None and model.state.dsn_env_var
        else "target.dsn_env_var (state.dsn_env_var unset, falling back)"
    )
    dsn = _resolve_dsn(model)
    try:
        return psycopg.connect(dsn)
    except psycopg.OperationalError as exc:
        raise ConnectionError(
            f"state DB unreachable for model {model.target.full_name!r}: "
            f"resolved via {source} → env var {env_var!r}. "
            f"psycopg: {exc}"
        ) from exc


# ── DDL ──────────────────────────────────────────────────────────────


_STATE_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL,
    since       TIMESTAMPTZ NOT NULL,
    until       TIMESTAMPTZ NOT NULL,
    status      TEXT NOT NULL,
    applied_at  TIMESTAMPTZ,
    UNIQUE (since, until)
)
"""

_STATE_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (status)
"""


def ensure_tables(model: "Model") -> None:
    """Create the state schema and per-model state table if absent.
    Idempotent."""
    schema = _state_schema(model)
    table = _state_table(model)
    index = f"{table}_status_idx"

    with _connect(model) as conn:
        with conn.transaction():
            conn.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(
                    schema=sql.Identifier(schema),
                )
            )
            conn.execute(
                sql.SQL(_STATE_DDL).format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table),
                )
            )
            conn.execute(
                sql.SQL(_STATE_INDEX_DDL).format(
                    index=sql.Identifier(index),
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table),
                )
            )
    logger.debug(
        "state: ensured tables for %s (%s.%s)",
        model.target.full_name,
        schema,
        table,
    )


# ── pre-fill ─────────────────────────────────────────────────────────


def prefill(
    model: "Model",
    *,
    run_id: UUID,
    intervals: list,
    state_mode: "StateMode",
) -> None:
    """Insert one pending row per interval.

    RESPECT    → ON CONFLICT DO NOTHING (applied rows survive untouched)
    DISRESPECT → ON CONFLICT DO UPDATE (every row reset to pending)
    """
    from bollhav.model.state import StateMode

    if not intervals:
        return

    schema = _state_schema(model)
    table = _state_table(model)

    if state_mode is StateMode.RESPECT:
        on_conflict = sql.SQL("ON CONFLICT (since, until) DO NOTHING")
    elif state_mode is StateMode.DISRESPECT:
        on_conflict = sql.SQL(
            "ON CONFLICT (since, until) DO UPDATE SET "
            "status = 'pending', applied_at = NULL, run_id = EXCLUDED.run_id"
        )
    else:
        raise ValueError(f"unknown state_mode: {state_mode!r}")

    insert = sql.SQL(
        "INSERT INTO {schema}.{table} (run_id, since, until, status) "
        "VALUES (%s, %s, %s, 'pending') {on_conflict}"
    ).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
        on_conflict=on_conflict,
    )

    with _connect(model) as conn:
        with conn.transaction():
            for interval in intervals:
                conn.execute(
                    insert,
                    [str(run_id), interval.since, interval.until],
                )
    logger.debug(
        "state: prefilled %d intervals for %s (mode=%s)",
        len(intervals),
        model.target.full_name,
        state_mode.value,
    )


# ── decorator hooks ──────────────────────────────────────────────────


def read_pending(model: "Model") -> list:
    """Return every (since, until) row with status='pending' as a list
    of `TZInterval`, ordered oldest first.

    Used by `@load_models` to filter `model.intervals` down to the
    intervals that still need work — an empty list means there's
    nothing to do and the user's loop exits cleanly."""
    from bollhav.model.intervals import TZInterval

    schema = _state_schema(model)
    table = _state_table(model)
    query = sql.SQL(
        "SELECT since, until FROM {schema}.{table} "
        "WHERE status = 'pending' "
        "ORDER BY since, until"
    ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))

    with _connect(model) as conn:
        rows = conn.execute(query).fetchall()
    logger.debug(
        "state: read %d pending intervals for %s",
        len(rows),
        model.target.full_name,
    )
    return [TZInterval(since=since, until=until) for since, until in rows]


def is_applied(model: "Model", *, since: datetime, until: datetime) -> bool:
    schema = _state_schema(model)
    table = _state_table(model)
    query = sql.SQL(
        "SELECT 1 FROM {schema}.{table} "
        "WHERE since = %s AND until = %s AND status = 'applied' "
        "LIMIT 1"
    ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))

    with _connect(model) as conn:
        row = conn.execute(query, [since, until]).fetchone()
    return row is not None


def mark_applied(
    model: "Model",
    *,
    run_id: UUID,
    since: datetime,
    until: datetime,
) -> None:
    schema = _state_schema(model)
    table = _state_table(model)
    update = sql.SQL(
        "UPDATE {schema}.{table} "
        "SET status = 'applied', applied_at = now(), run_id = %s "
        "WHERE since = %s AND until = %s"
    ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))

    with _connect(model) as conn:
        with conn.transaction():
            conn.execute(update, [str(run_id), since, until])
    logger.debug(
        "state: marked applied %s..%s for %s",
        since,
        until,
        model.target.full_name,
    )


__all__ = [
    "ensure_tables",
    "prefill",
    "read_pending",
    "is_applied",
    "mark_applied",
]
