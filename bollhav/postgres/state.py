"""Postgres backend for bollhav state tracking.

Each state-enabled model gets two tables in the state DB:

    <state_schema>.<target_name>_state
    <state_schema>.<target_name>_errors

`<state_schema>` resolves as follows:
    * separate state DB (model.state.dsn_env_var is set) →
        same schema as target.schema.resolved (e.g. `public`).
    * fallback to target DSN (state.dsn_env_var is None) →
        `z_<target.schema.resolved>` so the bollhav tables sort to the
        bottom of a DB editor's schema list and don't clutter the
        user's own schemas.
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
    """Pick the DSN to use for the state tables of this model.

    State DSN env var wins; otherwise we fall back to the target's DSN
    env var. The actual DSN string is read from the environment."""
    env_var = model.state.dsn_env_var or model.target.dsn_env_var
    if not env_var:
        raise ValueError(
            f"state tracking on model {model.target.full_name!r} requires "
            f"either state.dsn_env_var or target.dsn_env_var to be set"
        )
    dsn = os.environ.get(env_var)
    if not dsn:
        raise ValueError(
            f"state DSN env var {env_var!r} is unset for model "
            f"{model.target.full_name!r}"
        )
    return dsn


def _state_schema(model: "Model") -> str:
    """Schema name where this model's state tables live.

    Auto-prefixed with `z_` when state DSN falls back to target DSN —
    keeps bollhav's tables at the bottom of the schema list."""
    target_schema = model.target.schema.resolved
    if model.state.dsn_env_var is None:
        return f"z_{target_schema}"
    return target_schema


def _state_table(model: "Model") -> str:
    return f"{model.target.name}_state"


def _errors_table(model: "Model") -> str:
    return f"{model.target.name}_errors"


def _connect(model: "Model") -> psycopg.Connection:
    return psycopg.connect(_resolve_dsn(model))


# ── DDL ──────────────────────────────────────────────────────────────


_STATE_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              UUID NOT NULL,
    since               TIMESTAMPTZ NOT NULL,
    until               TIMESTAMPTZ NOT NULL,
    status              TEXT NOT NULL,
    directive_mode      TEXT NOT NULL,
    interval_expression TEXT NOT NULL,
    applied_at          TIMESTAMPTZ,
    UNIQUE (since, until)
)
"""

_STATE_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (status)
"""

_ERRORS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id            BIGSERIAL PRIMARY KEY,
    run_id        UUID NOT NULL,
    since         TIMESTAMPTZ NOT NULL,
    until         TIMESTAMPTZ NOT NULL,
    error_type    TEXT NOT NULL,
    error_message TEXT NOT NULL,
    traceback     TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

_ERRORS_RUN_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (run_id)
"""

_ERRORS_TIME_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (created_at DESC)
"""


def ensure_tables(model: "Model") -> None:
    """Create the state schema + per-model state and errors tables if
    they don't already exist. Idempotent."""
    schema = _state_schema(model)
    state_table = _state_table(model)
    errors_table = _errors_table(model)
    state_index = f"{state_table}_status_idx"
    errors_run_index = f"{errors_table}_run_id_idx"
    errors_time_index = f"{errors_table}_created_at_idx"
    logger.debug(
        "state: ensuring tables for %s — %s.%s, %s.%s (log_errors=%s)",
        model.target.full_name,
        schema,
        state_table,
        schema,
        errors_table,
        model.state.log_errors,
    )

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
                    table=sql.Identifier(state_table),
                )
            )
            conn.execute(
                sql.SQL(_STATE_INDEX_DDL).format(
                    index=sql.Identifier(state_index),
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(state_table),
                )
            )
            if model.state.log_errors:
                conn.execute(
                    sql.SQL(_ERRORS_DDL).format(
                        schema=sql.Identifier(schema),
                        table=sql.Identifier(errors_table),
                    )
                )
                conn.execute(
                    sql.SQL(_ERRORS_RUN_INDEX_DDL).format(
                        index=sql.Identifier(errors_run_index),
                        schema=sql.Identifier(schema),
                        table=sql.Identifier(errors_table),
                    )
                )
                conn.execute(
                    sql.SQL(_ERRORS_TIME_INDEX_DDL).format(
                        index=sql.Identifier(errors_time_index),
                        schema=sql.Identifier(schema),
                        table=sql.Identifier(errors_table),
                    )
                )


# ── pre-fill ─────────────────────────────────────────────────────────


def prefill(
    model: "Model",
    *,
    run_id: UUID,
    intervals: list,
    directive_mode: str,
    state_mode: "StateMode",
) -> None:
    """Insert one pending row per interval. Behavior depends on state_mode:

    RESPECT    → ON CONFLICT DO NOTHING (applied rows survive untouched)
    DISRESPECT → ON CONFLICT DO UPDATE (every row reset to pending)
    """
    from bollhav.model.state import StateMode

    if not intervals:
        return

    schema = _state_schema(model)
    table = _state_table(model)
    interval_expression = model.batching.interval.expression

    if state_mode is StateMode.RESPECT:
        on_conflict = sql.SQL("ON CONFLICT (since, until) DO NOTHING")
    elif state_mode is StateMode.DISRESPECT:
        on_conflict = sql.SQL(
            "ON CONFLICT (since, until) DO UPDATE SET "
            "status = 'pending', "
            "applied_at = NULL, "
            "run_id = EXCLUDED.run_id, "
            "directive_mode = EXCLUDED.directive_mode, "
            "interval_expression = EXCLUDED.interval_expression"
        )
    else:
        raise ValueError(f"unknown state_mode: {state_mode!r}")

    insert = sql.SQL(
        "INSERT INTO {schema}.{table} "
        "(run_id, since, until, status, directive_mode, interval_expression) "
        "VALUES (%s, %s, %s, 'pending', %s, %s) {on_conflict}"
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
                    [
                        str(run_id),
                        interval.since,
                        interval.until,
                        directive_mode,
                        interval_expression,
                    ],
                )
    logger.debug(
        "state: prefilled %d intervals for %s (mode=%s, %s)",
        len(intervals),
        model.target.full_name,
        state_mode.value,
        directive_mode,
    )


# ── decorator hooks ──────────────────────────────────────────────────


def reset_all_to_pending(model: "Model", *, run_id: UUID) -> None:
    """Flip every row in the state table back to status='pending' and
    clear applied_at. Used by DISCOVER + DISRESPECT to redo the whole
    state table from scratch."""
    schema = _state_schema(model)
    table = _state_table(model)
    update = sql.SQL(
        "UPDATE {schema}.{table} "
        "SET status = 'pending', applied_at = NULL, run_id = %s"
    ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))

    with _connect(model) as conn:
        with conn.transaction():
            cursor = conn.execute(update, [str(run_id)])
    logger.debug(
        "state: reset %d rows to pending for %s",
        cursor.rowcount,
        model.target.full_name,
    )


def read_pending(model: "Model") -> list:
    """Return every (since, until) row with status='pending' as a list of
    TZInterval. Used by DISCOVER mode to drive a run from existing state
    rather than computing intervals from bounds/backfill.

    Returned in (since, until) order so re-runs process them oldest-first."""
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
        "state: discovered %d pending intervals for %s",
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


def record_error(
    model: "Model",
    *,
    run_id: UUID,
    since: datetime,
    until: datetime,
    error_type: str,
    error_message: str,
    traceback_text: str | None,
) -> None:
    schema = _state_schema(model)
    table = _errors_table(model)
    insert = sql.SQL(
        "INSERT INTO {schema}.{table} "
        "(run_id, since, until, error_type, error_message, traceback) "
        "VALUES (%s, %s, %s, %s, %s, %s)"
    ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))

    with _connect(model) as conn:
        with conn.transaction():
            conn.execute(
                insert,
                [str(run_id), since, until, error_type, error_message, traceback_text],
            )
    logger.debug(
        "state: recorded error for %s (%s..%s): %s: %s",
        model.target.full_name,
        since,
        until,
        error_type,
        error_message,
    )


__all__ = [
    "ensure_tables",
    "prefill",
    "reset_all_to_pending",
    "read_pending",
    "is_applied",
    "mark_applied",
    "record_error",
]
