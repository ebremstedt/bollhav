"""Cross-pipeline model library — the central registry of every
bollhav model the state DB has ever seen.

Each row records a model's identity (full target name), its upstream
dependencies, and a pointer to its state table (`<state_schema>.<state_table>`).
The library lives in `z_bollhav.model_library` in the state DB.

Used by `@load_models` during the state bootstrap to:
  1. Register / refresh each staged model on every run.
  2. Resolve upstream pointers so we can query the upstream's state
     table for `applied` rows that satisfy a downstream interval.

The library is the source of truth for "what models exist." Different
pipelines (different `TAGS` expressions) see the same view, so a
downstream model in pipeline A can reason about an upstream that
ships in pipeline B.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

import psycopg
from psycopg import sql

from bollhav.postgres import state as pg_state

if TYPE_CHECKING:
    from bollhav.model.model import Model

logger = logging.getLogger(__name__)


LIBRARY_SCHEMA = "z_bollhav"
LIBRARY_TABLE = "model_library"


# ── DDL ──────────────────────────────────────────────────────────────


_LIBRARY_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    full_name      TEXT PRIMARY KEY,
    upstream       TEXT[] NOT NULL,
    state_schema   TEXT NOT NULL,
    state_table    TEXT NOT NULL,
    last_seen      TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def ensure_library(conn: psycopg.Connection) -> None:
    """Create the library schema + table if absent. Idempotent."""
    with conn.transaction():
        conn.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(
                schema=sql.Identifier(LIBRARY_SCHEMA),
            )
        )
        conn.execute(
            sql.SQL(_LIBRARY_DDL).format(
                schema=sql.Identifier(LIBRARY_SCHEMA),
                table=sql.Identifier(LIBRARY_TABLE),
            )
        )
    logger.debug("library: ensured %s.%s", LIBRARY_SCHEMA, LIBRARY_TABLE)


# ── upsert ───────────────────────────────────────────────────────────


def register(conn: psycopg.Connection, model: "Model") -> None:
    """Upsert a library row for `model`. Overwrites `upstream`,
    `state_schema`, `state_table`, and `last_seen` if the row already
    exists — so renaming an upstream or moving the state table both
    propagate the next time the model is registered."""
    state_schema = pg_state._state_schema(model)
    state_table = pg_state._state_table(model)
    upsert = sql.SQL(
        "INSERT INTO {schema}.{table} "
        "(full_name, upstream, state_schema, state_table, last_seen) "
        "VALUES (%s, %s, %s, %s, now()) "
        "ON CONFLICT (full_name) DO UPDATE SET "
        "upstream = EXCLUDED.upstream, "
        "state_schema = EXCLUDED.state_schema, "
        "state_table = EXCLUDED.state_table, "
        "last_seen = EXCLUDED.last_seen"
    ).format(
        schema=sql.Identifier(LIBRARY_SCHEMA),
        table=sql.Identifier(LIBRARY_TABLE),
    )
    with conn.transaction():
        conn.execute(
            upsert,
            [model.target.full_name, list(model.upstream), state_schema, state_table],
        )
    logger.debug(
        "library: registered %s (upstream=%s)",
        model.target.full_name,
        list(model.upstream),
    )


# ── lookups ──────────────────────────────────────────────────────────


def lookup(
    conn: psycopg.Connection, full_name: str
) -> tuple[list[str], str, str] | None:
    """Return `(upstream, state_schema, state_table)` for a model in
    the library, or None if unregistered."""
    row = conn.execute(
        sql.SQL(
            "SELECT upstream, state_schema, state_table "
            "FROM {schema}.{table} WHERE full_name = %s"
        ).format(
            schema=sql.Identifier(LIBRARY_SCHEMA),
            table=sql.Identifier(LIBRARY_TABLE),
        ),
        [full_name],
    ).fetchone()
    if row is None:
        return None
    upstream, state_schema, state_table = row
    return list(upstream), state_schema, state_table


def is_satisfied(
    conn: psycopg.Connection,
    *,
    upstream_state_schema: str,
    upstream_state_table: str,
    since: datetime,
    until: datetime,
) -> bool:
    """Does the upstream have an `applied` row that satisfies
    `(since, until)`?

    A row satisfies when it either matches exactly or fully encapsulates
    the requested window — so a daily-cadence upstream covers an
    hourly-cadence downstream's intervals without coordination.

    If the upstream's state table doesn't exist yet (it was discovered
    and registered in the library, but has never been bootstrapped),
    returns False — no applied row can exist on a missing table. We
    check `pg_tables` first to avoid hitting `UndefinedTable` mid-tx,
    which would poison the connection's current transaction."""
    exists = conn.execute(
        "SELECT 1 FROM pg_tables WHERE schemaname=%s AND tablename=%s LIMIT 1",
        [upstream_state_schema, upstream_state_table],
    ).fetchone()
    if not exists:
        return False

    query = sql.SQL(
        "SELECT 1 FROM {schema}.{table} "
        "WHERE status = 'applied' "
        "  AND since <= %s AND until >= %s "
        "LIMIT 1"
    ).format(
        schema=sql.Identifier(upstream_state_schema),
        table=sql.Identifier(upstream_state_table),
    )
    row = conn.execute(query, [since, until]).fetchone()
    return row is not None


__all__ = [
    "LIBRARY_SCHEMA",
    "LIBRARY_TABLE",
    "ensure_library",
    "register",
    "lookup",
    "is_satisfied",
]
