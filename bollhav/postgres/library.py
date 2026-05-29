"""Cross-pipeline model library — the central registry of every
bollhav model the state DB has ever seen.

Each row records a model's identity (full target name), its upstream
dependencies, the model's type (TABLE or VIEW), and — for tables —
a pointer to its state table (`<state_schema>.<state_table>`). View
rows store NULL for `state_schema` / `state_table` because views
don't have state.

Used by `@load_models` during the state bootstrap to:
  1. Register / refresh each model on every run (tables and views).
  2. Resolve upstream pointers so we can check satisfaction:
     * TABLE upstream — query the upstream's state table for an
       `applied` row that covers the downstream interval.
     * VIEW upstream — presence in the library is the satisfaction.
       Views are time-agnostic; once the view-model has registered,
       the view exists, and that's all a downstream needs. No
       catalog (`pg_views`) query — the library is the source of
       truth, and models that create views are responsible for
       registering themselves on every run.

The library is the source of truth for "what models exist." Different
pipelines (different `TAGS` expressions) see the same view, so a
downstream model in pipeline A can reason about an upstream that
ships in pipeline B.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, NamedTuple

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
    model_type     TEXT NOT NULL,
    state_schema   TEXT,
    state_table    TEXT,
    last_seen      TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def ensure_library(conn: psycopg.Connection) -> None:
    """Create the library schema + table if absent. Idempotent.

    The library is shared across many pipelines run by different
    bollhav images — possibly different versions of bollhav at the
    same time. Schema changes therefore have to be **additive**:
    new columns must have a safe default so old images can keep
    inserting without filling them in, and `NOT NULL` may only be
    relaxed, never tightened. Drops are forbidden — they would
    brick concurrent writers.

    Migrations applied here (when the relevant condition is met):

    * `model_type` (added in the views-as-upstream refactor): added
      with `DEFAULT 'TABLE' NOT NULL`. Old-image inserts that omit
      it land as TABLE — the only kind the old code could write.
    * `state_schema` / `state_table`: relaxed to nullable, since
      view rows and library-only TABLE rows have no state pointers.

    Each migration is gated on a sentinel check so it runs at most
    once and is safe to retry."""
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
        _migrate_additively(conn)
    logger.debug("library: ensured %s.%s", LIBRARY_SCHEMA, LIBRARY_TABLE)


def _migrate_additively(conn: psycopg.Connection) -> None:
    """Apply additive ALTERs to bring an older `model_library` up to
    the current shape without breaking concurrent old-image writers.
    Runs inside the caller's transaction. Each step checks the
    information_schema before issuing the ALTER, so the function is
    idempotent and cheap to call on every bootstrap."""
    has_model_type = conn.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s "
        "  AND column_name = 'model_type' LIMIT 1",
        [LIBRARY_SCHEMA, LIBRARY_TABLE],
    ).fetchone()
    if not has_model_type:
        conn.execute(
            sql.SQL(
                "ALTER TABLE {schema}.{table} "
                "ADD COLUMN model_type TEXT NOT NULL DEFAULT 'TABLE'"
            ).format(
                schema=sql.Identifier(LIBRARY_SCHEMA),
                table=sql.Identifier(LIBRARY_TABLE),
            )
        )
        logger.info(
            "library: migrated %s.%s — added model_type column "
            "(default 'TABLE' so older images can keep writing)",
            LIBRARY_SCHEMA,
            LIBRARY_TABLE,
        )

    for col in ("state_schema", "state_table"):
        is_nullable = conn.execute(
            "SELECT is_nullable FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "  AND column_name = %s LIMIT 1",
            [LIBRARY_SCHEMA, LIBRARY_TABLE, col],
        ).fetchone()
        if is_nullable and is_nullable[0] == "NO":
            conn.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} ALTER COLUMN {col} DROP NOT NULL"
                ).format(
                    schema=sql.Identifier(LIBRARY_SCHEMA),
                    table=sql.Identifier(LIBRARY_TABLE),
                    col=sql.Identifier(col),
                )
            )
            logger.info(
                "library: migrated %s.%s — relaxed NOT NULL on %s "
                "(view / library-only rows store NULL here)",
                LIBRARY_SCHEMA,
                LIBRARY_TABLE,
                col,
            )


# ── upsert ───────────────────────────────────────────────────────────


def register(conn: psycopg.Connection, model: "Model") -> None:
    """Upsert a library row for `model`. Overwrites every field
    except `full_name` (the PK) and `last_seen` (always `now()`).
    So renaming an upstream, moving the state table, or flipping a
    TABLE to a VIEW all propagate the next time `register` runs.

    Models without a state table (views, and tables that opted in
    via `library=True` without staging) write NULL for `state_schema`
    / `state_table`. Their satisfaction check is just "is the row
    in the library?" — see `is_satisfied`.
    """
    is_view = model.target.is_view
    has_state_table = model.state is not None
    state_schema = pg_state._state_schema(model) if has_state_table else None
    state_table = pg_state._state_table(model) if has_state_table else None
    model_type = "VIEW" if is_view else "TABLE"
    upsert = sql.SQL(
        "INSERT INTO {schema}.{table} "
        "(full_name, upstream, model_type, state_schema, state_table, last_seen) "
        "VALUES (%s, %s, %s, %s, %s, now()) "
        "ON CONFLICT (full_name) DO UPDATE SET "
        "upstream = EXCLUDED.upstream, "
        "model_type = EXCLUDED.model_type, "
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
            [
                model.target.full_name,
                list(model.upstream),
                model_type,
                state_schema,
                state_table,
            ],
        )
    logger.debug(
        "library: registered %s (model_type=%s, upstream=%s)",
        model.target.full_name,
        model_type,
        list(model.upstream),
    )


# ── lookups ──────────────────────────────────────────────────────────


class LibraryEntry(NamedTuple):
    """A row read from the library. `state_schema` / `state_table`
    are None for VIEW entries (views don't have state tables)."""

    upstream: list[str]
    model_type: str
    state_schema: str | None
    state_table: str | None


def lookup(conn: psycopg.Connection, full_name: str) -> LibraryEntry | None:
    """Return the `LibraryEntry` for a model, or None if unregistered."""
    row = conn.execute(
        sql.SQL(
            "SELECT upstream, model_type, state_schema, state_table "
            "FROM {schema}.{table} WHERE full_name = %s"
        ).format(
            schema=sql.Identifier(LIBRARY_SCHEMA),
            table=sql.Identifier(LIBRARY_TABLE),
        ),
        [full_name],
    ).fetchone()
    if row is None:
        return None
    upstream, model_type, state_schema, state_table = row
    return LibraryEntry(
        upstream=list(upstream),
        model_type=model_type,
        state_schema=state_schema,
        state_table=state_table,
    )


def is_satisfied(
    conn: psycopg.Connection,
    *,
    entry: LibraryEntry,
    since: datetime,
    until: datetime,
) -> bool:
    """Is the upstream satisfied for the window `[since, until)`?

    Two branches, decided by whether the library row carries state
    pointers:

    * `state_schema` / `state_table` are NULL — presence in the
      library is the satisfaction proof. This covers VIEW upstreams
      (always library-only) and TABLE upstreams that opted in to
      registration without state tracking (`library=True`). Either
      way, downstream intervals are always satisfied by the row's
      mere presence.
    * `state_schema` / `state_table` are set — look for an `applied`
      row in the upstream's state table that matches exactly or fully
      encapsulates the window. A daily-cadence upstream therefore
      covers an hourly-cadence downstream without coordination. If
      the state table doesn't exist yet (registered but never
      bootstrapped), returns False.
    """
    if entry.state_schema is None or entry.state_table is None:
        return True

    exists = conn.execute(
        "SELECT 1 FROM pg_tables WHERE schemaname=%s AND tablename=%s LIMIT 1",
        [entry.state_schema, entry.state_table],
    ).fetchone()
    if not exists:
        return False

    query = sql.SQL(
        "SELECT 1 FROM {schema}.{table} "
        "WHERE status = 'applied' "
        "  AND since <= %s AND until >= %s "
        "LIMIT 1"
    ).format(
        schema=sql.Identifier(entry.state_schema),
        table=sql.Identifier(entry.state_table),
    )
    row = conn.execute(query, [since, until]).fetchone()
    return row is not None


__all__ = [
    "LIBRARY_SCHEMA",
    "LIBRARY_TABLE",
    "LibraryEntry",
    "ensure_library",
    "register",
    "lookup",
    "is_satisfied",
]
