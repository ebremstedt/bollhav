from __future__ import annotations

# ── DDL ──────────────────────────────────────────────────────────────


# `since`/`until` are nullable: a temporal (batched) row carries its window,
# while a timeless or one-shot row carries a NULL window — the unit of work is
# "the whole thing" / "the view exists", not a time slice. `temporality` is the
# model's `temporal` | `timeless` value. The table UNIQUE keeps one row per
# window; the partial index below keeps exactly one NULL-window (oneshot) row.
_STATE_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id              BIGSERIAL PRIMARY KEY,
    model_name      TEXT NOT NULL,
    run_id          UUID NOT NULL,
    since           TIMESTAMPTZ,
    until           TIMESTAMPTZ,
    status          TEXT NOT NULL,
    blocked_reason  TEXT,
    applied_at      TIMESTAMPTZ,
    temporality     TEXT NOT NULL DEFAULT 'temporal',
    UNIQUE (since, until)
)
"""

_STATE_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (status)
"""

# Exactly one NULL-window (oneshot) row per state table. Indexing the
# expression `(since IS NULL)` — always TRUE for matching rows — makes the
# uniqueness independent of `temporality`, so a table can't hold two NULL-window
# rows. `ON CONFLICT ((since IS NULL)) WHERE since IS NULL` upserts it.
_STATE_ONESHOT_INDEX_DDL = """
CREATE UNIQUE INDEX IF NOT EXISTS {index} ON {schema}.{table} ((since IS NULL)) WHERE since IS NULL
"""

# One shared, central errors table — every model logs here, keyed by
# `full_name`. `since`/`until` are nullable: a timeless / one-shot failure
# logs a NULL window (it has no time slice).
_ERRORS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id              BIGSERIAL PRIMARY KEY,
    full_name       TEXT NOT NULL,
    run_id          UUID NOT NULL,
    since           TIMESTAMPTZ,
    until           TIMESTAMPTZ,
    error_type      TEXT NOT NULL,
    error_message   TEXT,
    traceback       TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

_ERRORS_MODEL_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (full_name)
"""

_ERRORS_TIME_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (created_at DESC)
"""


# ── cross-pipeline model library ─────────────────────────────────────
#
# The central registry of every bollhav model the state DB has ever
# seen — one shared table, keyed by full target name. Used during the
# state bootstrap to register each model and to resolve upstream
# satisfaction (a TABLE upstream is satisfied by an applied state row
# covering the window; a VIEW / library-only upstream by mere presence).
# The library is shared across pipelines run by different bollhav images,
# so its schema changes must be **additive** (see `_migrate_library_additively`).

LIBRARY_SCHEMA = "z_bollhav"
LIBRARY_TABLE = "library"
# Central errors table, shared by every model (keyed by full_name), in the
# same bollhav-owned schema as the library.
ERRORS_TABLE = "errors"

_LIBRARY_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    full_name      TEXT PRIMARY KEY,
    upstream       TEXT[] NOT NULL,
    sources        JSONB NOT NULL DEFAULT '[]'::jsonb,
    model_type     TEXT NOT NULL,
    state_schema   TEXT,
    state_table    TEXT,
    temporality    TEXT NOT NULL DEFAULT 'temporal',
    fixed_intervals BOOLEAN NOT NULL DEFAULT true,
    metadata       JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    last_seen      TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""
