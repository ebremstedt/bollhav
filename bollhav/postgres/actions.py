"""Postgres action implementations: the run-callables for built-in
actions, the `default_actions()` factory, and the runners that drive
PRE and POST phases.

The runners are the entry point. `run_pre_model_actions(conn, model)` is
called from `bollhav.postgres.write_modes.write` before any data
movement; `run_post_model_actions(conn, model)` is called from
`@load_models` after the user's loop returns cleanly.

User-extended actions plug in by passing a list to `Target(actions=...)`:

    Target(
        ...,
        actions=[
            *default_actions(),
            Action("grant_analytics", Level.MODEL, Phase.POST,
                   run=lambda c, m: c.execute("GRANT SELECT ON ...")),
        ],
    )

See `bollhav/model/actions.py` for the type definitions and
docs/content/ACTIONS.md for the full lifecycle story.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast, LiteralString

import psycopg
from psycopg import sql

from bollhav.model.actions import Action, Level, Phase

if TYPE_CHECKING:
    from bollhav.model.database import DatabaseColumn
    from bollhav.model.model import Model
    from bollhav.model.target import Target

logger = logging.getLogger(__name__)


# ── helpers shared with schema.py / staging.py ─────────────────────


def _col_ddl(col: "DatabaseColumn") -> LiteralString:
    """Render one column-definition line for a CREATE TABLE. Skips
    non-PostgresColumn entries with an empty string — the caller's
    `if isinstance(...)` filter normally prevents them anyway."""
    from bollhav.postgres.columns import PostgresColumn

    if not isinstance(col, PostgresColumn):
        return cast(LiteralString, "")
    pg_type = col.data_type.value
    if col.precision is not None and col.scale is not None:
        pg_type = f"{pg_type}({col.precision}, {col.scale})"
    elif col.length is not None:
        pg_type = f"{pg_type}({col.length})"
    constraints = " PRIMARY KEY" if col.primary_key else ""
    null_clause = "NOT NULL" if not col.nullable else ""
    return cast(
        LiteralString,
        f'    "{col.name}" {pg_type}{constraints} {null_clause}'.rstrip(),
    )


def _staging_schema_name(model: "Model") -> str:
    from bollhav.postgres.state import PostgresState

    if model.target.staging is not None and model.target.staging.schema:
        return model.target.staging.schema
    return PostgresState(model)._state_schema()


# ── PRE action implementations ─────────────────────────────────────


def _run_schema_created(conn: psycopg.Connection, model: "Model") -> None:
    conn.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(model.target.schema.resolved)
        )
    )


def _run_recreated(conn: psycopg.Connection, model: "Model") -> None:
    conn.execute(
        sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
            sql.Identifier(model.target.schema.resolved),
            sql.Identifier(model.target.name_resolved),
        )
    )


def _run_table_created(conn: psycopg.Connection, model: "Model") -> None:
    from bollhav.postgres.columns import PostgresColumn

    col_defs = sql.SQL(",\n").join(
        sql.SQL(_col_ddl(c))
        for c in model.target.columns
        if isinstance(c, PostgresColumn)
    )
    conn.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} (\n{}\n)").format(
            sql.Identifier(model.target.schema.resolved),
            sql.Identifier(model.target.name_resolved),
            col_defs,
        )
    )


def _run_truncated(conn: psycopg.Connection, model: "Model") -> None:
    conn.execute(
        sql.SQL("TRUNCATE TABLE {}.{}").format(
            sql.Identifier(model.target.schema.resolved),
            sql.Identifier(model.target.name_resolved),
        )
    )


def _run_indexes_created(conn: psycopg.Connection, model: "Model") -> None:
    target = model.target
    col = target.partitioned_by
    if col is None:
        raise RuntimeError(
            f"indexes_created ran for {target.full_name!r} but partitioned_by "
            f"is None — the should_run gate should have prevented this"
        )
    index_name = f"{target.name_resolved}_{col}_idx"
    conn.execute(
        sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} ({})").format(
            sql.Identifier(index_name),
            sql.Identifier(target.schema.resolved),
            sql.Identifier(target.name_resolved),
            sql.Identifier(col),
        )
    )


def _run_uniques_added(conn: psycopg.Connection, model: "Model") -> None:
    target = model.target
    constraint_name = f"{target.name_resolved}_uq"
    unique_col_ids = sql.SQL(", ").join(
        sql.Identifier(c.name) for c in target.unique_columns
    )
    conn.execute(
        sql.SQL("""
            DO $$ BEGIN
                ALTER TABLE {}.{}
                ADD CONSTRAINT {} UNIQUE ({});
            EXCEPTION WHEN duplicate_table THEN NULL;
            END $$
        """).format(
            sql.Identifier(target.schema.resolved),
            sql.Identifier(target.name_resolved),
            sql.Identifier(constraint_name),
            unique_col_ids,
        )
    )


def _run_staging_schema_created(conn: psycopg.Connection, model: "Model") -> None:
    conn.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(_staging_schema_name(model))
        )
    )


# ── default action set ──────────────────────────────────────────────

# State transitions (mark_running / mark_applied / mark_blocked) are NOT
# actions — `@interval_lifecycle` calls `pg_state.mark_*` directly on the
# state connection, with the interval window passed explicitly. So the
# default action set is MODEL-level only (target-side asset DDL).


def default_actions() -> list[Action]:
    """The built-in lifecycle for a Postgres target. List position
    is execution order. Customise by building your own list:

        Target(actions=[*default_actions(), my_action])

    or by filtering / replacing entries before passing to Target."""
    return [
        # ── MODEL / PRE: setup (target DB) ──
        Action("schema_created", Level.MODEL, Phase.PRE, _run_schema_created),
        Action(
            "recreated",
            Level.MODEL,
            Phase.PRE,
            _run_recreated,
            should_run=lambda m: m.target.recreate_table,
        ),
        Action("table_created", Level.MODEL, Phase.PRE, _run_table_created),
        Action(
            "truncated",
            Level.MODEL,
            Phase.PRE,
            _run_truncated,
            should_run=lambda m: m.target.truncate_table,
        ),
        Action(
            "indexes_created",
            Level.MODEL,
            Phase.PRE,
            _run_indexes_created,
            should_run=lambda m: m.target.partitioned_by is not None,
        ),
        Action(
            "uniques_added",
            Level.MODEL,
            Phase.PRE,
            _run_uniques_added,
            should_run=lambda m: bool(m.target.unique_columns),
        ),
        Action(
            "staging_schema_created",
            Level.MODEL,
            Phase.PRE,
            _run_staging_schema_created,
            should_run=lambda m: m.target.staging is not None,
        ),
        # The staging table itself is created per-interval inside
        # `stage()` and dropped in each interval's apply transaction —
        # no model-level PRE/POST staging-table action needed.
    ]


__all__ = [
    "default_actions",
]
