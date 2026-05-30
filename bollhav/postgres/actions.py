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
            Action("grant_analytics", Phase.POST_MODEL,
                   run=lambda c, m: c.execute("GRANT SELECT ON ...")),
        ],
    )

See `bollhav/model/actions.py` for the type definitions and
docs/content/ACTIONS.md for the full lifecycle story.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast, LiteralString
from uuid import UUID

import psycopg
from psycopg import sql

from bollhav.model.actions import Action, OnFailure, Phase

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
    from bollhav.postgres import state as pg_state

    if model.target.staging is not None and model.target.staging.schema:
        return model.target.staging.schema
    return pg_state._state_schema(model)


def _staging_table_name(model: "Model") -> str:
    run_id: UUID | None = getattr(model, "_state_run_id", None)
    if run_id is None:
        raise ValueError(
            f"staging action on {model.target.full_name!r} needs "
            f"`model._state_run_id` to be set — normally the bootstrap "
            f"mints one. Got None."
        )
    prefix = (
        model.target.staging.table_prefix
        if model.target.staging is not None and model.target.staging.table_prefix
        else f"{model.target.name}_staging_"
    )
    return f"{prefix}{str(run_id)[:8]}"


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
    assert col is not None, "should_run gate guarantees partitioned_by is set"
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


def _run_staging_table_created(conn: psycopg.Connection, model: "Model") -> None:
    from bollhav.postgres.columns import PostgresColumn

    logged = (
        model.target.staging is not None and model.target.staging.logged
    )
    table_keyword = "TABLE" if logged else "UNLOGGED TABLE"
    col_defs = sql.SQL(",\n").join(
        sql.SQL(_col_ddl(c))
        for c in model.target.columns
        if isinstance(c, PostgresColumn)
    )
    conn.execute(
        sql.SQL(
            f"CREATE {table_keyword} IF NOT EXISTS "
            "{}.{} (\n{}\n)"
        ).format(
            sql.Identifier(_staging_schema_name(model)),
            sql.Identifier(_staging_table_name(model)),
            col_defs,
        )
    )


# ── POST action implementations ────────────────────────────────────


def _run_staging_table_truncated(conn: psycopg.Connection, model: "Model") -> None:
    conn.execute(
        sql.SQL("TRUNCATE TABLE {}.{}").format(
            sql.Identifier(_staging_schema_name(model)),
            sql.Identifier(_staging_table_name(model)),
        )
    )


def _run_staging_table_dropped(conn: psycopg.Connection, model: "Model") -> None:
    conn.execute(
        sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
            sql.Identifier(_staging_schema_name(model)),
            sql.Identifier(_staging_table_name(model)),
        )
    )


# ── should_run gates ────────────────────────────────────────────────


def _staging_reused_cleanup_applies(target: "Target") -> bool:
    from bollhav.model.staging import StagingMode

    return (
        target.staging is not None
        and target.staging.mode is StagingMode.REUSED
        and not target.staging.keep_after_flush
    )


def _staging_table_created_applies(target: "Target") -> bool:
    from bollhav.model.staging import StagingMode

    return (
        target.staging is not None
        and target.staging.mode is StagingMode.REUSED
    )


# ── default action set ──────────────────────────────────────────────


def default_actions() -> list[Action]:
    """The built-in lifecycle for a Postgres target. List position
    is execution order. Customise by building your own list:

        Target(actions=[*default_actions(), my_action])

    or by filtering / replacing entries before passing to Target."""
    return [
        # ── PRE: setup ──
        Action("schema_created", Phase.PRE_MODEL, _run_schema_created),
        Action(
            "recreated",
            Phase.PRE_MODEL,
            _run_recreated,
            should_run=lambda t: t.recreate_table,
        ),
        Action("table_created", Phase.PRE_MODEL, _run_table_created),
        Action(
            "truncated",
            Phase.PRE_MODEL,
            _run_truncated,
            should_run=lambda t: t.truncate_table,
        ),
        Action(
            "indexes_created",
            Phase.PRE_MODEL,
            _run_indexes_created,
            should_run=lambda t: t.partitioned_by is not None,
        ),
        Action(
            "uniques_added",
            Phase.PRE_MODEL,
            _run_uniques_added,
            should_run=lambda t: bool(t.unique_columns),
        ),
        Action(
            "staging_schema_created",
            Phase.PRE_MODEL,
            _run_staging_schema_created,
            should_run=lambda t: t.staging is not None,
        ),
        Action(
            "staging_table_created",
            Phase.PRE_MODEL,
            _run_staging_table_created,
            should_run=_staging_table_created_applies,
        ),
        # ── POST: teardown ──
        Action(
            "staging_table_truncated",
            Phase.POST_MODEL,
            _run_staging_table_truncated,
            should_run=_staging_reused_cleanup_applies,
        ),
        Action(
            "staging_table_dropped",
            Phase.POST_MODEL,
            _run_staging_table_dropped,
            should_run=_staging_reused_cleanup_applies,
        ),
    ]


# ── runners ─────────────────────────────────────────────────────────


def _resolve_actions(target: "Target") -> None:
    """Populate `target.default_actions` from `default_actions()` on
    first use, if it's still `None` (i.e. the user didn't pass an
    explicit list or `[]` to opt out). Lazy so `Target` itself stays
    backend-agnostic — defaults are postgres-specific here, and a
    different backend would supply its own."""
    if target.default_actions is None:
        target.default_actions = default_actions()


def run_pre_model_actions(conn: psycopg.Connection, model: "Model") -> None:
    """Run every applicable PRE action that hasn't fired this pipeline
    run, in declared list order, inside a single transaction.

    PRE is always fail-fast — a half-failed setup cannot safely
    proceed to a write, so exceptions propagate (rolling back the
    whole PRE block).

    No-op when `target.setup_complete` says everything's already done,
    so on intervals 2..N we don't even open a transaction."""
    target = model.target
    _resolve_actions(target)
    if target.setup_complete:
        return
    with conn.transaction():
        for action in target.effective_actions:
            if action.phase is not Phase.PRE_MODEL:
                continue
            if target._applied_model_actions.get(action.name):
                continue
            if not action.should_run(target):
                continue
            action.run(conn, model)
            target._applied_model_actions[action.name] = True
            logger.debug(
                "action: %s.%s done", target.full_name, action.name
            )


def run_post_model_actions(conn: psycopg.Connection, model: "Model") -> None:
    """Run every applicable POST action that hasn't fired this pipeline
    run, in declared list order.

    Failure policy is per-target via `Target.on_failure`:

      * `OnFailure.FAIL_FAST` (default) — re-raise. Halts the rest of
        this target's POST AND propagates out of `run_post_model_actions`,
        so a `@load_models` post-sweep across many models halts on
        the first model's failure.
      * `OnFailure.SKIP` — log a warning and continue to the next
        action. Lets a flaky action (Slack notify, etc.) not break
        the pipeline.

    Each action runs in its own (autocommit-style) execution; there's
    no enclosing transaction across actions, because that would couple
    unrelated POST work into one atomic unit, which is rarely what you
    want (a failed GRANT shouldn't roll back an ANALYZE)."""
    target = model.target
    _resolve_actions(target)
    for action in target.effective_actions:
        if action.phase is not Phase.POST_MODEL:
            continue
        if target._applied_model_actions.get(action.name):
            continue
        if not action.should_run(target):
            continue
        try:
            action.run(conn, model)
            target._applied_model_actions[action.name] = True
            logger.debug(
                "action: %s.%s done", target.full_name, action.name
            )
        except Exception:
            if target.on_failure is OnFailure.SKIP:
                logger.warning(
                    "action: %s.%s failed (on_failure=SKIP, continuing)",
                    target.full_name,
                    action.name,
                    exc_info=True,
                )
                continue
            logger.exception(
                "action: %s.%s failed — halting POST sweep",
                target.full_name,
                action.name,
            )
            raise


__all__ = [
    "default_actions",
    "run_pre_model_actions",
    "run_post_model_actions",
]
