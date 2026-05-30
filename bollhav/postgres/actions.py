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
    from datetime import datetime

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

    logged = model.target.staging is not None and model.target.staging.logged
    table_keyword = "TABLE" if logged else "UNLOGGED TABLE"
    col_defs = sql.SQL(",\n").join(
        sql.SQL(_col_ddl(c))
        for c in model.target.columns
        if isinstance(c, PostgresColumn)
    )
    conn.execute(
        sql.SQL(f"CREATE {table_keyword} IF NOT EXISTS {{}}.{{}} (\n{{}}\n)").format(
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


# ── INTERVAL action implementations (state machinery) ─────────────


def _interval_run_ctx(model: "Model"):
    """Resolve the (run_id, since, until) triple that an interval-level
    state action needs. Centralises the not-None narrowing so each
    action body doesn't redo the assertions.

    Raises if any of the three are missing — these are stashed by
    the interval runner / pipeline bootstrap, so missing means the
    runner was bypassed or the model didn't go through `@load_models`."""
    from bollhav.model.state import _run_id_for

    run_id = _run_id_for(model)  # mints one lazily if unset
    since = model._interval_since
    until = model._interval_until
    if since is None or until is None:
        raise ValueError(
            f"interval action on {model.target.full_name!r} needs "
            f"`model._interval_since` and `_interval_until` to be set — "
            f"normally `run_pre_interval_actions` / "
            f"`run_post_interval_actions` stash them before invoking."
        )
    return run_id, since, until


def _run_mark_running(_conn: psycopg.Connection, model: "Model") -> None:
    """State lifecycle: flip `pending` → `running` for the current
    interval before the user's execute.

    Reads the interval window from `model._interval_since` /
    `_interval_until` which the interval runner stashes before
    invoking. The `_conn` parameter is part of the Action protocol
    but unused here — `pg_state.mark_running` opens its own
    connection because the state UPDATE may need to live in a
    different transaction than the user's write."""
    del _conn  # Action-protocol parameter; mark_running opens its own conn.
    from bollhav.postgres import state as pg_state

    run_id, since, until = _interval_run_ctx(model)
    pg_state.mark_running(model=model, run_id=run_id, since=since, until=until)


def _run_mark_applied(_conn: psycopg.Connection, model: "Model") -> None:
    """State lifecycle: flip → `applied` after a successful execute.

    Skipped when the staging flush has already set the state row to
    `applied` atomically with the data move (the `@state` decorator
    consumes the `_state_applied_via_staging` marker before calling
    POST_INTERVAL, so this action only fires for the non-staged path).
    `_conn` unused — `pg_state.mark_applied` opens its own conn."""
    del _conn  # Action-protocol parameter; mark_applied opens its own conn.
    from bollhav.postgres import state as pg_state

    run_id, since, until = _interval_run_ctx(model)
    pg_state.mark_applied(model=model, run_id=run_id, since=since, until=until)


def _mark_applied_should_run(model: "Model") -> bool:
    """Skip when staging already flipped the state row inside the
    flush transaction."""
    if model.state is None:
        return False
    return model._state_applied_via_staging != (
        model._interval_since,
        model._interval_until,
    )


# ── should_run gates ────────────────────────────────────────────────


def _staging_reused_cleanup_applies(model: "Model") -> bool:
    target = model.target
    from bollhav.model.staging import StagingMode

    return (
        target.staging is not None
        and target.staging.mode is StagingMode.REUSED
        and not target.staging.keep_after_flush
    )


def _staging_table_created_applies(model: "Model") -> bool:
    target = model.target
    from bollhav.model.staging import StagingMode

    return target.staging is not None and target.staging.mode is StagingMode.REUSED


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
            should_run=lambda m: m.target.recreate_table,
        ),
        Action("table_created", Phase.PRE_MODEL, _run_table_created),
        Action(
            "truncated",
            Phase.PRE_MODEL,
            _run_truncated,
            should_run=lambda m: m.target.truncate_table,
        ),
        Action(
            "indexes_created",
            Phase.PRE_MODEL,
            _run_indexes_created,
            should_run=lambda m: m.target.partitioned_by is not None,
        ),
        Action(
            "uniques_added",
            Phase.PRE_MODEL,
            _run_uniques_added,
            should_run=lambda m: bool(m.target.unique_columns),
        ),
        Action(
            "staging_schema_created",
            Phase.PRE_MODEL,
            _run_staging_schema_created,
            should_run=lambda m: m.target.staging is not None,
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
        # ── INTERVAL: state machinery (only fires when state is set) ──
        Action(
            "mark_running",
            Phase.PRE_INTERVAL,
            _run_mark_running,
            should_run=lambda m: m.state is not None,
        ),
        Action(
            "mark_applied",
            Phase.POST_INTERVAL,
            _run_mark_applied,
            should_run=_mark_applied_should_run,
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
            if action.name in target._applied_model_actions:
                continue
            if not action.should_run(model):
                # Record as not-applicable so `setup_complete` can
                # see that the runner already made its call about
                # this action.
                target._applied_model_actions[action.name] = False
                continue
            action.run(conn, model)
            target._applied_model_actions[action.name] = True
            logger.debug("action: %s.%s done", target.full_name, action.name)


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
        if action.name in target._applied_model_actions:
            continue
        if not action.should_run(model):
            target._applied_model_actions[action.name] = False
            continue
        try:
            action.run(conn, model)
            target._applied_model_actions[action.name] = True
            logger.debug("action: %s.%s done", target.full_name, action.name)
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


def run_pre_interval_actions(
    conn: psycopg.Connection,
    model: "Model",
    since: "datetime",
    until: "datetime",
) -> None:
    """Run every applicable PRE_INTERVAL action, in declared list
    order, before each interval's execute.

    Interval actions are NOT recorded in `_applied_model_actions`
    (they fire every interval, not pipeline-once), so this runner
    re-evaluates `should_run` every time. The interval's window is
    stashed on the model as `_interval_since` / `_interval_until`
    so action callables can read it.

    PRE_INTERVAL is fail-fast — an exception propagates out so the
    `@state` decorator (or whoever called the runner) can decide
    whether to abort the interval."""
    target = model.target
    _resolve_actions(target)
    model._interval_since = since
    model._interval_until = until
    for action in target.effective_actions:
        if action.phase is not Phase.PRE_INTERVAL:
            continue
        if not action.should_run(model):
            continue
        action.run(conn, model)
        logger.debug(
            "action: %s.%s done (interval %s..%s)",
            target.full_name,
            action.name,
            since,
            until,
        )


def run_post_interval_actions(
    conn: psycopg.Connection,
    model: "Model",
    since: "datetime",
    until: "datetime",
) -> None:
    """Run every applicable POST_INTERVAL action after a successful
    interval execute. Same conventions as PRE_INTERVAL: not recorded
    in `_applied_model_actions`, `should_run` re-evaluated each
    interval, `model._interval_since` / `_interval_until` available
    on the model.

    POST_INTERVAL fires on success only — the caller (`@state` etc.)
    is responsible for handling the exception path and routing to a
    separate error action chain if needed.

    Failure policy uses `Target.on_failure` (FAIL_FAST default, SKIP
    opt-in) the same way `run_post_model_actions` does."""
    target = model.target
    _resolve_actions(target)
    model._interval_since = since
    model._interval_until = until
    for action in target.effective_actions:
        if action.phase is not Phase.POST_INTERVAL:
            continue
        if not action.should_run(model):
            continue
        try:
            action.run(conn, model)
            logger.debug(
                "action: %s.%s done (interval %s..%s)",
                target.full_name,
                action.name,
                since,
                until,
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
                "action: %s.%s failed — halting interval POST sweep",
                target.full_name,
                action.name,
            )
            raise


__all__ = [
    "default_actions",
    "run_pre_model_actions",
    "run_post_model_actions",
    "run_pre_interval_actions",
    "run_post_interval_actions",
]
