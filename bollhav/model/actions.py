"""Pluggable actions on a Target.

Each lifecycle operation (CREATE SCHEMA, CREATE TABLE, TRUNCATE, ADD
UNIQUE, ANALYZE, GRANT, mark_running, mark_applied, …) is a callable
wrapped in an `Action`. The runners walk each (level, phase)'s
applicable actions in declared order, gated by `should_run`, recorded
in `target._applied_model_actions` for the model level so intervals
2..N can short-circuit.

An action is placed on three orthogonal axes:

  level       — MODEL    : fires once per pipeline run (recorded in
                           `_applied_model_actions`)
                INTERVAL : fires every interval (re-evaluated each time)

  phase       — PRE      : on the way in  (before the user's loop / execute)
                POST     : on the way out (after a clean return)

  connection  — DATA     : runs against the target DB connection
                STATE    : runs against the state DB connection

So `(MODEL, PRE, DATA)` = "before the loop, create the target table";
`(INTERVAL, PRE, STATE)` = "before each interval, mark the state row
running"; `(INTERVAL, POST, STATE)` = "after a clean interval, mark
applied". The four (level, phase) combinations form the lifecycle the
`@model_lifecycle` / `@interval_lifecycle` hooks drive; `connection`
tells the runner which connection to hand the action.

MODEL/PRE is always fail-fast — you cannot safely continue a write
whose setup half-failed. MODEL/POST is per-target via
`Target.on_failure`: FAIL_FAST (default, halts the pipeline POST sweep)
or SKIP (logs + continues to the next action).

To extend, users supply their own action list:

    Target(
        ...,
        actions=[
            Action("grant_analytics", Level.MODEL, Phase.POST,
                   run=lambda c, m: c.execute("GRANT SELECT ON ...")),
        ],
    )

Framework defaults (CREATE TABLE etc.) live in
`Target.default_actions`. `actions` runs after defaults — see
`Target.effective_actions`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    import psycopg

    from bollhav.model.model import Model


class Level(Enum):
    """Whether an action fires once per model run or once per interval."""

    MODEL = "model"
    INTERVAL = "interval"


class Phase(Enum):
    """Whether an action fires on the way in (PRE) or out (POST) of its
    level's bracket."""

    PRE = "pre"
    POST = "post"


class Conn(Enum):
    """Which connection an action runs against — the target's data DB
    or the state DB (they may be different databases)."""

    DATA = "data"
    STATE = "state"


class OnFailure(Enum):
    """How a MODEL/POST action's exception is handled on a `Target`.
    MODEL/PRE is always strict — a half-failed setup cannot safely
    proceed to a write, so it ignores this policy."""

    FAIL_FAST = "fail_fast"
    SKIP = "skip"


@dataclass
class Action:
    """An action on a Target. See the module docstring for the three
    axes (`level`, `phase`, `connection`).

    `name` is used as the key in `target._applied_model_actions` (for
    the model level) and in `logger.debug("action: <full_name>.<name>
    done")` lines, so keep it short and snake_case.

    `run(conn, model)` does the work and is handed the target
    connection. The model-level runners manage the enclosing
    transaction. (Interval-level state transitions are no longer
    actions — `@interval_lifecycle` calls `pg_state.mark_*` directly
    with the interval window passed explicitly; the `INTERVAL` /
    `Conn.STATE` axes are kept on the enum but currently unused.)

    `should_run(model)` gates whether this action applies to the
    current model. Use it for directive-conditional actions like
    `should_run=lambda m: m.target.recreate_table` or feature-gated
    ones like `should_run=lambda m: m.state is not None`. Defaults
    to "always."

    `connection` defaults to `DATA` — most actions are target DDL;
    state-table actions set `connection=Conn.STATE`.
    """

    name: str
    level: Level
    phase: Phase
    run: Callable[["psycopg.Connection", "Model"], None]
    should_run: Callable[["Model"], bool] = field(default=lambda m: True)
    connection: Conn = Conn.DATA


__all__ = ["Level", "Phase", "Conn", "OnFailure", "Action"]
