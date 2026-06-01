"""Pluggable actions on a Target.

Each lifecycle operation (CREATE SCHEMA, CREATE TABLE, TRUNCATE, ADD
UNIQUE, DROP staging, ANALYZE, GRANT, mark_running, mark_applied, …)
is a callable wrapped in an `Action`. The runners walk each phase's
applicable actions in declared order, gated by `should_run`, recorded
in `target._applied_model_actions` for the model-level phases so that
intervals 2..N can short-circuit.

Four phases form a 2×2 grid:

                    ┌────────────────────────────────────────────┐
                    │              MODEL LEVEL                   │
                    │  fires once per pipeline run               │
                    │  state recorded in `_applied_model_actions`│
                    ├────────────────────────────────────────────┤
   PRE_MODEL        │  before the user's loop starts             │
                    │  e.g. CREATE TABLE, CREATE INDEX           │
                    │                                            │
   POST_MODEL       │  after the user's loop returns cleanly     │
                    │  e.g. ANALYZE the_whole_table, GRANT       │
                    └────────────────────────────────────────────┘

                    ┌────────────────────────────────────────────┐
                    │           INTERVAL LEVEL                   │
                    │  fires every interval                      │
                    │  not gated by `_applied_model_actions` —   │
                    │  every interval re-runs the hook           │
                    ├────────────────────────────────────────────┤
   PRE_INTERVAL     │  before each interval's execute()          │
                    │  e.g. mark_running, lock acquire           │
                    │                                            │
   POST_INTERVAL    │  after each interval's execute() returns   │
                    │  e.g. mark_applied, emit metric            │
                    └────────────────────────────────────────────┘

PRE_INTERVAL / POST_INTERVAL is where today's `@state` machinery is
heading — `mark_running` / `mark_applied` / `record_failure` are
naturally per-interval actions, so the long-term shape is "add the
state-tracking actions to your model" instead of "wrap execute with
`@state`." For now the interval phases are placeholder enum values;
the runners exist only for the model-level phases.

PRE_MODEL is always fail-fast — you cannot safely continue a write
whose setup half-failed. POST_MODEL is per-target via `Target.on_failure`:
FAIL_FAST (default, halts the pipeline POST sweep) or SKIP (logs +
continues to the next action).

To extend, users supply their own action list:

    Target(
        ...,
        actions=[
            Action("grant_analytics", Phase.POST_MODEL,
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


class Phase(Enum):
    """When an action fires within a pipeline run. See module
    docstring for the 2×2 grid."""

    PRE_MODEL = "pre_model"
    POST_MODEL = "post_model"
    PRE_INTERVAL = "pre_interval"
    POST_INTERVAL = "post_interval"


class OnFailure(Enum):
    """How a POST_MODEL action's exception is handled on a `Target`.
    PRE_MODEL is always strict — a half-failed setup cannot safely
    proceed to a write, so PRE_MODEL ignores this policy."""

    FAIL_FAST = "fail_fast"
    SKIP = "skip"


@dataclass
class Action:
    """An action on a Target. See `Phase` for what each value means.

    `name` is used as the key in `target._applied_model_actions` (for
    model-level phases) and in `logger.debug("action: <full_name>.<name>
    done")` lines, so keep it short and snake_case.

    `run(conn, model)` does the work. The model-level runners manage
    the enclosing transaction; interval-level runners may not. For
    interval actions, the runner stashes `model._interval_since` and
    `model._interval_until` before invoking so actions can read them.

    `should_run(model)` gates whether this action applies to the
    current model. Use it for directive-conditional actions like
    `should_run=lambda m: m.target.recreate_table` or feature-gated
    ones like `should_run=lambda m: m.state is not None`. Defaults
    to "always."
    """

    name: str
    phase: Phase
    run: Callable[["psycopg.Connection", "Model"], None]
    should_run: Callable[["Model"], bool] = field(default=lambda m: True)


__all__ = ["Phase", "OnFailure", "Action"]
