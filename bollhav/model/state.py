"""Per-model interval state tracking.

Opt-in: set `state=State(...)` on a Model. The user is then expected to:
  1. Call the backend's `ensure_tables(model)` once per pipeline run
  2. Call `prefill(model, run_id=..., intervals=..., state_mode=...)` with
     the intervals they intend to process
  3. Wrap their `execute()` with `@state_tracker`, which gates on
     applied rows and flips pending → applied after a successful run

Scope is intentionally narrow in this first cut:
  * one state table per model (no separate errors table yet)
  * RESPECT / DISRESPECT modes only — DISCOVER, NUKE_STATE come later
  * state always co-locates with the target DB (atomic flip with
    staging requires same DB); the `dsn_env_var` field exists on
    `State` for future use but isn't honored yet
"""

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from typing import TYPE_CHECKING, Callable
from uuid import UUID, uuid4

if TYPE_CHECKING:
    from bollhav.model.model import Model

logger = logging.getLogger(__name__)


@dataclass
class State:
    """Opt-in state-tracking config for a Model.

    `dsn_env_var` — env var holding the DSN for the state DB. When set,
        the state table lives in a separate database from the target.
        When unset, falls back to `target.dsn_env_var` (state shares the
        target's DB, which is the only configuration compatible with
        `Target(staging=...)` — staging's atomic flush requires
        state-and-target to be in one transaction).
    `schema_prefix` — override the default `"z_"` prefix applied to the
        state schema. The default `z_` keeps bollhav-owned tables out
        of the user's target schema and sorts them to the bottom of a
        DB editor's schema list. Set `""` to drop the prefix entirely
        (state schema then equals target's schema name).
    `table_suffix` — override the default `"_state"` suffix appended
        to the target name to derive the state table name. Example: a
        target `orders` with default suffix produces `orders_state`;
        with `table_suffix="_history"` you get `orders_history`.
    """

    dsn_env_var: str | None = None
    schema_prefix: str | None = None
    table_suffix: str | None = None


class StateMode(Enum):
    """How the pre-fill step treats existing state rows.

    RESPECT    — preserve applied rows; only insert pending rows for
                 new (since, until) intervals. The resumable mode.
    DISRESPECT — reset every interval back to pending, regardless of
                 prior status. The whole window reruns."""

    RESPECT = "respect"
    DISRESPECT = "disrespect"


def state_tracker(func: Callable) -> Callable:
    """Execution decorator that records state in the model's state table.

    For models without `state`, the decorator is a zero-overhead
    passthrough. For state-enabled models:

      1. Gate: skip if `(since, until)` is already applied.
      2. Run the wrapped function.
      3. On success → mark applied.
      4. On exception → re-raise (no error table in this first cut).

    The state row's existence (with status='pending') must be
    established by `prefill()` before the wrapped function runs."""
    sig = inspect.signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        model = bound.arguments.get("model")
        since = bound.arguments.get("since")
        until = bound.arguments.get("until")

        if not _is_state_enabled(model) or since is None or until is None:
            return func(*args, **kwargs)

        run_id = _run_id_for(model)
        if _is_applied(model, since=since, until=until):
            logger.debug(
                "state: gate skipped applied %s..%s for %s",
                since,
                until,
                model.target.full_name,
            )
            return None

        result = func(*args, **kwargs)

        # When `stage()` flushed staging → target in its own tx, it
        # already flipped the state row atomically. Re-issuing
        # mark_applied here would be a redundant UPDATE.
        staged = getattr(model, "_state_applied_via_staging", None)
        if staged == (since, until):
            model._state_applied_via_staging = None
        else:
            _mark_applied(model, run_id=run_id, since=since, until=until)
        return result

    return wrapper


def _is_state_enabled(model: "Model | None") -> bool:
    return (
        model is not None
        and getattr(model, "state", None) is not None
        and getattr(model, "batching", None) is not None
    )


def _run_id_for(model: "Model") -> UUID:
    """Return the run_id for this pipeline invocation. Callers
    (typically a setup step in the user's pipeline, or a test) stash
    it on the model; if missing, mint one lazily."""
    run_id = getattr(model, "_state_run_id", None)
    if run_id is None:
        run_id = uuid4()
        model._state_run_id = run_id
    return run_id


def _backend(model: "Model"):
    """Dispatch — postgres-only for now. MSSQL will hang off the same
    seam: branch on `model.target.database`."""
    from bollhav.postgres import state as pg_state

    return pg_state


def _is_applied(model: "Model", *, since, until) -> bool:
    return _backend(model).is_applied(model=model, since=since, until=until)


def _mark_applied(model: "Model", *, run_id: UUID, since, until) -> None:
    _backend(model).mark_applied(model=model, run_id=run_id, since=since, until=until)


__all__ = ["State", "StateMode", "state_tracker"]
