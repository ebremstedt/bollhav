from __future__ import annotations

import inspect
import logging
import traceback
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
    """Opt-in state tracking config for a Model.

    Setting `state=State(...)` on a Model causes its interval runs to be
    recorded in a per-model state table. Re-runs can be made resumable
    (skip already-applied intervals) via `STATE_MODE=resume` at runtime.

    `dsn_env_var` — env var holding the state DB DSN. Falls back to the
    model's target DSN when None. When falling back, the state schema is
    auto-prefixed with `z_` to keep the user's target schema uncluttered.

    `log_errors` — when True (default), exceptions raised by the wrapped
    execute function are recorded in a sibling `<name>_errors` table
    before being re-raised.
    """

    dsn_env_var: str | None = None
    log_errors: bool = True


class StateMode(Enum):
    """How the pre-fill step should treat existing state rows.

    `RESPECT`    — preserve applied rows; only insert pending rows for
        new (since, until) intervals. The decorator gate then skips
        applied intervals at execute time. This is the resumable mode.
    `DISRESPECT` — reset every interval back to pending and clear
        applied_at, regardless of prior state. The gate still skips
        applied rows, but nothing is applied after a reset, so the whole
        window runs again.
    """

    RESPECT = "respect"
    DISRESPECT = "disrespect"


# ── decorator ────────────────────────────────────────────────────────


def state_tracker(func: Callable) -> Callable:
    """Execution decorator that records state in the model's state table.

    Wraps an `execute(model, since, until, ...)` function. When `model.state`
    is None, the decorator is a passthrough — zero overhead.

    For state-enabled models:
        1. Gate: if (since, until) is already applied → skip the call.
        2. Run the wrapped function.
        3. On success → UPDATE row to status='applied', set applied_at.
        4. On exception → INSERT into <name>_errors (if log_errors=True),
           then re-raise.

    The state row's existence (with status='pending') is established
    earlier by `@load_models` during pre-fill. The decorator does not
    create rows that the pre-fill missed.
    """
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

        try:
            result = func(*args, **kwargs)
        except Exception as exc:
            if model.state.log_errors:
                _record_error(
                    model,
                    run_id=run_id,
                    since=since,
                    until=until,
                    exc=exc,
                )
            raise

        _mark_applied(model, run_id=run_id, since=since, until=until)
        return result

    return wrapper


# ── backend dispatch ─────────────────────────────────────────────────


def _is_state_enabled(model: "Model | None") -> bool:
    return (
        model is not None
        and getattr(model, "state", None) is not None
        and getattr(model, "batching", None) is not None
    )


def _run_id_for(model: "Model") -> UUID:
    """Return the run_id associated with this pipeline invocation.

    `load_models` stashes a single run_id on each model during pre-fill.
    If a model arrives here without one (e.g. tests that bypass
    load_models), mint a fresh one and cache it on the instance."""
    run_id = getattr(model, "_state_run_id", None)
    if run_id is None:
        run_id = uuid4()
        model._state_run_id = run_id
    return run_id


def _backend(model: "Model"):
    from bollhav.postgres import state as pg_state

    return pg_state


def _is_applied(model: "Model", *, since, until) -> bool:
    return _backend(model).is_applied(model=model, since=since, until=until)


def _mark_applied(model: "Model", *, run_id: UUID, since, until) -> None:
    _backend(model).mark_applied(
        model=model, run_id=run_id, since=since, until=until
    )


def _record_error(model: "Model", *, run_id: UUID, since, until, exc: Exception) -> None:
    _backend(model).record_error(
        model=model,
        run_id=run_id,
        since=since,
        until=until,
        error_type=type(exc).__name__,
        error_message=str(exc),
        traceback_text=traceback.format_exc(),
    )


__all__ = ["State", "StateMode", "state_tracker"]
