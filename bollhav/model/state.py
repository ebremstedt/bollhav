"""Per-model interval state tracking.

Opt-in: set `state=State(...)` on a Model. The user is then expected to:
  1. Call the backend's `ensure_tables(model)` once per pipeline run
  2. Call `prefill(model, run_id=..., intervals=..., state_mode=...)` with
     the intervals they intend to process
  3. Wrap their `execute()` with `@state`, which gates on
     applied rows and flips pending → applied after a successful run

Scope is intentionally narrow in this first cut:
  * one state table per model (no separate errors table yet)
  * DISCOVER / BULLDOZER modes only — DISCOVER, NUKE_STATE come later
  * state always co-locates with the target DB (atomic flip with
    staging requires same DB); the `dsn_env_var` field exists on
    `State` for future use but isn't honored yet
"""

from __future__ import annotations

import inspect
import logging
from contextlib import contextmanager
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

    DISCOVER  — preserve applied rows; only insert pending rows for
                 new (since, until) intervals. The resumable mode.
    BULLDOZER — reset every interval back to pending, regardless of
                 prior status. The whole window reruns."""

    DISCOVER = "discover"
    BULLDOZER = "bulldozer"


class BlockCode(Enum):
    """Stable identifiers for blocked-state reasons.

    Codes are namespaced `<DOMAIN>_<NNN>` and **permanent**: once
    assigned, never reused, never renumbered. New block conditions
    get the next number in the domain. Document each code's cause and
    remediation in docs/content/BLOCK_CODES.md.

    The reason text written to `state.blocked_reason` looks like:

        STATE_001: upstream 'warehouse.orders' not registered

    so operators can grep on `STATE_001`, look it up, and act."""

    UPSTREAM_NOT_REGISTERED = "STATE_001"
    UPSTREAM_NOT_SATISFIED = "STATE_002"


def format_block_reason(code: BlockCode, message: str) -> str:
    """`STATE_001: upstream 'warehouse.orders' not registered`"""
    return f"{code.value}: {message}"


def state(func: Callable) -> Callable:
    """Execution decorator that records state in the model's state table.

    For models without `state`, the decorator is a zero-overhead
    passthrough. For state-enabled models:

      1. Gate: skip if `(since, until)` is already applied.
      2. Run the wrapped function.
      3. On success → mark applied (or trust the staging flush marker).
      4. On exception → log a row to the model's `_errors` table,
         flip state to `'error'`, and **re-raise**. The original
         exception propagates so callers can decide what to do.

    Edge case: if the staging flush already set state to `'applied'`
    (data IS in target) and user code AFTER the `with stage(...)`
    block raised, we log the error but DO NOT downgrade state to
    `'error'` — the write was successful. The marker
    `model._state_applied_via_staging` is how we detect that.

    The state row's existence (with status='pending') must be
    established by `prefill()` before the wrapped function runs."""
    import traceback as _tb

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
        assert model is not None  # narrowed by _is_state_enabled

        run_id = _run_id_for(model)
        if _is_applied(model, since=since, until=until):
            logger.debug(
                "state: gate skipped applied %s..%s for %s",
                since,
                until,
                model.target.full_name,
            )
            return None

        # Per-interval advisory lock — lets two workers race on the
        # same model but different intervals without conflicting. If
        # another worker already holds this interval, we skip silently
        # (they'll handle it) and move on. The lock connection is
        # kept open across the user's execute and released after
        # mark_applied / record_failure.
        backend = _backend(model)
        lock_conn = backend._connect(model)
        try:
            if not backend.try_acquire_interval_lock(lock_conn, model, since, until):
                logger.debug(
                    "state: interval lock held by another worker, skipping "
                    "%s..%s on %s",
                    since,
                    until,
                    model.target.full_name,
                )
                return None

            # Live upstream check: if this model has upstreams, ask
            # the library RIGHT NOW (not from bootstrap-time snapshot)
            # whether each one has an applied row covering this
            # interval. If not, mark this row blocked with a reason
            # and skip — the next run will re-evaluate.
            if model.upstream:
                ok, reason = backend.is_upstream_satisfied_live(
                    lock_conn, model, since, until
                )
                if not ok:
                    logger.debug(
                        "state: upstream not satisfied at runtime for "
                        "%s..%s on %s — marking blocked (%s)",
                        since,
                        until,
                        model.target.full_name,
                        reason,
                    )
                    backend.mark_blocked(
                        model,
                        run_id=run_id,
                        since=since,
                        until=until,
                        reason=reason or "",
                    )
                    return None

            # PRE_INTERVAL actions — for state-enabled models this
            # fires the `mark_running` action (pending → running),
            # plus any user-added per-interval hooks (metrics, log,
            # idempotency markers). The runner stashes since/until on
            # the model so action callables can read them.
            from bollhav.postgres.actions import (
                run_pre_interval_actions,
                run_post_interval_actions,
            )

            run_pre_interval_actions(lock_conn, model, since, until)

            try:
                result = func(*args, **kwargs)
            except Exception as exc:
                # Was staging already successful for this interval before
                # the post-stage code raised? If so, data is in target —
                # log the failure but keep state='applied'.
                staged_ok = getattr(model, "_state_applied_via_staging", None) == (
                    since,
                    until,
                )
                _record_failure(
                    model,
                    run_id=run_id,
                    since=since,
                    until=until,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    traceback_text=_tb.format_exc(),
                    update_state=not staged_ok,
                )
                # Consume the marker so it doesn't leak to a later interval.
                if staged_ok:
                    model._state_applied_via_staging = None
                raise

            # POST_INTERVAL actions — for state-enabled models this
            # fires `mark_applied` (gated by `should_run` to skip
            # when the staging flush already flipped the row), plus
            # any user-added per-interval hooks. Consume the staging
            # marker first so `mark_applied`'s should_run can see it.
            staged = getattr(model, "_state_applied_via_staging", None)
            if staged == (since, until):
                # Leave the marker in place across the POST_INTERVAL
                # sweep — `_mark_applied_should_run` reads it to decide
                # whether to skip — then clear after.
                run_post_interval_actions(lock_conn, model, since, until)
                model._state_applied_via_staging = None
            else:
                run_post_interval_actions(lock_conn, model, since, until)
            return result
        finally:
            try:
                backend.release_interval_lock(lock_conn, model, since, until)
            except Exception:
                # Lock is also released automatically when conn closes.
                pass
            lock_conn.close()

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


def _record_failure(
    model: "Model",
    *,
    run_id: UUID,
    since,
    until,
    error_type: str,
    error_message: str,
    traceback_text: str | None,
    update_state: bool,
) -> None:
    _backend(model).record_failure(
        model=model,
        run_id=run_id,
        since=since,
        until=until,
        error_type=error_type,
        error_message=error_message,
        traceback_text=traceback_text,
        update_state=update_state,
    )


class ModelLockedError(RuntimeError):
    """Raised by `model_lock` when another pipeline already holds the
    advisory lock on this model. Operators can catch this and decide
    whether to skip the model, wait, or fail the run."""


@contextmanager
def model_lock(model: "Model"):
    """Acquire a Postgres advisory lock on `model` for the duration
    of the with-block — prevents two pipelines from concurrently
    processing the same model. Released automatically on exit
    (success OR exception). The user's loop wraps this around its
    per-model iteration:

        for model in models:
            with model_lock(model):
                for interval in model.intervals:
                    execute(model=model, since=interval.since, until=interval.until)

    Raises `ModelLockedError` if the lock can't be acquired (another
    pipeline holds it). Catch it to skip / wait / fail as you prefer."""
    backend = _backend(model)
    conn = backend._connect(model)
    try:
        if not backend.try_acquire_lock(conn, model):
            raise ModelLockedError(
                f"another pipeline holds the lock on {model.target.full_name!r} "
                f"— concurrent runs of the same model are not allowed"
            )
        try:
            yield
        finally:
            backend.release_lock(conn, model)
    finally:
        conn.close()


__all__ = [
    "State",
    "StateMode",
    "BlockCode",
    "format_block_reason",
    "state",
    "model_lock",
    "ModelLockedError",
]
