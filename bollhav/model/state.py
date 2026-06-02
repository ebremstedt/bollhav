"""Per-model interval state tracking — config + shared helpers.

Opt-in: set `state=State(...)` on a Model. The lifecycle hooks drive
the rest: `@model_lifecycle` bootstraps the state table + prefills the
contract, and `@interval_lifecycle` gates on applied rows, takes the
per-interval lock, and flips pending → applied after a successful run.
See `bollhav/model/lifecycle.py`.

This module holds the user-facing config (`State`, `StateMode`,
`BlockCode`) and the small helpers the hooks share (`_run_id_for`,
`ModelLockedError`). The Postgres implementation of the transitions
lives in `bollhav/postgres/state.py`.

Scope is intentionally narrow in this first cut:
  * one state table per model (no separate errors table yet)
  * DISCOVER / BULLDOZER modes only — DISCOVER, NUKE_STATE come later
  * state always co-locates with the target DB (atomic flip with
    staging requires same DB); the `dsn_env_var` field exists on
    `State` for future use but isn't honored yet
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

if TYPE_CHECKING:
    from bollhav.model.model import Model

logger = logging.getLogger(__name__)


class StateBackend(Enum):
    """Which database backend stores a model's interval state.

    Set via `State(backend=...)`. The lifecycle hooks resolve the
    matching implementation module from this — so the dispatch is driven
    by the model, not hardcoded. Postgres is the only backend today."""

    POSTGRES = "postgres"


class StateMode(Enum):
    """How the pre-fill step treats existing state rows.

    DISCOVER  — preserve applied rows; only insert pending rows for
                 new (since, until) intervals. The resumable mode.
    BULLDOZER — reset every interval back to pending, regardless of
                 prior status. The whole window reruns."""

    DISCOVER = "discover"
    BULLDOZER = "bulldozer"


@dataclass
class State:
    """Opt-in state-tracking config for a Model.

    `backend` — which database stores the state (a `StateBackend`).
        Defaults to Postgres, the only implementation today.
    `schema_prefix` — override the default `"z_"` prefix applied to the
        state schema. The default `z_` keeps bollhav-owned tables out
        of the user's target schema and sorts them to the bottom of a
        DB editor's schema list. Set `""` to drop the prefix entirely
        (state schema then equals target's schema name).
    `table_suffix` — override the default `"_state"` suffix appended
        to the target name to derive the state table name. Example: a
        target `orders` with default suffix produces `orders_state`;
        with `table_suffix="_history"` you get `orders_history`.
    `allow_concurrent_runs` — Default True: the per-interval lock
        `@interval_lifecycle` takes is enough for typical workloads (it
        lets two workers race the same model on different intervals).
        Set False to forbid concurrent whole-pipeline runs of this
        model — `@model_lifecycle` then takes a Postgres advisory lock
        keyed by the model's full name, serializing runs. Use that when
        interval ordering matters or your loop has cross-interval side
        effects that would conflict between concurrent workers.
    """

    backend: StateBackend = StateBackend.POSTGRES
    mode: StateMode = StateMode.DISCOVER
    schema_prefix: str | None = None
    table_suffix: str | None = None
    allow_concurrent_runs: bool = True


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


def _run_id_for(model: "Model") -> UUID:
    """Return the run_id for this pipeline invocation. The lifecycle
    hooks stash it on the model so the bootstrap and each interval's
    transitions share one id; minted lazily if unset."""
    run_id = getattr(model, "_state_run_id", None)
    if run_id is None:
        run_id = uuid4()
        model._state_run_id = run_id
    return run_id


class ModelLockedError(RuntimeError):
    """Raised by `@model_lifecycle` when another pipeline already holds
    the advisory lock on this model (gated by
    `State(allow_concurrent_runs=False)`). Operators can catch this and
    decide whether to skip the model, wait, or fail the run."""


__all__ = [
    "State",
    "StateBackend",
    "StateMode",
    "BlockCode",
    "format_block_reason",
    "ModelLockedError",
]
