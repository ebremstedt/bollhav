from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class StateBackend(Enum):
    POSTGRES = "postgres"


class StateMode(Enum):
    """How the pre-fill step treats existing state rows.

    DISCOVER  — preserve applied rows; only insert pending rows for
                 new (since, until) intervals. The resumable mode.
    BULLDOZER — reset every *existing* interval row back to pending,
                 regardless of prior status. The whole window reruns, but the
                 rows keep their (since, until) boundaries — so the chunk
                 granularity is fixed.
    TORCH      — DELETE every state row for the model first, then prefill fresh
                 at the current chunk. The escape hatch for changing chunk
                 granularity (e.g. hourly → monthly) or wiping a stale backlog:
                 unlike BULLDOZER it clears the rows entirely, so the new
                 granularity starts clean. Destructive — applied history is
                 lost, so the next run reprocesses every interval (idempotent
                 for MERGE / recreate writes; APPEND would duplicate). Does NOT
                 touch the model's data."""

    DISCOVER = "discover"
    BULLDOZER = "bulldozer"
    TORCH = "torch"


@dataclass
class State:
    """Opt-in state-tracking config for a Model.

    `backend` — which database stores the state (a `StateBackend`).
        Defaults to Postgres, the only implementation today.
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
