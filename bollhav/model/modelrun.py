from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from bollhav.model.intervals import TZInterval

if TYPE_CHECKING:
    from bollhav.model.model import Model


@dataclass
class ModelRun:
    """One invocation of a model — the immutable `model` definition paired with
    this run's temporal state.

    `Model` answers *what / where / how* (target, batching, bounds, kind); a
    `ModelRun` answers *when / how-far*:

        window     — the single time window this run targets (resolved once by
                     `runtime.resolve_window` from bounds + the run instruction).
        intervals  — the window split into the chunk contract; narrowed to the
                     still-actionable subset during the state bootstrap. `(None,)`
                     for a model with no window (monolithic / view).
        run_id     — minted once here and shared across this run's state
                     transitions (insert / mark_running / mark_applied / …).

    `runtime.apply_runtime_overrides` mints one `ModelRun` per matched model;
    it's what flows through `@model_lifecycle` and the user's run loop. The
    definition is immutable; the run-state on this object is what evolves."""

    model: "Model"
    window: TZInterval | None = None
    intervals: tuple[TZInterval, ...] | tuple[None] = (None,)
    run_id: UUID = field(default_factory=uuid4)


__all__ = ["ModelRun"]
