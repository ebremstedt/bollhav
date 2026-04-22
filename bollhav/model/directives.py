from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from bollhav.model.batch import ChunkMode


@dataclass
class Directives:
    """Per-run directives attached to a Model.

    Everything here is invocation-specific — not part of the model
    definition. Matching sets the tag-driven reload fields; `Model.apply_pipe`
    sets the pipe-driven ones on the returned model.

    Effective model config (batching, bounds, target.schema) lives on the
    Model itself — after `apply_pipe`, those fields already reflect pipe
    overrides, so consumers read them directly.
    """

    # Tag-driven (set by matching)
    reload: bool = False
    reload_mode: ChunkMode | None = None
    reload_batch_size: int | None = None
    reload_interval_expression: str | None = None

    # Pipe-driven
    latest: bool = False
    since: datetime | None = None
    until: datetime | None = None
