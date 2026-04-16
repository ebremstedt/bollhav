from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, tzinfo
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bollhav.pipe.pipe_config import PipeConfig


@dataclass
class RuntimeOverride:
    """Runtime state set from the pipe config — not part of the model definition."""

    reload: bool = False
    latest: bool = False
    schema_suffix: str = ""
    batch_expression: str | None = None
    tz: tzinfo | None = None
    since: datetime | None = None
    until: datetime | None = None

    def apply_pipe(self, pipe: PipeConfig) -> None:
        """Apply pipe-level overrides, preserving state set during matching."""
        self.latest = pipe.latest.enabled and not self.reload
        self.schema_suffix = pipe.schema_suffix
        self.batch_expression = pipe.batch_expression_override
        self.tz = pipe.tz_override
        self.since = pipe.backfill.since
        self.until = pipe.backfill.until
