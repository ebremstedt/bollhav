from dataclasses import dataclass
from datetime import datetime

from bollhav.model.messages.error import (
    NaiveContractBeginError,
    NaiveContractEndError,
)


@dataclass
class Contract:
    """A model's time bounds — the outer `begin`/`end` window its runs may
    target. Both ends are optional and, when set, must be timezone-aware."""

    begin: datetime | None = None
    end: datetime | None = None

    def __post_init__(self) -> None:
        if self.begin is not None and self.begin.tzinfo is None:
            raise NaiveContractBeginError()
        if self.end is not None and self.end.tzinfo is None:
            raise NaiveContractEndError()
