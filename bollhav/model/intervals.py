from dataclasses import dataclass
from datetime import datetime

from bollhav.model.messages.error import (
    NaiveIntervalBoundsError,
    SinceAfterUntilError,
    SinceEqualsUntilError,
)


@dataclass
class TZInterval:
    since: datetime
    until: datetime

    def __post_init__(self) -> None:
        if self.since.tzinfo is None or self.until.tzinfo is None:
            raise NaiveIntervalBoundsError()
        if self.since >= self.until:
            raise SinceAfterUntilError()
        if self.since == self.until:
            raise SinceEqualsUntilError()

    def __str__(self) -> str:
        return f"{self.since:%Y-%m-%d %H:%M} → {self.until:%Y-%m-%d %H:%M}"
