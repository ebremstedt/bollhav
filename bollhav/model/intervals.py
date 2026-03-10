from dataclasses import dataclass
from datetime import datetime


@dataclass
class TZInterval:
    since: datetime
    until: datetime

    def __post_init__(self) -> None:
        if self.since.tzinfo is None or self.until.tzinfo is None:
            raise ValueError("since and until must be timezone-aware")
        if self.since >= self.until:
            raise ValueError("since must be before until")
        if self.since == self.until:
            raise ValueError("Since can not be equal to until")
