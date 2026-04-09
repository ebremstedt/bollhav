from dataclasses import dataclass
from datetime import datetime


@dataclass
class Bounds:
    begin: datetime | None = None
    end: datetime | None = None

    def __post_init__(self) -> None:
        if self.begin is not None and self.begin.tzinfo is None:
            raise ValueError("bounds.begin must be timezone-aware")
        if self.end is not None and self.end.tzinfo is None:
            raise ValueError("bounds.end must be timezone-aware")
