from dataclasses import dataclass, field
from datetime import datetime, date


@dataclass
class Bounds:
    begin: datetime | None = None
    end: date = field(default_factory=date.today)
