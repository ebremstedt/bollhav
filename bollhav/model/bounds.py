from dataclasses import dataclass
from datetime import datetime


@dataclass
class Bounds:
    begin: datetime | None = None
    end: datetime | None = None
