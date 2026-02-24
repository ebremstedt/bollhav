from dataclasses import dataclass
from enum import Enum


class Database(Enum):
    POSTGRES = "POSTGRES"


@dataclass
class DatabaseColumn:
    name: str
    nullable: bool = True
    order: int | None = None
