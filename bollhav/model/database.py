from dataclasses import dataclass
from enum import Enum


class Database(Enum):
    POSTGRES = "POSTGRES"
    PARQUET = "PARQUET"
    MSSQL = "MSSQL"


@dataclass
class DatabaseColumn:
    name: str
    nullable: bool = True
    order: int | None = None
    sensitive: bool = False
    description: str | None = None
