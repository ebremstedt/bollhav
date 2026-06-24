from dataclasses import dataclass, fields
from enum import Enum
from bollhav.model.database import DatabaseColumn
from bollhav.mssql.messages.error import NullablePrimaryKeyColumnError


class MssqlType(Enum):
    BIGINT = "BIGINT"
    BIT = "BIT"
    CHAR = "CHAR"
    DATE = "DATE"
    DATETIME = "DATETIME"
    DATETIME2 = "DATETIME2"
    DATETIMEOFFSET = "DATETIMEOFFSET"
    DECIMAL = "DECIMAL"
    FLOAT = "FLOAT"
    INT = "INT"
    NVARCHAR = "NVARCHAR"
    NUMERIC = "NUMERIC"
    REAL = "REAL"
    SMALLINT = "SMALLINT"
    TIME = "TIME"
    TINYINT = "TINYINT"
    UNIQUEIDENTIFIER = "UNIQUEIDENTIFIER"
    VARBINARY_MAX = "VARBINARY(MAX)"
    VARCHAR = "VARCHAR"


@dataclass
class MssqlColumn(DatabaseColumn):
    """
    Defines a single column in an MSSQL table.

    Inherits name, nullable, order, sensitive, and description from DatabaseColumn.

    Args:
        data_type:   MSSQL data type. Defaults to NVARCHAR.
        primary_key: Marks column as primary key. Cannot be nullable.
        unique:      Marks column as part of the composite UNIQUE constraint.
        precision:   Total digits (DECIMAL/NUMERIC).
        scale:       Digits right of the decimal (DECIMAL/NUMERIC/DATETIME2).
        length:      Max character length. None means MAX for NVARCHAR/VARCHAR.
    """

    data_type: MssqlType = MssqlType.NVARCHAR
    primary_key: bool = False
    unique: bool = False
    precision: int | None = None
    scale: int | None = None
    length: int | None = None

    def __post_init__(self) -> None:
        if self.primary_key and self.nullable:
            raise NullablePrimaryKeyColumnError(self.name)

    def __repr__(self) -> str:
        base_parts = [
            f"{f.name}={getattr(self, f.name)!r}" for f in fields(DatabaseColumn)
        ]
        parts = base_parts + [
            f"data_type={self.data_type}",
            f"primary_key={self.primary_key}",
            f"unique={self.unique}",
        ]
        if self.precision is not None:
            parts.append(f"precision={self.precision}")
        if self.scale is not None:
            parts.append(f"scale={self.scale}")
        if self.length is not None:
            parts.append(f"length={self.length}")
        return f"MssqlColumn({', '.join(parts)})"
