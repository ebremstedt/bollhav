from dataclasses import dataclass, fields
from bollhav.database import DatabaseColumn
from enum import Enum


class PostgresType(Enum):
    BIGINT = "BIGINT"
    BIGSERIAL = "BIGSERIAL"
    BIT = "BIT"
    BOOLEAN = "BOOLEAN"
    BOX = "BOX"
    BYTEA = "BYTEA"
    CHAR = "CHAR"
    CHARACTER_VARYING = "CHARACTER VARYING"
    CIDR = "CIDR"
    CIRCLE = "CIRCLE"
    DATE = "DATE"
    DATERANGE = "DATERANGE"
    DATEMULTIRANGE = "DATEMULTIRANGE"
    DECIMAL = "DECIMAL"
    DOUBLE_PRECISION = "DOUBLE PRECISION"
    FLOAT4 = "FLOAT4"
    FLOAT8 = "FLOAT8"
    INET = "INET"
    INT2 = "INT2"
    INT4 = "INT4"
    INT4MULTIRANGE = "INT4MULTIRANGE"
    INT4RANGE = "INT4RANGE"
    INT8 = "INT8"
    INT8MULTIRANGE = "INT8MULTIRANGE"
    INT8RANGE = "INT8RANGE"
    INTEGER = "INTEGER"
    INTERVAL = "INTERVAL"
    JSON = "JSON"
    JSONB = "JSONB"
    LINE = "LINE"
    LSEG = "LSEG"
    MACADDR = "MACADDR"
    MACADDR8 = "MACADDR8"
    MONEY = "MONEY"
    NUMMULTIRANGE = "NUMMULTIRANGE"
    NUMRANGE = "NUMRANGE"
    NUMERIC = "NUMERIC"
    PATH = "PATH"
    POINT = "POINT"
    POLYGON = "POLYGON"
    REAL = "REAL"
    SERIAL = "SERIAL"
    SMALLINT = "SMALLINT"
    SMALLSERIAL = "SMALLSERIAL"
    TEXT = "TEXT"
    TEXT_ARRAY = "TEXT[]"
    TIME = "TIME"
    TIMETZ = "TIMETZ"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMPTZ"
    TSMULTIRANGE = "TSMULTIRANGE"
    TSQUERY = "TSQUERY"
    TSRANGE = "TSRANGE"
    TSTZMULTIRANGE = "TSTZMULTIRANGE"
    TSTZRANGE = "TSTZRANGE"
    TSVECTOR = "TSVECTOR"
    UUID = "UUID"
    VARBIT = "VARBIT"
    VARCHAR = "VARCHAR"
    XML = "XML"


@dataclass
class PostgresColumn(DatabaseColumn):
    """
    Defines a single column in a Postgres table.

    Inherits name, nullable, and order from DatabaseColumn.

    Args:
        data_type:   Postgres data type. Defaults to TEXT.
        primary_key: Marks column as primary key. Cannot be nullable.
        unique:      Adds a UNIQUE constraint.
        precision:   Total number of significant digits (NUMERIC/DECIMAL).
        scale:       Digits to the right of the decimal point.
        length:      Max character length (CHAR/VARCHAR).
        description: Optional human-readable description of the column.

    Raises:
        ValueError: If primary_key=True and nullable=True.
    """

    data_type: PostgresType = PostgresType.TEXT
    primary_key: bool = False
    unique: bool = False
    precision: int | None = None
    scale: int | None = None
    length: int | None = None

    def __post_init__(self) -> None:
        if self.primary_key and self.nullable:
            raise ValueError(
                f"Column {self.name!r}: primary_key=True cannot be nullable"
            )

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
        return f"PostgresColumn({', '.join(parts)})"
