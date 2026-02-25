from dataclasses import dataclass
from bollhav.database import DatabaseColumn
from enum import Enum


class PostgresType(Enum):
    SMALLINT = "SMALLINT"
    INT2 = "INT2"
    INTEGER = "INTEGER"
    INT4 = "INT4"
    BIGINT = "BIGINT"
    INT8 = "INT8"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    REAL = "REAL"
    FLOAT4 = "FLOAT4"
    DOUBLE_PRECISION = "DOUBLE PRECISION"
    FLOAT8 = "FLOAT8"
    SMALLSERIAL = "SMALLSERIAL"
    SERIAL = "SERIAL"
    BIGSERIAL = "BIGSERIAL"
    MONEY = "MONEY"
    CHAR = "CHAR"
    CHARACTER_VARYING = "CHARACTER VARYING"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    BYTEA = "BYTEA"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMPTZ"
    DATE = "DATE"
    TIME = "TIME"
    TIMETZ = "TIMETZ"
    INTERVAL = "INTERVAL"
    BOOLEAN = "BOOLEAN"
    POINT = "POINT"
    LINE = "LINE"
    LSEG = "LSEG"
    BOX = "BOX"
    PATH = "PATH"
    POLYGON = "POLYGON"
    CIRCLE = "CIRCLE"
    CIDR = "CIDR"
    INET = "INET"
    MACADDR = "MACADDR"
    MACADDR8 = "MACADDR8"
    BIT = "BIT"
    VARBIT = "VARBIT"
    TSVECTOR = "TSVECTOR"
    TSQUERY = "TSQUERY"
    UUID = "UUID"
    XML = "XML"
    JSON = "JSON"
    JSONB = "JSONB"
    INT4RANGE = "INT4RANGE"
    INT8RANGE = "INT8RANGE"
    NUMRANGE = "NUMRANGE"
    TSRANGE = "TSRANGE"
    TSTZRANGE = "TSTZRANGE"
    DATERANGE = "DATERANGE"
    INT4MULTIRANGE = "INT4MULTIRANGE"
    INT8MULTIRANGE = "INT8MULTIRANGE"
    NUMMULTIRANGE = "NUMMULTIRANGE"
    TSMULTIRANGE = "TSMULTIRANGE"
    TSTZMULTIRANGE = "TSTZMULTIRANGE"
    DATEMULTIRANGE = "DATEMULTIRANGE"


@dataclass
class PostgresColumn(DatabaseColumn):
    data_type: PostgresType = PostgresType.TEXT
    primary_key: bool = False
    unique: bool = False
    precision: int | None = None
    scale: int | None = None
    length: int | None = None
    description: str | None = None

    def __post_init__(self) -> None:
        if self.primary_key and self.nullable:
            raise ValueError(
                f"Column {self.name!r}: primary_key=True cannot be nullable"
            )

    def __repr__(self) -> str:
        return (
            f"PostgresColumn(name={self.name!r}, data_type={self.data_type}, "
            f"nullable={self.nullable}, order={self.order}, primary_key={self.primary_key}, "
            f"unique={self.unique}, precision={self.precision}, scale={self.scale}, "
            f"length={self.length}, description={self.description!r})"
        )
