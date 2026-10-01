from dataclasses import dataclass
from enum import Enum

from bollhav.model.database import DatabaseColumn


class IcebergType(Enum):
    """Iceberg's primitive types, named as Iceberg names them."""

    BOOLEAN = "boolean"
    INT = "int"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"
    DATE = "date"
    TIMESTAMP = "timestamp"
    TIMESTAMPTZ = "timestamptz"
    STRING = "string"
    BINARY = "binary"


@dataclass
class IcebergColumn(DatabaseColumn):
    """Defines a single column in an Iceberg table.

    Inherits name, nullable, order, sensitive, description and partition_on
    from DatabaseColumn. `nullable=False` becomes a required Iceberg field.

    Args:
        data_type: Iceberg data type. Defaults to STRING.
        precision: Total digits (DECIMAL).
        scale:     Digits right of the decimal point (DECIMAL).
    """

    data_type: IcebergType = IcebergType.STRING
    precision: int | None = None
    scale: int | None = None
