from dataclasses import dataclass
from enum import Enum

from bollhav.model.database import DatabaseColumn


class IcebergPrimaryKeyNotNullableError(ValueError):
    """An Iceberg column was declared both `primary_key=True` and
    `nullable=True`. Key columns become the table's identifier fields, which
    Iceberg requires to be non-null."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Column {name!r}: primary_key=True cannot be nullable")


class IcebergType(Enum):
    """Iceberg's primitive types, named as Iceberg names them."""

    BOOLEAN = "boolean"
    INT = "int"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"
    DATE = "date"
    TIME = "time"
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
        data_type:   Iceberg data type. Defaults to STRING.
        primary_key: Marks the column as part of the row identity. The key
                     columns are the merge key for WriteMode.UPSERT_NO_DELETE
                     and the table's identifier fields. Cannot be nullable.
        precision:   Total digits (DECIMAL).
        scale:       Digits right of the decimal point (DECIMAL).
    """

    data_type: IcebergType = IcebergType.STRING
    primary_key: bool = False
    precision: int | None = None
    scale: int | None = None

    def __post_init__(self) -> None:
        if self.primary_key and self.nullable:
            raise IcebergPrimaryKeyNotNullableError(self.name)
