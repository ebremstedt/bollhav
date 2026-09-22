"""Model columns -> Iceberg / arrow schemas.

The table is created from the DECLARED schema (IcebergColumn per column), and
every chunk is cast to its arrow equivalent before writing — so chunk-to-chunk
inference drift (an all-NULL column, int32 vs int64, a non-UTC timezone) never
reaches the table.
"""

import pyarrow as pa  # pyright: ignore[reportMissingImports]  # optional iceberg extra
from pyiceberg.schema import Schema  # pyright: ignore[reportMissingImports]  # optional iceberg extra
from pyiceberg.types import (  # pyright: ignore[reportMissingImports]  # optional iceberg extra
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

from bollhav.iceberg.columns import IcebergColumn, IcebergType
from bollhav.model.model import Model


class NotIcebergColumnsError(TypeError):
    """An Iceberg target's columns must all be `IcebergColumn` — the table is
    created from the declared types, so an untyped column can't be mapped."""

    def __init__(self, full_name: str, column_name: str) -> None:
        super().__init__(
            f"{full_name!r}: column {column_name!r} is not an IcebergColumn — "
            f"Iceberg targets declare their types"
        )


def _iceberg_type(column: IcebergColumn):
    match column.data_type:
        case IcebergType.BOOLEAN:
            return BooleanType()
        case IcebergType.INT:
            return IntegerType()
        case IcebergType.LONG:
            return LongType()
        case IcebergType.FLOAT:
            return FloatType()
        case IcebergType.DOUBLE:
            return DoubleType()
        case IcebergType.DECIMAL:
            return DecimalType(column.precision or 38, column.scale or 0)
        case IcebergType.DATE:
            return DateType()
        case IcebergType.TIMESTAMP:
            return TimestampType()
        case IcebergType.TIMESTAMPTZ:
            return TimestamptzType()
        case IcebergType.STRING:
            return StringType()
        case IcebergType.BINARY:
            return BinaryType()


def _arrow_type(column: IcebergColumn) -> pa.DataType:
    match column.data_type:
        case IcebergType.BOOLEAN:
            return pa.bool_()
        case IcebergType.INT:
            return pa.int32()
        case IcebergType.LONG:
            return pa.int64()
        case IcebergType.FLOAT:
            return pa.float32()
        case IcebergType.DOUBLE:
            return pa.float64()
        case IcebergType.DECIMAL:
            return pa.decimal128(column.precision or 38, column.scale or 0)
        case IcebergType.DATE:
            return pa.date32()
        case IcebergType.TIMESTAMP:
            return pa.timestamp("us")
        case IcebergType.TIMESTAMPTZ:
            return pa.timestamp("us", "UTC")
        case IcebergType.STRING:
            return pa.string()
        case IcebergType.BINARY:
            return pa.binary()


def _typed_columns(model: Model) -> list[IcebergColumn]:
    columns = []
    for column in model.target.columns:
        if not isinstance(column, IcebergColumn):
            raise NotIcebergColumnsError(model.target.full_name, column.name)
        columns.append(column)
    return columns


def iceberg_schema(model: Model) -> Schema:
    return Schema(
        *[
            NestedField(
                field_id=field_id,
                name=column.name,
                field_type=_iceberg_type(column),
                required=not column.nullable,
            )
            for field_id, column in enumerate(_typed_columns(model), start=1)
        ]
    )


def arrow_schema(model: Model) -> pa.Schema:
    return pa.schema(
        [
            pa.field(column.name, _arrow_type(column), nullable=column.nullable)
            for column in _typed_columns(model)
        ]
    )
