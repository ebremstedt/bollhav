from dataclasses import dataclass, fields
from bollhav.database import DatabaseColumn
from enum import Enum


class ParquetType(Enum):
    BOOLEAN = "BOOLEAN"
    INT32 = "INT32"
    INT64 = "INT64"
    INT96 = "INT96"  # deprecated, legacy only
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    BYTE_ARRAY = "BYTE_ARRAY"
    FIXED_LEN_BYTE_ARRAY = "FIXED_LEN_BYTE_ARRAY"


@dataclass
class ParquetColumn(DatabaseColumn):
    data_type: ParquetType = ParquetType.BYTE_ARRAY
    length: int | None = None
    precision: int | None = None
    scale: int | None = None

    def __post_init__(self) -> None:
        if self.data_type == ParquetType.FIXED_LEN_BYTE_ARRAY and self.length is None:
            raise ValueError(
                f"Column {self.name!r}: FIXED_LEN_BYTE_ARRAY requires length"
            )

        decimal_types = {
            ParquetType.INT32,
            ParquetType.INT64,
            ParquetType.FIXED_LEN_BYTE_ARRAY,
        }
        has_decimal_annotation = self.precision is not None or self.scale is not None

        if has_decimal_annotation and self.data_type not in decimal_types:
            raise ValueError(
                f"Column {self.name!r}: precision/scale (DECIMAL annotation) only valid "
                f"on INT32, INT64, or FIXED_LEN_BYTE_ARRAY, not {self.data_type.value}"
            )
        if has_decimal_annotation and not (
            self.precision is not None and self.scale is not None
        ):
            raise ValueError(
                f"Column {self.name!r}: precision and scale must both be set for DECIMAL annotation"
            )

    def __repr__(self) -> str:
        base_parts = [
            f"{f.name}={getattr(self, f.name)!r}" for f in fields(DatabaseColumn)
        ]
        parts = base_parts + [f"data_type={self.data_type}"]
        if self.length is not None:
            parts.append(f"length={self.length}")
        if self.precision is not None:
            parts.append(f"precision={self.precision}")
        if self.scale is not None:
            parts.append(f"scale={self.scale}")
        return f"ParquetColumn({', '.join(parts)})"
