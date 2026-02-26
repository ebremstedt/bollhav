# ParquetColumn

Column definitions for Parquet targets.

**Back to** [Model](README.md)

---

## Usage

```python
from bollhav.parquet import ParquetColumn, ParquetType

ParquetColumn(
    name="amount",
    data_type=ParquetType.INT64,
    nullable=False,
    order=0,
    precision=18,
    scale=4,
    sensitive=False,
    description="Order total",
)
```

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Column name |
| `data_type` | `ParquetType` | `BYTE_ARRAY` | Parquet primitive type |
| `nullable` | `bool` | `True` | Whether the column allows nulls |
| `order` | `int` | `None` | Column position |
| `sensitive` | `bool` | `False` | Marks column as sensitive. Propagates to `Model.sensitive` |
| `length` | `int` | `None` | Required when `data_type` is `FIXED_LEN_BYTE_ARRAY` |
| `precision` | `int` | `None` | Required alongside `scale` for DECIMAL annotation |
| `scale` | `int` | `None` | Required alongside `precision` for DECIMAL annotation |
| `description` | `str` | `None` | Human-readable description |

## Types

Parquet defines 8 primitive types. Higher-level types (strings, dates, decimals) are expressed as logical type annotations on top of these primitives.

| Type | Description |
|---|---|
| `BOOLEAN` | 1-bit boolean |
| `INT32` | 32-bit signed integer |
| `INT64` | 64-bit signed integer |
| `INT96` | 96-bit signed integer (deprecated, legacy only) |
| `FLOAT` | IEEE 32-bit floating point |
| `DOUBLE` | IEEE 64-bit floating point |
| `BYTE_ARRAY` | Arbitrarily long byte array |
| `FIXED_LEN_BYTE_ARRAY` | Fixed-length byte array. Requires `length` |

## DECIMAL annotation

Precision and scale can only be set on `INT32`, `INT64`, or `FIXED_LEN_BYTE_ARRAY`. Both must be set together.

```python
ParquetColumn(
    name="price",
    data_type=ParquetType.INT64,
    nullable=False,
    order=0,
    precision=18,
    scale=4,
)
```