# PostgresColumn

Column definitions for Postgres targets.

**Back to** [Model](README.md)

---

## Usage

```python
from bollhav import PostgresColumn, PostgresType

PostgresColumn(
    name="amount",
    data_type=PostgresType.NUMERIC,
    nullable=False,
    order=0,
    precision=18,
    scale=4,
    sensitive=False,
    description="Order total in USD",
)
```

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Column name |
| `data_type` | `PostgresType` | `TEXT` | Postgres data type |
| `nullable` | `bool` | `True` | Whether the column allows nulls |
| `order` | `int` | `None` | Column position |
| `sensitive` | `bool` | `False` | Marks column as sensitive. Propagates to `Model.sensitive` |
| `primary_key` | `bool` | `False` | Marks column as primary key. Cannot be nullable |
| `unique` | `bool` | `False` | Adds a UNIQUE constraint |
| `precision` | `int` | `None` | Total significant digits (NUMERIC/DECIMAL) |
| `scale` | `int` | `None` | Digits to the right of the decimal point |
| `length` | `int` | `None` | Max character length (CHAR/VARCHAR) |
| `description` | `str` | `None` | Human-readable description |

## Types

```python
class PostgresType(Enum):
    # Integers
    SMALLINT, INT2
    INTEGER, INT4
    BIGINT, INT8
    SMALLSERIAL, SERIAL, BIGSERIAL

    # Floats
    REAL, FLOAT4
    DOUBLE_PRECISION, FLOAT8

    # Exact numerics
    DECIMAL, NUMERIC
    MONEY

    # Strings
    CHAR, CHARACTER_VARYING, VARCHAR, TEXT
    BYTEA

    # Date/time
    TIMESTAMP, TIMESTAMPTZ
    DATE
    TIME, TIMETZ
    INTERVAL

    # Boolean
    BOOLEAN

    # Geometric
    POINT, LINE, LSEG, BOX, PATH, POLYGON, CIRCLE

    # Network
    CIDR, INET, MACADDR, MACADDR8

    # Bit strings
    BIT, VARBIT

    # Text search
    TSVECTOR, TSQUERY

    # Misc
    UUID, XML, JSON, JSONB

    # Ranges
    INT4RANGE, INT8RANGE, NUMRANGE
    TSRANGE, TSTZRANGE, DATERANGE

    # Multiranges
    INT4MULTIRANGE, INT8MULTIRANGE, NUMMULTIRANGE
    TSMULTIRANGE, TSTZMULTIRANGE, DATEMULTIRANGE
```