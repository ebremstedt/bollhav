[back to README](..README.md)

# PostgresColumn

Column definitions for Postgres targets.

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