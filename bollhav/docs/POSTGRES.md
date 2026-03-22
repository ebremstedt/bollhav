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

# Write Modes

See [MODES.md](MODES.md) for the general concepts. Below describes the Postgres-specific implementation of each mode.

## APPEND

Uses `COPY ... FROM STDIN` inside a transaction. No deduplication or conflict handling.

## RECREATE_INSERT

Runs `DROP TABLE IF EXISTS`, recreates the table via `ensure_schema_and_table`, then uses `COPY`. All in one transaction.

## TRUNCATE_INSERT

Runs `TRUNCATE TABLE` then `COPY`. All in one transaction.

## OVERWRITE_INSERT

Requires `since` and `until` (UTC-aware datetimes) and `target.partitioned_by` to be set. Deletes rows where the partition column is `>= since AND < until`, then uses `COPY`. All in one transaction. IDEMPOTENT.

## UPDATE_INSERT

Loads data into a temp table via `COPY`, then runs `INSERT ... ON CONFLICT (...) DO UPDATE SET ...`. Requires `target.unique_columns` to be set. The temp table is dropped on commit.

## VIEW

Runs `CREATE OR REPLACE VIEW`. Requires `model.source.query` to be set. No dataframe needed.