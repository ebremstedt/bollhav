[Home](index.md) › **Postgres**

# PostgresColumn

Column definitions for Postgres targets.

## Usage

```python
from bollhav.postgres import PostgresColumn, PostgresType

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

## Pre-load flags: `recreate_table` / `truncate_table`

Both live on `Target`. They run once in `ensure_table` **before** the chunked write loop starts:

- `recreate_table=True` → `DROP TABLE IF EXISTS` then `CREATE TABLE` (schema reset).
- `truncate_table=True` → `CREATE TABLE IF NOT EXISTS` then `TRUNCATE TABLE` (rows wiped, schema kept).

Both default to `False`; setting both raises. Combine with any non-VIEW write mode — typically `APPEND` for a full reload, or `UPSERT_NO_DELETE` when you also want dedup after the wipe.

## RECREATE_PARTITION

Requires `since` and `until` (UTC-aware datetimes) and `target.partitioned_by` to be set. Deletes rows where the partition column is `>= since AND < until`, then uses `COPY`. All in one transaction. IDEMPOTENT.

## UPSERT_NO_DELETE

Loads data into a temp table via `COPY`, then runs `INSERT ... ON CONFLICT (...) DO UPDATE SET ...`. Requires `target.unique_columns` to be set. The temp table is dropped on commit.

## VIEW

Runs `CREATE OR REPLACE VIEW`. Requires a `Source` in `upstream` whose `SourceModel.query` is set (the view's definition). No dataframe needed.