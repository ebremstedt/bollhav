[Home](index.md) › **Iceberg**

# IcebergColumn

Column definitions for Iceberg targets. The table is created from the declared columns and every chunk is cast to them before writing.

## Usage

```python
from bollhav.iceberg import IcebergColumn, IcebergType

IcebergColumn(
    name="amount",
    data_type=IcebergType.DECIMAL,
    nullable=False,
    order=0,
    precision=18,
    scale=4,
    description="Order total in USD",
)
```

## IcebergType values

| Value | Notes |
|---|---|
| `BOOLEAN` | |
| `INT` / `LONG` | 32 / 64-bit |
| `FLOAT` / `DOUBLE` | 32 / 64-bit |
| `DECIMAL` | `precision` / `scale`, default 38 / 0 |
| `DATE` / `TIME` | |
| `TIMESTAMP` | no zone |
| `TIMESTAMPTZ` | UTC only, tz-aware input is normalized on write |
| `STRING` | default. Iceberg has no JSON type, use this |
| `BINARY` | |

## IcebergColumn fields

Inherits `name`, `nullable`, `order`, `sensitive`, `description`, `partition_on` from `DatabaseColumn`.

- `nullable=False` → required field.
- `primary_key=True` → identifier field and the merge key for `UPSERT_NO_DELETE`. Cannot be nullable.
- `partition_on=True` → partition spec, at most one column. By day for `DATE` / `TIMESTAMP` / `TIMESTAMPTZ`, by value otherwise.

# Target

```python
Target(
    name="orders",             # table
    schema="sales",            # namespace
    catalog="lake",            # pyiceberg catalog name, required
    database=Database.ICEBERG,
    columns=[...],
)
```

- `catalog` is required. The app connects the catalog and passes it as `data_conn`.
- `writer` defaults to `Writer.PYICEBERG`. `Writer.TRINO` is for views only.
- `indexes` and `staging` are rejected. Iceberg has neither.
- `recreate_table` → drop + create. `truncate_table` → delete all rows in one commit. Both run once, before the write loop.

# Write Modes

See [Write modes](MODES.md) for the concepts. Chunks are polars or pyarrow, empty ones skipped, each cast to the declared schema. **Every chunk is one Iceberg commit.**

## APPEND

`Table.append` per chunk.

## RECREATE_PARTITION

Requires `since`, `until` and a `partition_on` column. First chunk deletes `>= since AND < until` and appends, in one commit. Remaining chunks append. Idempotent per window.

## UPSERT_NO_DELETE

Joins on the `primary_key` columns. Raises without them, and raises on duplicate keys within a chunk. Single key → pyiceberg `Table.upsert`. Composite key → bollhav's own overwrite path, since pyiceberg's crashes on large chunks.

## Views

`materialization=Materialization.VIEW` plus `writer=Writer.TRINO`. The lifecycle runs `CREATE OR REPLACE VIEW catalog.schema.name COMMENT '<description>' AS <query>` through Trino, because pyiceberg's SQL and Hive catalogs cannot create views. The query is Trino SQL.

# Connections

| writer | `data_conn` |
|---|---|
| `Writer.PYICEBERG` (tables) | a pyiceberg `Catalog` |
| `Writer.TRINO` (views) | a Trino DB-API connection |

A mismatch raises when the lifecycle builds the handler. State still lives in Postgres: a stateful model needs a Postgres `state_conn` alongside the catalog.

# Entry points

```python
from bollhav.iceberg import write, write_dataframes

write(conn, run, df_gen, since=None, until=None)             # same signature as bollhav.postgres.write
write_dataframes(catalog, model, df_gen, since=None, until=None)
```

`append`, `upsert` and `overwrite` are the per-chunk operations behind them, on a loaded `Table` and a cast arrow chunk.

# Install

```bash
pip install 'bollhav[iceberg]'      # pyiceberg + pyarrow
```

Add your catalog driver, e.g. `pyiceberg[sql-sqlite]` locally or `pyiceberg[hive]` for a metastore. `IcebergColumn` and `IcebergType` import without the extra.

Runnable local example: [examples/iceberg](https://github.com/ebremstedt/bollhav/tree/main/examples/iceberg).
