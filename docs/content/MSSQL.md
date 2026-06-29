[Home](index.md) › **MSSQL**

# MssqlColumn

Column definitions for MSSQL targets.

## Usage

```python
from bollhav.mssql import MssqlColumn, MssqlType

MssqlColumn(
    name="amount",
    data_type=MssqlType.DECIMAL,
    nullable=False,
    precision=18,
    scale=4,
)
```

## MssqlType values

| MssqlType          | SQL Server type    | Notes                              |
|--------------------|--------------------|------------------------------------|
| `BIGINT`           | `BIGINT`           |                                    |
| `BIT`              | `BIT`              |                                    |
| `CHAR`             | `CHAR(n)`          | Requires `length`                  |
| `DATE`             | `DATE`             |                                    |
| `DATETIME`         | `DATETIME`         |                                    |
| `DATETIME2`        | `DATETIME2(n)`     | Optional `scale` (0–7)             |
| `DATETIMEOFFSET`   | `DATETIMEOFFSET`   | Timezone-aware datetime            |
| `DECIMAL`          | `DECIMAL(p, s)`    | Requires `precision` and `scale`   |
| `FLOAT`            | `FLOAT`            |                                    |
| `INT`              | `INT`              |                                    |
| `NUMERIC`          | `NUMERIC(p, s)`    | Requires `precision` and `scale`   |
| `NVARCHAR`         | `NVARCHAR(n\|MAX)` | `None` length → `MAX`              |
| `REAL`             | `REAL`             |                                    |
| `SMALLINT`         | `SMALLINT`         |                                    |
| `TIME`             | `TIME`             |                                    |
| `TINYINT`          | `TINYINT`          |                                    |
| `UNIQUEIDENTIFIER` | `UNIQUEIDENTIFIER` |                                    |
| `VARBINARY_MAX`    | `VARBINARY(MAX)`   |                                    |
| `VARCHAR`          | `VARCHAR(n\|MAX)`  | `None` length → `MAX`              |

## MssqlColumn fields

| Field         | Type           | Default          | Description                                         |
|---------------|----------------|------------------|-----------------------------------------------------|
| `name`        | `str`          | required         | Column name                                         |
| `data_type`   | `MssqlType`    | `NVARCHAR`       | SQL Server type                                     |
| `nullable`    | `bool`         | `True`           | Whether NULL is allowed                             |
| `primary_key` | `bool`         | `False`          | Marks as PRIMARY KEY; cannot be nullable            |
| `unique`      | `bool`         | `False`          | Part of the composite UNIQUE constraint on the table|
| `precision`   | `int \| None`  | `None`           | Total digits for DECIMAL/NUMERIC                    |
| `scale`       | `int \| None`  | `None`           | Decimal digits for DECIMAL/NUMERIC, or scale for DATETIME2 |
| `length`      | `int \| None`  | `None`           | Max character length for NVARCHAR/VARCHAR/CHAR; `None` → MAX |

# Schema helpers

```python
from bollhav.mssql import ensure_schema, ensure_table, ensure_primary_key, ensure_schema_and_table
```

| Function                  | Description                                                        |
|---------------------------|--------------------------------------------------------------------|
| `ensure_schema`           | Creates the schema if it does not exist                            |
| `ensure_table`            | Creates the table if it does not exist; adds `<table>_uq` UNIQUE for any `unique=True` columns *not* already covered by the PK |
| `ensure_primary_key`      | Adds `<table>_pk` PRIMARY KEY CLUSTERED if any columns have `primary_key=True` and the table has no PK yet — works on existing tables, idempotent |
| `ensure_schema_and_table` | Calls all three; the usual entry point                             |

`primary_key=True` is added via a separate `ALTER TABLE … ADD CONSTRAINT` (rather than inline in `CREATE TABLE`) so the constraint name is deterministic (`<table>_pk`), composite PKs are supported, and existing tables get the constraint without a recreate. When a column is flagged both `primary_key=True` and `unique=True`, the redundant UNIQUE is skipped — the PK already enforces uniqueness.

# Write Modes

See [Write modes](MODES.md) for general concepts. Below describes the MSSQL-specific implementation.

## APPEND

Bulk inserts all rows using `cursor.fast_executemany`. Committed in one transaction.

```python
from bollhav.mssql import write
from bollhav.model import WriteMode

target = Target(..., write_mode=WriteMode.APPEND)
write(conn=conn, model=model, df_gen=df_gen)
```

## Pre-load flags: `recreate_table` / `truncate_table`

Set on `Target`. Executed once inside `ensure_table` **before** the chunked write loop:

- `recreate_table=True` → `DROP TABLE` (if exists) then `CREATE TABLE`. Resets schema.
- `truncate_table=True` → `CREATE TABLE IF NOT EXISTS` then `TRUNCATE TABLE`. Keeps schema.

```python
target = Target(..., write_mode=WriteMode.APPEND, truncate_table=True)
```

Both `False` by default; both `True` raises. Does not apply to a view (views are not a write mode).

## UPSERT_NO_DELETE

Loads data into a session-scoped temp table (`#tmp_<table>`), then runs a `MERGE` statement:

- `WHEN MATCHED THEN UPDATE SET ...` — updates all non-key columns
- `WHEN NOT MATCHED THEN INSERT ...` — inserts new rows

Requires at least one column with `unique=True` to form the `ON` clause. Committed in one transaction.

```python
MssqlColumn(name="id", data_type=MssqlType.INT, unique=True, nullable=False)
```

If all columns are part of the unique key (no non-key columns), the `WHEN MATCHED` clause is omitted (insert-only merge).

## Views

A view is `materialization=Materialization.VIEW` on the model, not a write mode. Runs `CREATE OR ALTER VIEW` from the model's `query` (its SELECT body, required for a view). No dataframe is consumed.

```python
from bollhav.model import Materialization, Temporality, Model, Target

model = Model(
    target=Target(...),
    temporality=Temporality.TIMELESS,
    materialization=Materialization.VIEW,
    query="SELECT id, name FROM dbo.raw_table",
)
write(conn=conn, model=model)   # no df_gen
```

# Entry points

## `write`

The main entry point. Routes to the correct implementation based on `model.target.write_mode`.

```python
from bollhav.mssql import write

write(
    conn=conn,           # pyodbc.Connection (autocommit=False recommended)
    model=model,
    df_gen=df_gen,       # Generator[pl.DataFrame, None, None] — omit for VIEW
    create_if_missing=True,  # call ensure_schema_and_table before writing
)
```

## `write_dataframes`

Like `write` but skips VIEW handling and always expects a dataframe generator.

## `append` / `merge` / `create_replace_view`

Low-level functions if you need direct control. Prefer `write` in pipelines.
