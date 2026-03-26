[back to README](../README.md)

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
from bollhav.mssql import ensure_schema, ensure_table, ensure_schema_and_table
```

| Function                  | Description                                                        |
|---------------------------|--------------------------------------------------------------------|
| `ensure_schema`           | Creates the schema if it does not exist                            |
| `ensure_table`            | Creates the table if it does not exist; adds UNIQUE constraint if any columns have `unique=True` |
| `ensure_schema_and_table` | Calls both; the usual entry point                                  |

# Write Modes

See [MODES.md](MODES.md) for general concepts. Below describes the MSSQL-specific implementation.

## TRUNCATE_TABLE_INSERT

Runs `TRUNCATE TABLE` then bulk-inserts all rows using `cursor.fast_executemany`. Committed in one transaction.

```python
from bollhav.mssql import write
from bollhav.model import WriteMode

target = Target(..., write_mode=WriteMode.TRUNCATE_TABLE_INSERT)
write(conn=conn, model=model, df_gen=df_gen)
```

## UPSERT_NO_DELETE

Loads data into a session-scoped temp table (`#tmp_<table>`), then runs a `MERGE` statement:

- `WHEN MATCHED THEN UPDATE SET ...` — updates all non-key columns
- `WHEN NOT MATCHED THEN INSERT ...` — inserts new rows

Requires at least one column with `unique=True` to form the `ON` clause. Committed in one transaction.

```python
MssqlColumn(name="id", data_type=MssqlType.INT, unique=True, nullable=False)
```

If all columns are part of the unique key (no non-key columns), the `WHEN MATCHED` clause is omitted (insert-only merge).

## VIEW

Runs `CREATE OR ALTER VIEW`. Requires `model.source.query` to be set. No dataframe is consumed.

```python
from bollhav.model import WriteMode

target = Target(..., write_mode=WriteMode.VIEW)
source = Source(..., query="SELECT id, name FROM dbo.raw_table")
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

## `merge` / `truncate_write` / `create_replace_view`

Low-level functions if you need direct control. Prefer `write` in pipelines.
