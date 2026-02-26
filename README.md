# bollhav

Model definition framework for data pipeline targets.

## Implementations
[Postgres](README_postgres.md) \
[Parquet](README_parquet.md)

---

## Installation

```bash
pip install bollhav
```

## Model

```python
from bollhav import Model, ModelType, WriteMode, Database, PostgresColumn, PostgresType

model = Model(
    name="orders",
    source_entity="raw.orders",
    table="orders",
    schema="public",
    database=Database.POSTGRES,
    columns=[
        PostgresColumn(name="id", data_type=PostgresType.BIGINT, primary_key=True, nullable=False, order=0),
        PostgresColumn(name="created_at", data_type=PostgresType.TIMESTAMPTZ, nullable=False, order=1),
        PostgresColumn(name="email", data_type=PostgresType.TEXT, nullable=True, order=2, sensitive=True),
    ],
    write_mode=WriteMode.APPEND,
    cron="0 3 * * *",
    partitioned_by=["created_at"],
)
```

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Unique identifier for the model |
| `source_entity` | `str` | required | Source table or view to read from |
| `table` | `str` | `""` | Destination table name |
| `schema` | `str` | `""` | Destination schema name |
| `database` | `Database` | `None` | Target database. Required if `columns` is set |
| `columns` | `list[PostgresColumn \| ParquetColumn]` | `None` | Column definitions. Required if `database` is set |
| `model_type` | `ModelType` | `TABLE` | `TABLE` or `VIEW` |
| `write_mode` | `WriteMode` | `APPEND` | How to write data. `VIEW` requires `ModelType.VIEW` |
| `tags` | `list[str]` | `None` | Labels for filtering |
| `cron` | `str` | `None` | Cron expression. Automatically infers `batch_size` |
| `enabled` | `bool` | `True` | Whether the model is active |
| `debug` | `bool` | `False` | Enables debug mode |
| `description` | `str` | `None` | Human-readable description |
| `source_dsn` | `str` | `None` | DSN for the source connection |
| `source_query` | `str` | `None` | Optional query to use instead of `source_entity` |
| `partitioned_by` | `list[str]` | `None` | Column names to partition by. Must exist in `columns` |
| `**kwargs` | | | Extra metadata. Callable values are resolved with non-callable kwargs as arguments |

### Computed attributes

| Attribute | Description |
|---|---|
| `batch_size` | Inferred from `cron` if set, otherwise `None` |
| `sensitive` | `True` if any column has `sensitive=True` |

## Databases

```python
from bollhav import Database

Database.POSTGRES
Database.PARQUET
```

## Write modes

```python
from bollhav import WriteMode

WriteMode.APPEND
WriteMode.VIEW     # Must be used with ModelType.VIEW
```

## Extra kwargs

Non-reserved keyword arguments are stored in `model.extra`. Callable values are resolved at init time using the non-callable kwargs as arguments.

```python
model = Model(
    name="orders",
    source_entity="raw.orders",
    static="production",
    env=lambda static: f"env={static}",
)

model.extra  # {"static": "production", "env": "env=production"}
```