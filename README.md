# Bollhav

A lightweight config class for defining data pipeline models.

## Usage

### Basic table
```python
model = Model(
    name="orders",
    source_table="raw.orders",
    destination_table="orders",
    destination_schema="public",
    write_mode=WriteMode.TRUNCATE_INSERT,
    destination_db=Database.POSTGRES,
    columns=[
        PostgresColumn(name="id", data_type=PostgresType.BIGINT, primary_key=True, nullable=False),
        PostgresColumn(name="customer_id", data_type=PostgresType.BIGINT),
        PostgresColumn(name="amount", data_type=PostgresType.NUMERIC, precision=10, scale=2),
        PostgresColumn(name="created_at", data_type=PostgresType.TIMESTAMPTZ),
    ],
)
```

### View
```python
model = Model(
    name="orders_view",
    source_table="raw.orders",
    destination_table="orders_view",
    destination_schema="public",
    model_type=ModelType.VIEW,
    write_mode=WriteMode.VIEW,
    destination_db=Database.POSTGRES,
    columns=[
        PostgresColumn(name="id", data_type=PostgresType.BIGINT),
        PostgresColumn(name="amount", data_type=PostgresType.NUMERIC, precision=10, scale=2),
    ],
)
```

### With a schedule
```python
model = Model(
    name="orders",
    source_table="raw.orders",
    destination_db=Database.POSTGRES,
    columns=[
        PostgresColumn(name="id", data_type=PostgresType.BIGINT),
        PostgresColumn(name="amount", data_type=PostgresType.NUMERIC, precision=10, scale=2),
    ],
    cron="0 3 * * *",
)
model.batch_size  # BatchSize.DAILY
```

`batch_size` is inferred from the cron expression and is read-only.

| `BatchSize` | Example cron  |
|-------------|---------------|
| `YEARLY`    | `0 0 1 1 *`   |
| `MONTHLY`   | `0 0 1 * *`   |
| `WEEKLY`    | `0 0 * * 0`   |
| `DAILY`     | `0 3 * * *`   |
| `HOURLY`    | `0 * * * *`   |

### With dynamic kwargs
```python
def my_ddl(table_name: str, schema: str, **kwargs) -> str:
    return f"CREATE TABLE {schema}.{table_name} (id SERIAL PRIMARY KEY);"

model = Model(
    name="orders",
    source_table="raw.orders",
    destination_db=Database.POSTGRES,
    columns=[
        PostgresColumn(name="id", data_type=PostgresType.BIGINT, primary_key=True, nullable=False),
    ],
    destination_ddl=my_ddl,
    table_name="orders",
    schema="public",
)
model.extra["destination_ddl"]  # resolved DDL string
```

Callables in `**kwargs` are resolved at init using the non-callable kwargs as arguments.

## Write modes

| `WriteMode`        | `ModelType` | Description                   |
|--------------------|-------------|-------------------------------|
| `APPEND`           | `TABLE`     | Insert without truncating     |
| `TRUNCATE_INSERT`  | `TABLE`     | Truncate then insert          |
| `OVERWRITE_INSERT` | `TABLE`     | Overwrite matching rows       |
| `MERGE`            | `TABLE`     | Upsert based on keys          |
| `VIEW`             | `VIEW`      | Create or replace view        |

`ModelType` and `WriteMode` are validated against each other at init.

## PostgresColumn

| Field         | Type            | Default  | Notes                        |
|---------------|-----------------|----------|------------------------------|
| `name`        | `str`           | required |                              |
| `data_type`   | `PostgresType`  | required |                              |
| `nullable`    | `bool`          | `True`   |                              |
| `order`       | `int \| None`   | `None`   |                              |
| `primary_key` | `bool`          | `False`  | Implies `nullable=False`     |
| `unique`      | `bool`          | `False`  |                              |
| `precision`   | `int \| None`   | `None`   | For `NUMERIC`, `DECIMAL`     |
| `scale`       | `int \| None`   | `None`   | For `NUMERIC`, `DECIMAL`     |
| `length`      | `int \| None`   | `None`   | For `VARCHAR`, `CHAR`, `BIT` |

`primary_key=True` with `nullable=True` raises at init.

## Tags

Optional and freeform.
```python
model = Model(
    name="orders",
    source_table="raw.orders",
    destination_db=Database.POSTGRES,
    columns=[
        PostgresColumn(name="id", data_type=PostgresType.BIGINT),
    ],
    tags=["finance", "critical"],
)
```