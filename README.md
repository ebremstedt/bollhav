# Bollhav

A lightweight config class for defining data pipeline models. The goal is simple: standardize how models are declared across a project without boxing anyone in.

## Usage

### Basic table

```python
model = Model(
    name="orders",
    source_table="raw.orders",
    destination_table="orders",
    destination_schema="public",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns={
        "id": pl.Int64,
        "customer_id": pl.Int64,
        "amount": pl.Float64,
        "created_at": pl.Datetime,
    },
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
    columns={
        "id": pl.Int64,
        "amount": pl.Float64,
    },
)
```

### With a schedule

```python
model = Model(
    name="orders",
    source_table="raw.orders",
    destination_table="orders",
    destination_schema="public",
    write_mode=WriteMode.APPEND,
    columns={"id": pl.Int64, "amount": pl.Float64},
    cron="0 3 * * *",
)

model.batch_size  # BatchSize.DAILY
```

`batch_size` is inferred automatically from the cron expression — a daily cron means you're pulling a day's worth of data, a monthly cron a month's worth, and so on. It is read-only and not something you set manually.

| `BatchSize`  | Example cron      |
|--------------|-------------------|
| `YEARLY`     | `0 0 1 1 *`       |
| `MONTHLY`    | `0 0 1 * *`       |
| `WEEKLY`     | `0 0 * * 0`       |
| `DAILY`      | `0 3 * * *`       |
| `HOURLY`     | `0 * * * *`       |
| `SUBHOURLY`  | `*/15 * * * *`    |

### With dynamic DDL via callable

```python
def my_ddl(table_name: str, schema: str, **kwargs) -> str:
    return f"CREATE TABLE {schema}.{table_name} (id SERIAL PRIMARY KEY);"

model = Model(
    name="orders",
    source_table="raw.orders",
    destination_table="orders",
    destination_schema="public",
    columns={"id": pl.Int64},
    destination_ddl=my_ddl,
    table_name="orders",
    schema="public",
)
# model.extra["destination_ddl"] is the resolved DDL string
```

Callables in `**kwargs` are resolved at init time using the non-callable values in the same `kwargs` as arguments. This means you can define DDL, index creation, or any other dynamic logic as functions and pass them in alongside the data they need — no subclassing required.

## Column definitions

Columns are defined using Polars dtypes as the source of truth. Conversion to the target database's type system (e.g. `pl.Int64 -> BIGINT` for Postgres) is left to the implementor — Bollhav makes no assumptions about your destination.

## Write modes

| `WriteMode`        | `ModelType` | Description                        |
|--------------------|-------------|------------------------------------|
| `APPEND`           | `TABLE`     | Insert without truncating          |
| `TRUNCATE_INSERT`  | `TABLE`     | Truncate then insert               |
| `OVERWRITE_INSERT` | `TABLE`     | Overwrite matching rows            |
| `MERGE`            | `TABLE`     | Upsert based on keys               |
| `VIEW`             | `VIEW`      | Create or replace view             |

`ModelType` and `WriteMode` are validated against each other at init — passing `WriteMode.VIEW` with `ModelType.TABLE` or vice versa raises immediately.

## Tags

Tags are optional and freeform. Use them however makes sense for your project — filtering, grouping, documentation.

```python
model = Model(
    name="orders",
    source_table="raw.orders",
    columns={"id": pl.Int64},
    tags=["finance", "critical"],
)
```