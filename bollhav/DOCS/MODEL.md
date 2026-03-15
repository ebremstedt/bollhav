[README.md](..README.md)

# Model

Standardizes code at the **model** level

## Model creation example
```python
from bollhav.model import Model, ModelConfig, WriteMode, Database, PostgresColumn, PostgresType, TZInterval
import polars as pl

config = ModelConfig(
    name="orders",
    source_entity="raw.orders",
    table="orders",
    schema="public",
    database=Database.POSTGRES,
    columns=[
        PostgresColumn(name="id", data_type=PostgresType.BIGINT, primary_key=True, nullable=False),
        PostgresColumn(name="created_at", data_type=PostgresType.TIMESTAMPTZ, nullable=False),
        PostgresColumn(name="email", data_type=PostgresType.TEXT, nullable=True, sensitive=True),
    ],
    write_mode=WriteMode.APPEND,
    cron="0 3 * * *",
    partitioned_by="created_at",
)

def execute(interval: TZInterval) -> pl.DataFrame:
    return pl.read_database(
        f"SELECT * FROM {config.source_entity} WHERE created_at >= '{interval.since}' AND created_at < '{interval.until}'",
        connection=...,
    )

model = Model(model_config=config, execute=execute)
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
| `tags` | `set[str]` | `None` | Labels for filtering |
| `cron` | `str` | `None` | Cron expression. Automatically infers `batch_size` |
| `enabled` | `bool` | `True` | Whether the model is active |
| `debug` | `bool` | `False` | Enables debug mode |
| `description` | `str` | `None` | Human-readable description |
| `source_dsn` | `str` | `None` | DSN for the source connection |
| `source_query` | `str` | `None` | Optional query to use instead of `source_entity` |
| `partitioned_by` | `str` | `None` | Column name to partition by. Must exist in `columns` |
| `begin` | `datetime` | `None` | Backfill start — must be UTC-aware |
| `end` | `datetime` | `None` | Backfill end — must be UTC-aware |
| `retries` | `int` | `None` | Retry count on failure |
| `lookback` | `int` | `None` | Lookback window in batch units |
| `tz_aware` | `bool` | `True` | Enforces UTC on `begin`/`end` |
| `**kwargs` | | | Extra metadata. Callable values are resolved with non-callable kwargs as arguments |

### Computed attributes

| Attribute | Description |
|---|---|
| `batch_size` | Inferred from `cron` if set, otherwise `None` |
| `sensitive` | `True` if any column has `sensitive=True` |
| `unique_columns` | Columns with `unique=True` — required for `UPDATE_INSERT` |
| `partitioned_by_index` | `True` if `partitioned_by` is set |


## Write modes

Read more [here](MODES.md)
```python
from bollhav import WriteMode

WriteMode.APPEND
WriteMode.OVERWRITE_INSERT  # requires partitioned_by
WriteMode.TRUNCATE_INSERT
WriteMode.UPDATE_INSERT     # requires at least one column with unique=True
WriteMode.VIEW              # requires ModelType.VIEW
```

## UTC enforcement

When `tz_aware=True` (default), `begin` and `end` must be UTC-aware. Naive or non-UTC datetimes raise `ValueError`.
```python
from datetime import datetime, timezone

model = Model(
    ...,
    begin=datetime(2025, 1, 1, tzinfo=timezone.utc),
    end=datetime(2025, 2, 1, tzinfo=timezone.utc),
)
```

model.extra  # {"static": "production", "env": "env=production"}
```

## Batch intervals

`Model.get_batch_intervals` splits a `TZInterval` into sub-intervals driven by the model's cron expression. Useful for chunked backfills.

```python
from datetime import datetime, timezone
from bollhav.intervals import TZInterval

interval = TZInterval(
    since=datetime(2025, 1, 1, tzinfo=timezone.utc),
    until=datetime(2025, 1, 1, 3, 0, tzinfo=timezone.utc),
)

batches = model.get_batch_intervals(interval)
# With cron="0 * * * *":
# [TZInterval(00:00, 01:00), TZInterval(01:00, 02:00), TZInterval(02:00, 03:00)]
```

Pass `cron_override` to use a different cron expression without changing the model config:
```python
batches = model.get_batch_intervals(interval, cron_override="*/15 * * * *")
```

## Tag filtering

Tags are automatically populated at init time. By default `name`, `schema`, and `"all"` are added.

```python
model = ModelConfig(name="orders", source_entity="raw.orders", schema="public")
model.tags  # {"orders", "public", "all"}
```

Control which tags are auto-added:
```python
ModelConfig(..., name_add_to_tags=False, schema_add_to_tags=False, model_gets_all_tag=False)
```

Use `match_models` to discover and filter model instances from a folder by tag expression:

```python
from bollhav.match_models import match_models

models = match_models(folder="src/models", tags="[orders|payments]")
models = match_models(folder="src/models", tags="[public&reporting]")
models = match_models(folder="src/models", tags="[public&(orders|payments)]")
```

### Tag expression syntax

| Syntax | Meaning |
|---|---|
| `[tag]` | model has `tag` |
| `[a\|b]` | model has `a` OR `b` |
| `[a&b]` | model has `a` AND `b` |
| `[a&(b\|c)]` | model has `a` AND (`b` OR `c`) |
| `[g1],[g2]` | matches `g1` OR `g2` (comma = outer OR) |

Square brackets are required around every group. Only one level of parentheses is supported.

---