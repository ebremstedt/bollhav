[back to README](..README.md)

# Model

Standardizes code at the **model** level.

A `Model` is a pure data object describing what data looks like and where it goes. It does not contain execution logic — that lives in a separate `execute` function that receives the model as a parameter.

## Model creation example

```python
from bollhav.model.model import Model
from bollhav.model.target import Target
from bollhav.model.source import Source
from bollhav.model.bounds import Bounds
from bollhav.model.batch import Batch
from bollhav.model.schema import Schema
from bollhav.model.write_modes import WriteMode
from bollhav.postgres.columns import PostgresColumn, PostgresType
from bollhav.model.database import Database

model = Model(
    name="orders",
    target=Target(
        name="orders",
        schema=Schema(name="public"),
        database=Database.POSTGRES,
        columns=[
            PostgresColumn(name="id", data_type=PostgresType.BIGINT, primary_key=True, nullable=False),
            PostgresColumn(name="created_at", data_type=PostgresType.TIMESTAMPTZ, nullable=False),
            PostgresColumn(name="email", data_type=PostgresType.TEXT, nullable=True, sensitive=True),
        ],
        write_mode=WriteMode.APPEND,
        partitioned_by="created_at",
    ),
    source=Source(name="raw.orders"),
    bounds=Bounds(begin=datetime(2024, 1, 1, tzinfo=timezone.utc)),
    batching=Batch(default="0 * * * *"),
    debug=True,
)
```

### Model parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Unique identifier for the model |
| `target` | `Target` | required | Defines where and how data is written |
| `source` | `Source` | `None` | Defines where data is read from |
| `bounds` | `Bounds` | `None` | Optional backfill begin/end bounds |
| `batching` | `Batch` | `None` | Controls batch size and retries |
| `tagging` | `Tags` | `None` | Controls tag auto-assembly |
| `enabled` | `bool` | `True` | Whether the model is active |
| `debug` | `bool` | `False` | Pretty-prints the model at construction time |
| `description` | `str` | `None` | Human-readable description |
| `**kwargs` | | | Extra metadata. Callable values are resolved with non-callable kwargs as arguments |

### Target parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Destination table name |
| `schema` | `Schema` | `Schema()` | Destination schema |
| `database` | `Database` | `None` | Target database. Required if `columns` is set |
| `columns` | `list[PostgresColumn]` | `[]` | Column definitions. Required if `database` is set |
| `model_type` | `ModelType` | `TABLE` | `TABLE` or `VIEW` |
| `write_mode` | `WriteMode` | `APPEND` | How to write data |
| `partitioned_by` | `str` | `None` | Column to partition by. Must exist in `columns` |
| `dsn_env_var` | `str` | `None` | DSN env var for the target connection |

### Source parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Source table or entity name |
| `schema` | `str` | `None` | Source schema |
| `dsn_env_var` | `str` | `None` | DSN env var for the source connection |
| `query` | `str` | `None` | Optional query override |

### Bounds parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `begin` | `datetime` | `None` | Backfill start — must be UTC-aware |
| `end` | `datetime` | `None` | Backfill end — must be UTC-aware |

### Batch parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `default` | `CronBatch` | `"0 0 * * *"` | Chunk size as a cron batch expression |
| `lookback` | `int` | `None` | Extends interval start backwards by N cron-ticks |
| `retries` | `int` | `None` | Retry count on failure |

### Computed attributes

| Attribute | Description |
|---|---|
| `target.sensitive` | `True` if any column has `sensitive=True` |
| `target.unique_columns` | Columns with `unique=True` — required for `UPSERT_NO_DELETE` |
| `target.partitioned_by_index` | `True` if `partitioned_by` is set |
| `tags` | Auto-assembled from `name`, `target.schema.name`, and `"all"` |

## Debug

When `debug=True`, the model is pretty-printed at construction time. You can also call it manually at any point:

```python
model.pretty()
```

## Write modes

Read more [here](MODES.md)

```python
from bollhav.model.write_modes import WriteMode

WriteMode.APPEND
WriteMode.RECREATE_PARTITION     # requires partitioned_by
WriteMode.RECREATE_TABLE_INSERT
WriteMode.TRUNCATE_TABLE_INSERT
WriteMode.UPSERT_NO_DELETE          # requires at least one column with unique=True
WriteMode.VIEW                   # requires ModelType.VIEW
```

## Tag filtering

Tags are automatically assembled at init time. By default `name`, `schema`, and `"all"` are added.

```python
model = Model(name="orders", target=Target(name="orders", schema=Schema(name="public")))
model.tags  # {"orders", "public", "all"}
```

Control which tags are auto-added via `Tags`:

```python
from bollhav.model.tags import Tags

Model(..., tagging=Tags(name_add_to_tags=False, schema_add_to_tags=False, model_gets_all_tag=False))
```