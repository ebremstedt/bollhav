[← home](index.md)

# Model

A `Model` is a pure data object describing what data looks like and where it goes. It does not contain execution logic — that lives in a separate `execute` function that receives the model as a parameter.

The `Model` itself is the top-level container. Its sub-objects each have their own page:

- [Target](TARGET.md) — where data lands (table, schema, columns, write mode)
- [TargetSchema](TARGETSCHEMA.md) — schema part of Target (with optional rotating suffix)
- [SourceTable](SOURCETABLE.md) — where data is read from (database)
- [SourceFile](SOURCEFILE.md) — where data is read from (file)
- [Bounds](BOUNDS.md) — historical envelope for backfill mode
- [Batch](BATCH.md) — chunk size, lookback, retries
- [Tagging](TAGGING.md) — controls which auto-derived tags get added to the model

## Example

```python
from datetime import datetime, timezone
from bollhav.model import (
    Model, Target, TargetSchema, SourceTable,
    Bounds, Batch, Database, WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType

model = Model(
    target=Target(
        name="orders",
        schema=TargetSchema(name="public"),
        database=Database.POSTGRES,
        columns=[
            PostgresColumn(name="id", data_type=PostgresType.BIGINT, primary_key=True, nullable=False),
            PostgresColumn(name="created_at", data_type=PostgresType.TIMESTAMPTZ, nullable=False, partition_on=True),
            PostgresColumn(name="email", data_type=PostgresType.TEXT, nullable=True, sensitive=True),
        ],
        write_mode=WriteMode.APPEND,
    ),
    source=SourceTable(name="raw.orders"),
    bounds=Bounds(begin=datetime(2024, 1, 1, tzinfo=timezone.utc)),
    batching=Batch(interval_expression="0 * * * *"),
    debug=True,
)
```

## Parameters

### target

Type: `Target` · Default: required

Defines where and how data is written. See [Target](TARGET.md).

### source

Type: `SourceTable` · Default: `None`

Defines where data is read from. See [SourceTable](SOURCETABLE.md) or [SourceFile](SOURCEFILE.md).

### bounds

Type: `Bounds` · Default: `None`

Optional backfill begin/end bounds. See [Bounds](BOUNDS.md).

### batching

Type: `Batch` · Default: `None`

Controls chunk size and retries. When `None`, the model runs as a single unit — see [Chunking](CHUNKING.md). See [Batch](BATCH.md) for the field reference.

### tagging

Type: `Tags` · Default: `None`

Controls tag auto-assembly. See [Tagging](TAGGING.md) for the config object; see [Tags](TAGS.md) for the expression syntax used to select models at runtime.

### enabled

Type: `bool` · Default: `True`

Whether the model is active.

### debug

Type: `bool` · Default: `False`

Pretty-prints the model at construction time. See [Debug](#debug) below.

### description

Type: `str` · Default: `None`

Human-readable description.

### upstream

Type: `list[str]` · Default: `None`

Names of models that must run before this one. See [Upstream dependencies](#upstream-dependencies) below.

### `**kwargs`

Extra metadata. Callable values are resolved with non-callable kwargs as arguments.

## Computed attributes

| Attribute | Description |
|---|---|
| `target.sensitive` | `True` if any column has `sensitive=True` |
| `target.unique_columns` | Columns with `unique=True` — required for `UPSERT_NO_DELETE` |
| `target.partitioned_by_index` | `True` if `partitioned_by` is set |
| `tags` | Auto-assembled from `name`, `target.schema.name`, and `"all"` |

## Upstream dependencies

Models can declare dependencies on other models using the `upstream` parameter. When `apply_runtime_overrides` (or `match_models` directly) returns results, they are topologically sorted so that upstream models always appear before their dependents.

```python
raw_orders = Model(
    target=Target(name="raw_orders"),
)

enriched_orders = Model(
    target=Target(name="enriched_orders"),
    upstream=["raw_orders"],
)
```

If a matched model depends on an upstream model that is not in the matched set, matching raises a `ValueError`. This ensures you never accidentally run a model without its dependencies.

Circular dependencies are also detected and raise a `ValueError`.

### Upstream mode

The `upstream_mode` parameter controls how upstream dependencies are enforced. It can be set via the `UPSTREAM` environment variable (read by `@load_models`) or passed directly to `match_models` / `apply_runtime_overrides`.

| Mode | Value | Description |
|---|---|---|
| `ENFORCE` | `enforce` | All upstream dependencies must be present and are ordered (default) |
| `IGNORE_VIEWS` | `ignore_views` | Views skip upstream checks; tables are still enforced |
| `IGNORE_COMPLETELY` | `ignore_completely` | No ordering or validation, models returned as-is |

```bash
export UPSTREAM=ignore_views
```

```python
from bollhav.model import match_models, UpstreamMode

match_models(folder="src/models", tags="[all]", upstream_mode=UpstreamMode.IGNORE_VIEWS)
# or, more commonly, just `@load_models` and let it read UPSTREAM from env.
```

Given these models:

```mermaid
graph LR
    A[raw_orders<br/>TABLE] --> B[enriched_orders<br/>TABLE]
    A --> V[orders_view<br/>VIEW]
    B --> V
```

Each mode behaves differently:

```mermaid
graph TD
    subgraph "ENFORCE (default)"
        direction LR
        E1[raw_orders] --> E2[enriched_orders]
        E1 --> E3[orders_view]
        E2 --> E3
        E4[All upstream must<br/>be in matched set]
    end

    subgraph "IGNORE_VIEWS"
        direction LR
        IV1[raw_orders] --> IV2[enriched_orders]
        IV3[orders_view runs<br/>without requiring<br/>upstream in matched set]
    end

    subgraph "IGNORE_COMPLETELY"
        direction LR
        IC1[raw_orders]
        IC2[enriched_orders]
        IC3[orders_view]
        IC4[No ordering.<br/>Returned as discovered]
    end
```

## Debug

When `debug=True`, the model is pretty-printed at construction time — every field is dumped to stdout in a readable layout so you can see exactly how bollhav resolved the configuration: the target (catalog/schema/name, columns, write mode), source, bounds, batching (interval/window expressions, size, lookback, retries), tags (both user-supplied and auto-assembled), upstream dependencies, and directives. Use this when you're not sure what bollhav inferred from your `Model(...)` call, or when a runtime override or tag-driven mutation has reshaped the model and you want to see the final state.

You can also call it manually at any point:

```python
model.pretty()
```

## Write modes

Read more [here](MODES.md)

```python
from bollhav.model.write_modes import WriteMode

WriteMode.APPEND
WriteMode.RECREATE_PARTITION     # requires partitioned_by
WriteMode.UPSERT_NO_DELETE       # requires at least one column with unique=True
WriteMode.VIEW                   # requires ModelType.VIEW
```

For full-reload semantics, combine a write mode with `Target(recreate_table=True)` or `Target(truncate_table=True)` — these run once before the chunked write loop (see [MODES.md](MODES.md#pre-load-flags-recreate_table-and-truncate_table)).
