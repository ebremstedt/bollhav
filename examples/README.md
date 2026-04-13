# bollhav examples

A self-contained pipeline that mocks both reading and writing, showing the full bollhav pattern — `@with_pipe_config`, `match_models`, `@progress_bar` — without any database connections.

## Structure

```
examples/
  models/
    products.py       # WriteMode.APPEND
    customers.py      # WriteMode.TRUNCATE_TABLE_INSERT
    orders.py         # WriteMode.RECREATE_TABLE_INSERT
  main.py             # entry point — @with_pipe_config
  execute.py          # batch handler — @progress_bar
  mock_read.py        # returns fake polars DataFrames
  mock_write.py       # prints instead of writing to a database
```

## Setup

Install bollhav and polars (no database drivers needed):

```bash
pip install bollhav polars
```

## Running

Latest mode:
```bash
TAGS="[all]" USE_SCHEMA_SUFFIX=false LATEST_ENABLED=true python examples/main.py
```

Backfill mode:
```bash
TAGS="[all]" USE_SCHEMA_SUFFIX=false BACKFILL_ENABLED=true BACKFILL_SINCE=2024-01-01T00:00:00Z BACKFILL_UNTIL=2024-01-11T00:00:00Z python examples/main.py
```

## Write modes

The three example models each demonstrate a different write mode. The mode is printed next to each batch output so you can see which strategy applies.

| Model       | Write mode                 | Behaviour                                         |
|-------------|----------------------------|---------------------------------------------------|
| `products`  | `APPEND`                   | `cosmic_raw` — rows added every batch, nothing removed          |
| `customers` | `TRUNCATE_TABLE_INSERT`    | `cosmic_clean` — table wiped then reloaded each batch           |
| `orders`    | `RECREATE_TABLE_INSERT`    | `cosmic_5583` — table dropped, recreated, then loaded each batch|

Other write modes not shown here (require additional model config):

- `UPSERT_NO_DELETE` — requires columns with `unique=True`
- `RECREATE_PARTITION` — requires `partitioned_by` and `unique=True` columns
- `VIEW` — requires `ModelType.VIEW` and `source.query`

See [WRITEMODES.md](../bollhav/docs/WRITEMODES.md) for guidance on choosing a mode.
