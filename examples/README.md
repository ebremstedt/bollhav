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

Export the environment variables, then run:

```bash
# required
export TAGS="[all]"
export USE_SCHEMA_SUFFIX=false

# pick one: latest or backfill (not both)

# latest — resolves the most recent complete interval from the model's
#           batch expression (or LATEST_BATCH_EXPRESSION override)
export LATEST_ENABLED=true
# export LATEST_BATCH_EXPRESSION="0 * * * *"  # optional override

# backfill — explicit time window
# export BACKFILL_ENABLED=true
# export BACKFILL_SINCE=2024-01-01T00:00:00Z
# export BACKFILL_UNTIL=2024-01-11T00:00:00Z
# export BACKFILL_BATCH_EXPRESSION="0 0 * * *"  # optional

# optional
export SCHEMA_SUFFIX=dev       # appended to target schema, e.g. cosmic_raw → cosmic_raw_dev
export DEBUG=false
# export TIMEZONE_OVERRIDE=Europe/Stockholm  # overrides model timezone for all models
```

```bash
cd examples
python main.py
```

**Common `TAGS` values:**

| Expression            | Selects                         |
|-----------------------|---------------------------------|
| `[all]`               | every model                     |
| `[customers]`         | only `customers`                |
| `[products \| orders]`| `products` or `orders`          |

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
