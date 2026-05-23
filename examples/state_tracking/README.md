# state_tracking

Demonstrates per-model state tracking with two decorators:

- `@load_models` — pre-fills per-interval `pending` rows in the state table
- `@state_tracker` — gates execute on applied rows, marks success, logs errors

## What gets created

For a model `warehouse_clean.orders`:

| Object | Notes |
|---|---|
| `z_warehouse_clean.orders_state` | one row per `(since, until)`, status `pending` or `applied` |
| `z_warehouse_clean.orders_errors` | one row per exception, joinable by `run_id` |

The `z_` prefix appears because no separate state DSN was set — state
lives in the target DB. Configure `State(dsn_env_var="STATE_DSN")` and
set `STATE_DSN` to a separate Postgres to drop the `z_` prefix.

## Run

```bash
export TARGET_DSN=postgresql://localhost:5432/warehouse
export TAGS=orders
export USE_SCHEMA_SUFFIX=false
export BACKFILL_ENABLED=true

# First run — populates state, runs all intervals.
python main.py

# Second run — every interval is applied, all skipped.
python main.py

# Force-rerun under disrespect.
STATE_MODE=disrespect python main.py
```

## Env vars

| Var | Default | Notes |
|---|---|---|
| `STATE_MODE` | `respect` | `respect` skips applied rows; `disrespect` resets all to pending |
