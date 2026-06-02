# Lifecycle decorators — end-to-end shape

Shows the new `@model_lifecycle` / `@interval_lifecycle` hooks with
connections passed as **function parameters** (created in `main()`,
threaded through), per the [lifecycle redesign](../../docs/content/LIFECYCLE_REDESIGN.md).

```python
@model_lifecycle
def execute_model(model, data_conn, state_conn=None):
    for interval in model.intervals:
        execute_interval(model, interval, data_conn, state_conn)

@interval_lifecycle
def execute_interval(model, interval, data_conn, state_conn=None):
    read(); write(conn=data_conn, ...)

def main():
    target_conn = psycopg.connect(os.environ["TARGET_DSN"])
    state_conn  = target_conn          # co-located; separate conn → cross-DB state
    for model in [orders]:
        execute_model(model, target_conn, state_conn)
```

What the hooks do for you:

- **`@model_lifecycle`** — runs target DDL (on `data_conn`); when the model
  has `State()`, ensures the state table, registers in the library,
  prefills, and filters `model.intervals` to the actionable ones (on
  `state_conn`); takes the exclusive model lock if `State(exclusive_run=True)`.
- **`@interval_lifecycle`** — gates on applied, takes the per-interval
  advisory lock, marks the row running, runs your work, then marks it
  applied (or records the failure) — all state writes on `state_conn`.

`data_conn` is required; `state_conn` is optional and defaults to
`data_conn` (co-located state). State tracking happens only when the
model has `State()`.

**Open connections with `autocommit=True`.** The non-atomic data→state
model commits each step on its own (data write, then state flip). On a
non-autocommit connection the work opens a dangling transaction that
never commits and is rolled back on `close()` — so nothing persists.
Staging's internal `with conn.transaction()` still gives per-interval
atomicity on top of an autocommit connection.

## Run

Point `TARGET_DSN` at any Postgres you have:

```bash
export TARGET_DSN='postgresql://postgres:postgres@localhost:5432/postgres'
pip install -e ../.. polars psycopg   # if not already installed

python pipeline.py            # friendly per-interval progress + summary
DEBUG=1 python pipeline.py    # + the framework's internal log lines
```

No local Postgres? Spin one up with the bundled compose file instead:

```bash
docker compose up -d --wait          # --wait blocks until Postgres is ready
export TARGET_DSN='postgresql://bollhav:pw@localhost:5433/bollhav'
python pipeline.py
docker compose down -v
```

The example uses its own isolated schema (`lifecycle_demo` / state in
`z_lifecycle_demo`) so it won't collide with other examples' data. Drop
it with `DROP SCHEMA lifecycle_demo, z_lifecycle_demo CASCADE;` when done.

Each run processes the 3 daily intervals (writes rows, flips state to
`applied`). The example defaults to `STATE_MODE=bulldozer`, which resets
every interval to `pending` each run — so you always see the full
process-3-intervals output (row count stays 9; the write is an idempotent
UPSERT).

To see **resume** instead, run with `STATE_MODE=discover`: the first run
processes all 3, and a second run prints **"0 interval(s) — everything
already applied"** and writes nothing — the applied-gate + idempotency in
action.

`DEBUG=1` surfaces every framework step (schema/table created, prefilled,
marked running, `Writing N rows`, marked applied).

> **Status:** this demonstrates the new lifecycle path. `@load_models` is
> not used here yet — once its bootstrap is removed (it then becomes pure
> discovery), you can put `@load_models` back on `main` and drop the
> manual model list. The older examples still use the previous `@state`
> path until they're migrated.
