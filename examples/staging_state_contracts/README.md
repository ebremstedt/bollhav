# staging + state + upstream contracts

A minimal Postgres pipeline that creates **a view, a monolith, and an
interval table**, then a fourth table that declares **upstream contracts**
on all three.

The four models (`src/models/`):

| model           | kind        | state | staging | upstream contracts |
|-----------------|-------------|:-----:|:-------:|--------------------|
| `orders`        | interval    | ✓     | ✓       | —                  |
| `customers`     | view        | ✓     | —       | —                  |
| `app_config`    | monolith    | ✓     | —       | —                  |
| `daily_summary` | interval    | ✓     | ✓       | **interval + view + monolith** |

`daily_summary` depends on:

```python
upstream=[
    IntervalContract("warehouse.orders"),       # an applied interval covering the window
    ViewContract("warehouse.customers"),         # the view exists
    MonolithicContract("warehouse.app_config"),  # the whole table is loaded
]
```

Each contract is checked per unit of work by `@execute_lifecycle`. If any
is unsatisfied the interval is `blocked` (the reason names every missing
upstream); when all are satisfied it runs.

## How it's wired

```
main.py          @load_models      main(models)               # discovery: match by TAGS
run_model.py       @model_lifecycle   run_model(model, conn)        # assets + state bootstrap
run_interval.py      @execute_lifecycle  run_interval(model, unit, conn)  # gate/lock/contracts/mark + staging
mock_read.py         read(model, unit)                              # tiny per-model mock data
src/models/*.py    the four Model definitions
```

`run_model` loops `model.intervals`, which is time windows for batched
models or a single `None` for the view / monolith. The staging table's
lifecycle (create → write → merge → drop) lives in `@execute_lifecycle`;
`run_interval` just `read()`s and `write()`s. The view is created by
`@model_lifecycle` (`CREATE OR REPLACE VIEW`), so its execute body is empty.

## Run it

Needs a running Postgres (you supply the DSN — no Docker here).

```bash
export TARGET_DSN='postgresql://postgres:postgres@localhost:5432/postgres'
export TAGS='[demo]'                  # tag expression uses [group] syntax
export USE_SCHEMA_SUFFIX=false        # keep schema = "warehouse" so the contract names match
export BACKFILL_SINCE='2024-01-01T00:00:00+00:00'   # the window the interval models backfill
export BACKFILL_UNTIL='2024-01-04T00:00:00+00:00'

python main.py                        # add DEBUG=true for the full state:/stage: trail
```

First run: `app_config` (1 whole-table load) → `orders` (3 intervals) →
`customers` (view created) → `daily_summary` (3 intervals; all three
contracts satisfied, so it runs). Second run: everything is already
`applied`, so every model's `model.intervals` is empty and the loop does
no work.

### Inspect

```sql
-- the data
SELECT count(*) FROM warehouse.orders;
SELECT * FROM warehouse.customers;        -- the view
SELECT * FROM warehouse.app_config;
SELECT * FROM warehouse.daily_summary;

-- the state rows (one per interval for orders/daily_summary;
-- one NULL-window row for the view / monolith)
SELECT since, until, status, kind FROM z_warehouse.orders_state ORDER BY since;
SELECT since, until, status, kind FROM z_warehouse.customers_state;
SELECT since, until, status, kind FROM z_warehouse.app_config_state;

-- the cross-pipeline library (every registered model + its kind)
SELECT full_name, kind, upstream FROM z_bollhav.model_library ORDER BY full_name;
```

### See a contract block it

Reset the monolith's state and run only `daily_summary`'s tag — its
`MonolithicContract` is now unsatisfied, so its intervals go `blocked`
with a reason naming `warehouse.app_config`:

```sql
UPDATE z_warehouse.app_config_state SET status = 'pending';
```
