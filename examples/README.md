# bollhav examples

Each folder below is a self-contained runnable pipeline. The first one
shows the full end-to-end pattern. The rest isolate one feature at a
time so you can see how that feature behaves on its own.

| Folder | Demonstrates |
|---|---|
| [`company_xyz_pipeline/`](company_xyz_pipeline/) | End-to-end: `@load_models`, progress bar, all WriteModes |
| [`tag_matching/`](tag_matching/) | Tag expressions and how name/schema auto-contribute tags (snake_case + PascalCase) |
| [`latest_vs_backfill/`](latest_vs_backfill/) | Same model, different modes — latest pulls the last complete chunk, backfill uses an explicit window |
| [`latest_window_vs_batch/`](latest_window_vs_batch/) | `window_expression` + `interval_expression` together — big-warehouse scenario (daily scope, 15-minute writes) |
| [`reload_flag/`](reload_flag/) | `r:` tag prefix — reprocess a model's full declared bounds, overriding latest/backfill |
| [`lookback_and_retries/`](lookback_and_retries/) | `Batch(lookback=N, retries=N)` — extending intervals for late data and wiring up a retry loop |

## `company_xyz_pipeline/`

A self-contained pipeline that mocks both reading and writing, showing the full bollhav pattern — `@load_models`, `@progress_bar` — without any database connections.

### Structure

```
company_xyz_pipeline/
  src/
    models/
      raw.py            # warehouse_raw — source tables
      clean.py          # warehouse_clean — cleaned / derived tables
      products.py       # WriteMode.APPEND
      customers.py      # WriteMode.APPEND + truncate_table=True
      orders.py         # WriteMode.APPEND + recreate_table=True
      views.py          # WriteMode.VIEW — CREATE OR ALTER VIEW
  main.py               # entry point — @load_models
  execute.py            # batch handler — @progress_bar
  mock_read.py          # returns fake polars DataFrames
  mock_write.py         # prints instead of writing to a database
```

### Setup

Install bollhav and polars (no database drivers needed):

```bash
pip install bollhav polars
```

### Running

Run from the repo root:

Latest mode:
```bash
TAGS="[all]" USE_SCHEMA_SUFFIX=false LATEST_ENABLED=true python examples/company_xyz_pipeline/main.py
```

Backfill mode:
```bash
TAGS="[all]" USE_SCHEMA_SUFFIX=false PROGRESS_BAR=batch BACKFILL_ENABLED=true BACKFILL_SINCE=2024-01-01T00:00:00Z BACKFILL_UNTIL=2024-01-11T00:00:00Z python examples/company_xyz_pipeline/main.py
```

Just the views:
```bash
TAGS="[views]" USE_SCHEMA_SUFFIX=false UPSTREAM=ignore_views LATEST_ENABLED=true python examples/company_xyz_pipeline/main.py
```
