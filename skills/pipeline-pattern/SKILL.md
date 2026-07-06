---
name: pipeline-pattern
description: The recommended structure for a bollhav pipeline — a single src/main.py that holds all three decorated functions (main with @load_models, run_model with @model_lifecycle, run_interval with @execute_lifecycle), one Model per file under src/models/, and read/transform/write split into their own modules called from the execute stage. Use when a developer is starting a new bollhav project, laying out files, or asking how the pieces are wired together.
---

# bollhav — recommended pipeline structure

A bollhav pipeline is one entry file that wires the three decorators, your
models (one per file), and your read/transform/write logic (each in its own
file).

## Layout

```
your-pipeline/
└── src/
    ├── main.py           # entry — ALL THREE decorated functions live here
    ├── read.py           # pull source data → DataFrame(s)
    ├── transform.py      # shape/clean the DataFrame
    ├── write.py          # persist (delegates to bollhav's writer)
    └── models/
        ├── orders.py     # one Model per file
        ├── customers.py
        └── ...
```

Run it with `python src/main.py` plus the env block — see the `env-vars`
skill.

## The three decorators — all in `src/main.py`

The three functions wrap three call levels (all-models → one-model →
one-interval). Keep them together in `main.py`; each decorator does the heavy
lifting (discovery, state, locking, asset DDL, contracts) and calls your body.

```python
# src/main.py
import os
import psycopg

from bollhav.model import (
    ModelRun, load_models, model_lifecycle, execute_lifecycle,
)
from read import read
from transform import transform
from write import write


@load_models
def main(runs: list[ModelRun], debug: bool) -> None:
    # @load_models: read env, apply overrides, match models by TAGS=, resolve
    # each run's window, hand them back topologically sorted (producers first).
    dsn = ...  # your connection string
    with psycopg.connect(dsn, autocommit=True) as conn:
        for run in runs:
            run_model(run, conn)


@model_lifecycle
def run_model(run: ModelRun, conn) -> None:
    # @model_lifecycle: build the target asset (table, or CREATE VIEW), ensure
    # state, prefill, narrow run.intervals to what still needs doing.
    for interval in run.intervals:
        run_interval(run, interval, conn)


@execute_lifecycle
def run_interval(run: ModelRun, interval, conn) -> None:
    # @execute_lifecycle: gate on applied → lock → check upstream contracts →
    # run this body → mark applied. This is where read → transform → write goes.
    if run.model.is_view:
        return                      # a view has nothing to write
    df = read(run, interval)        # interval is a TZInterval, or None (timeless)
    df = transform(df)
    write(conn, run, df)


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))  # so sibling imports resolve
    main()
```

`interval` is a time window for a batched model, or `None` for a timeless
model / view (their unit of work is the whole thing).

## read / transform / write — their own files

- **`read.py`** — pull the source data for this interval. For a relational
  input, build the SQL with `run.model.ref("schema.table")` so it's
  suffix-aware; for an API/file, fetch it here. Return a DataFrame (or a
  generator of them).
- **`transform.py`** — pure shaping: rename, cast, derive, filter. No I/O.
- **`write.py`** — persist. Thin wrapper over bollhav's writer, which honors
  the model's `write_mode` and `staging`:

  ```python
  # src/write.py
  from bollhav.postgres import write as pg_write   # or bollhav.mssql

  def write(conn, run, df):
      pg_write(conn=conn, run=run, df_gen=iter([df]))
  ```

## Models — one per file in `src/models/`

Each file defines one `Model` — declarative only, no I/O. See the `guide`
skill to design one and the `tags` skill for how they're selected.

```python
# src/models/orders.py
from datetime import datetime, timezone
from bollhav.model import (
    Batch, Contract, Database, Model, Staging, State,
    Tags, Target, TimeChunking, Temporality, WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType

orders = Model(
    target=Target(
        name="orders", schema="warehouse", catalog="demo",
        database=Database.POSTGRES, write_mode=WriteMode.APPEND,
        dsn_env_var="TARGET_DSN", staging=Staging(),
        columns=[
            PostgresColumn(name="id", data_type=PostgresType.BIGINT, nullable=False),
            PostgresColumn(name="total", data_type=PostgresType.NUMERIC, nullable=False),
        ],
    ),
    temporality=Temporality.TEMPORAL,
    state=State(),                                  # required for staging
    batching=Batch(time=TimeChunking(chunk="@daily")),
    contract=Contract(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    tagging=Tags(tags={"demo"}),
)
```

Working end-to-end versions live in `examples/staging_state_contracts/` and
the other `examples/*` folders — read those for a complete, runnable shape.
