[Home](index.md) › **Examples** › **Decorator example**

# The decorator example

Each bollhav pipeline is the same **three nested loops**, each wrapped by one
**lifecycle decorator**. The decorators carry all the bookkeeping — discovery,
ordering, target assets, state, locking, upstream contracts, progress — so your
own functions stay tiny: read + transform + write.



```python
# main.py
from bollhav.model import ModelRun, load_models
from execute import run_model


@load_models(folder="src/models")
def main(runs: list[ModelRun], debug: bool) -> None:
    dsn = env_var_dsn("TARGET_DSN").connection_string
    with (
        psycopg.connect(dsn, autocommit=True) as data_conn,
        psycopg.connect(dsn, autocommit=True) as state_conn,
    ):
        for run in runs:
            run_model(run, data_conn, state_conn)


# execute.py
from bollhav.model import ModelRun, execute_lifecycle, model_lifecycle
from bollhav.postgres import write
from transform import transform
from read import read


@model_lifecycle
def run_model(run: ModelRun, data_conn, state_conn=None):
    for interval in run.intervals:
        execute(run, interval, data_conn, state_conn)


@execute_lifecycle
def execute(run: ModelRun, interval, data_conn, state_conn=None):
    df_gen = read(run, interval) 
    df_gen = transform(df_gen)
    write(conn=data_conn, run=run, df_gen=df_gen)
```

## `@load_models` — discovery + matching + preparing

Decorate your entry point and bollhav **discovers** the `Model`s ➜ **matches** them by
`TAGS` ➜ resolves the **run window** ➜ hands them back **topologically sorted**
(producers → consumers).

## `@model_lifecycle` — one model's run

`@model_lifecycle` runs the per-model setup *before* `run_model`: build the
target table, and — when stateful — ensure the state tables, register in the
library, prefill, and narrow `run.intervals` down to the still-actionable units.
Your body just loops them and hands each to `execute`.

## `@execute_lifecycle` — one unit of work

`@execute_lifecycle` brackets each unit: gate on `applied` → take the lock →
check the model's **upstream contracts** → mark `running` → run your body → mark
`applied` (or record the failure). For staging models it also creates the
staging table and merges it into the target around your body. So `execute` is
just read + write.

`interval` is a time window for a batched model, or `None` for a oneshot (its
unit of work is the whole thing). If any upstream contract is unsatisfied the
unit is left `blocked` — the reason names every missing upstream — and runs on a
later pass once they're satisfied.

See [Decorators](DECORATORS.md) for the full reference.