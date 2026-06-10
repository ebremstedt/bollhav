# direct_write — interval models written without staging

The counterpart to [`staging_state_contracts`](../staging_state_contracts):
two state-tracked INTERVAL models whose rows go **straight to the target** —
no per-interval staging table.

```
main.py          @load_models       main(runs)                    # discovery: match by TAGS
run_model.py       @model_lifecycle   run_model(run, conns)        # assets + state bootstrap
run_interval.py      @execute_lifecycle  run_interval(run, ival, …)  # gate / lock / mark + write()
```

`write()` picks the path from the model's target:

| model     | write_mode          | what happens per interval                              |
|-----------|---------------------|--------------------------------------------------------|
| `events`  | `APPEND`            | COPY the rows straight into the target table           |
| `metrics` | `RECREATE_PARTITION`| `DELETE` the interval's window, then COPY fresh rows    |

Neither sets `staging=…`, so there's **no staging table** — the write commits
directly. `run_interval` passes the interval's `since`/`until` to `write()`,
which `RECREATE_PARTITION` uses as the DELETE bounds and `APPEND` ignores.

### state without staging

Both models still set `state=State()`, so the lifecycle tracks one row per
interval and **gates reruns** — run it twice and the second pass does nothing
(every interval is already `applied`). That shows `state` and `staging` are
independent: you get an idempotent, gated pipeline with no staging at all.

The trade-off vs. staging: a crash mid-write can leave partial rows in the
target (staging is what makes the write atomic). `RECREATE_PARTITION` softens
this — rerunning a window replaces it — which is why it pairs well with the
direct path.

### run it

Needs a running Postgres (you supply the DSN — no Docker here).

```bash
export TARGET_DSN='postgresql://postgres:postgres@localhost:5432/postgres'
export TAGS='[demo]'
export USE_SCHEMA_SUFFIX=false
export BACKFILL_SINCE='2024-01-01T00:00:00+00:00'
export BACKFILL_UNTIL='2024-01-04T00:00:00+00:00'
export PROGRESS_BAR=execute          # per-interval bar; or =model / =minimal

python main.py                       # add DEBUG=true for the full write/state trail
```

Fresh start (drop the target schema **and** the state/library schema):

```bash
python -c "import os,psycopg; c=psycopg.connect(os.environ['TARGET_DSN'],autocommit=True); c.execute('DROP SCHEMA IF EXISTS warehouse CASCADE'); c.execute('DROP SCHEMA IF EXISTS z_bollhav CASCADE')"
```
