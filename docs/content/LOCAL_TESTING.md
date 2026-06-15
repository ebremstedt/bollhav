[Home](index.md) › [Articles](ARTICLES.md) › **Testing locally**

# Testing pipelines locally

Run an entire pipeline — every model, in dependency order — on your machine with `python main.py` and a local Postgres. No Airflow, no separate orchestration app to stand up. Reach for this when developing or testing a multi-model pipeline end-to-end, or backfilling a window.

## Why end-to-end is actually easy here

The target is **Postgres**, which is one `docker run` away — so "end to end" means the *real* thing: your read/transform, the real write modes and staging, and the real state machine, all against an actual database. You're not mocking the warehouse or pointing at a shared cloud instance you can't reset.

```bash
docker run -d --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres   # the whole backend
```

That one container is **both the target and the state DB** (state co-locates with the target by default), so a single throwaway Postgres reproduces production behavior on your laptop — run it, inspect it, `docker rm` it, start clean. Contrast a cloud-warehouse stack, where there's no local equivalent to run the full pipeline against.

## The whole DAG travels with the models

Because the dependency graph is [upstream contracts](UPSTREAM.md) baked into the model config (not an external DAG), it ships with your code. `@load_models` matches by `TAGS`, topologically orders the matched set, and your loop runs them — a downstream stays `blocked` until its upstream's window is `applied`. One process reproduces the full pipeline locally, exactly as it behaves in prod.

```python
# main.py — the whole pipeline, locally
@load_models
def main(runs, debug):
    for run in runs:            # already ordered by upstream
        dsn = os.environ[run.model.target.dsn_env_var]
        with psycopg.connect(dsn, autocommit=True) as conn:
            run_model(run, conn)   # @model_lifecycle does setup + gating
```

```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres   # state + target
export TARGET_DSN='postgresql://postgres:postgres@localhost:5432/postgres'
export TAGS='[all]'
python main.py        # every model, in order, gated by contracts
```

[examples/staging_state_contracts/](https://github.com/ebremstedt/bollhav/tree/main/examples/staging_state_contracts) is exactly this: a view, a whole-table (timeless) model, a temporal table, and a fourth model with contracts on all three — one `python main.py` against any local Postgres.

## Bake upstreams into the config

Declaring `upstream=[Source("warehouse.orders", type=SourceModel(), contract=UpstreamContract.WINDOW), ...]` with `state=State(...)` makes ordering and gating **data, not orchestration code**. So locally there's nothing to wire: the contract that blocks a model in prod blocks it identically on your laptop. You test the real dependency behavior, not a mock of it — run `python main.py` twice and the second run does nothing (everything `applied`), same as prod.

## Backfill with separate processes

Per-interval advisory locks make the same command safe to run many times at once — each process pulls *different* intervals, and the lock guarantees each interval runs exactly once (the loser skips). So to backfill faster, just launch more processes. No coordination code.

```bash
export BACKFILL_SINCE='2024-01-01T00:00:00+00:00'
export BACKFILL_UNTIL='2024-04-01T00:00:00+00:00'
export TAGS='[orders]'

python main.py &      # three workers, same command —
python main.py &      # they split the windows between them
python main.py &
```

All three see the same `pending` rows, race per interval, and converge. Scale up or down by adding or killing processes — your laptop, or one box, becomes the backfill cluster. See [State → concurrency](STATE.md#concurrency-per-interval-advisory-locks).

## A fast inner loop

| Env var | Effect |
|---|---|
| `DRY_RUN=true` | print the matched models (cron, interval count) and exit — no DB needed |
| `DRY_STATE=true` | bootstrap state + print each model's plan (would-run / applied / blocked), then exit — see what *would* run against state, without executing |
| `DEBUG=true` | pretty-print every fully-resolved model |
| `STATE_DISABLED=true` | run the write path with no state DB (quick smoke test on a fresh DB) |
| `STATE_MODE=bulldozer` | reset every state row and recompute from scratch |

Inspect state directly between runs. State tables are digest-named and live in the central `z_bollhav` schema (`z_bollhav_<suffix>` for a suffixed run), so find a model's table via the `library` registry, then read it:

```sql
-- which table holds a model's state?
SELECT state_schema, state_table FROM z_bollhav.library
WHERE full_name = 'demo.warehouse.daily_summary';

-- then read it (substitute the name from above)
SELECT since, until, status FROM z_bollhav.<state_table> ORDER BY since;

-- errors are one shared table, keyed by full_name
SELECT full_name, error_type, error_message, created_at FROM z_bollhav.errors
ORDER BY created_at DESC;
```

## Tearing a test environment down

A suffixed run isolates all its data + state under a branch namespace, so you can reset cleanly without touching prod:

- **`clear_state()`** — drop one model's state table and delete its `library` / `errors` rows, leaving the env schema and the model's data intact. The next run rebuilds state from scratch and re-discovers every interval. Refuses on an unsuffixed (prod) model.
- **`drop_environment(conn, models)`** — full teardown of a suffixed environment: each model's data schema (`warehouse_<suffix>`), its staging schema, and the shared `z_bollhav_<suffix>` state/library/errors schema. Drops **data too**. Refuses unless the models carry a schema suffix, so it can't target prod.

```python
from bollhav.postgres.state import PostgresState, drop_environment

# reset just one model's state (keeps its data)
PostgresState(model, conn).clear_state()

# nuke the whole suffixed environment (data + state)
drop_environment(conn, [run.model for run in runs])
```

Both require `SCHEMA_SUFFIX` to be set — by design, clearing prod state is not offered.

## See also

- [Orchestration](ORCHESTRATION.md) — the same state-vs-no-state fork, at the production level.
- [State](STATE.md) · [Upstream](UPSTREAM.md) · [@load_models](DECORATORS.md#load_models) · [Env](ENV.md)
