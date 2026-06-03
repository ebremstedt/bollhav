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
def main(models, debug):
    for model in models:        # already ordered by upstream
        dsn = os.environ[model.target.dsn_env_var]
        with psycopg.connect(dsn, autocommit=True) as conn:
            run_model(model, conn)   # @model_lifecycle does setup + gating
```

```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres   # state + target
export TARGET_DSN='postgresql://postgres:postgres@localhost:5432/postgres'
export TAGS='[all]'
python main.py        # every model, in order, gated by contracts
```

[examples/staging_state_contracts/](https://github.com/ebremstedt/bollhav/tree/main/examples/staging_state_contracts) is exactly this: a view, a monolith, an interval table, and a fourth model with contracts on all three — one `python main.py` against any local Postgres.

## Bake upstreams into the config

Declaring `upstream=[IntervalContract("warehouse.orders"), ...]` with `state=State(...)` makes ordering and gating **data, not orchestration code**. So locally there's nothing to wire: the contract that blocks a model in prod blocks it identically on your laptop. You test the real dependency behavior, not a mock of it — run `python main.py` twice and the second run does nothing (everything `applied`), same as prod.

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
| `PEEK=true` | bootstrap + print the state banner, then exit — see what *would* run |
| `DEBUG=true` | pretty-print every fully-resolved model |
| `STATE_DISABLED=true` | run the write path with no state DB (quick smoke test on a fresh DB) |
| `STATE_MODE=bulldozer` | reset every state row and recompute from scratch |

Inspect state directly between runs:

```sql
SELECT since, until, status FROM z_warehouse.daily_summary_state ORDER BY since;
```

## See also

- [Orchestration](ORCHESTRATION.md) — the same state-vs-no-state fork, at the production level.
- [State](STATE.md) · [Upstream](UPSTREAM.md) · [@load_models](LOAD_MODELS.md) · [Env](ENV.md)
