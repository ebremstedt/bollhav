[← home](index.md)

# State

Per-model progress, tracked in a per-model state table. Opt in with `state=State(...)`; bollhav records each unit of work's lifecycle and re-runs become resumable. A unit is one window for an `interval` model, or the single whole-table / view existence row for a `monolithic` / `view` model — so every [kind](UPSTREAM.md) carries state, not just intervals.

## Status values

The `status` column on `<state_schema>.<target_name>_state` is one of:

| Status | Meaning |
|---|---|
| `pending` | Queued to run. The user's loop iterates these. |
| `running` | Currently being processed. Set by `@state` immediately before invoking your `execute`. Visible in live dashboards. |
| `applied` | Completed successfully. Set by `@state` after a clean execute, or atomically by the staging flush. |
| `blocked` | Cannot run: an out-of-pipeline upstream isn't fulfilled. See `blocked_reason` for the [BLOCK CODE](BLOCK_CODES.md). |
| `error` | Execute raised. Full details (type, message, traceback) are in the sibling `_errors` table. Auto-retried on next run under `STATE_MODE=discover`. |

## Re-evaluation on rerun

Under `STATE_MODE=discover` (the default), `prefill` keeps `applied` rows untouched and re-evaluates everything else against the current upstream state. So:

- `pending` rows whose upstreams have since regressed → `blocked`
- `blocked` rows whose upstreams now satisfy → `pending`
- `error` rows → `pending` (automatic retry)
- `running` rows orphaned by a process crash → `pending` (automatic recovery)

Under `STATE_MODE=bulldozer`, every row resets to the computed status regardless of prior value (`applied_at` cleared too).

## Concurrency: per-interval advisory locks

`@state` takes a **per-interval** Postgres advisory lock keyed by `(model.full_name, since, until)` for every interval it processes. Two workers running on the same model but different intervals don't conflict; two workers racing on the same interval — only one wins, the other silently skips that interval and moves on.

This means you can scale horizontally on the same model:

```bash
# Terminal 1
TAGS=[orders] python main.py

# Terminal 2 — same model, splits the work
TAGS=[orders] python main.py
```

Both bootstraps see the same pending rows; both loops try the same intervals; the lock ensures each interval is processed exactly once. No special code in your loop — the decorator handles it.

### Optional model-wide lock

The per-interval lock is the right default — two workers safely parallelize across different intervals of the same model. For stricter **"one whole-pipeline run at a time per model"** semantics (rare — usually only when interval ordering matters or your loop has cross-interval side effects), set `State(exclusive_run=True)` and wrap with `model_lock`:

```python
from bollhav.model import Model, State, model_lock, ModelLockedError

# In the model:
Model(state=State(exclusive_run=True), ...)

# In the loop:
for model in models:
    try:
        with model_lock(model):
            for interval in model.intervals:
                execute(model=model, since=interval.since, until=interval.until)
    except ModelLockedError:
        logger.warning("%s is locked; skipping", model.target.full_name)
        continue
```

`model_lock` is a **no-op** when `state is None` or `exclusive_run is False` (the default) — safe to wrap any model preventively; the flag on `State` is what actually decides. When `exclusive_run=True`, it takes a Postgres advisory lock keyed by the model's `full_name` and holds it for the entire loop, preventing any parallel processing on that model.

## Errors

When an execute raises, `@state` does three things atomically:

1. Insert a row into `<state_schema>.<target_name>_errors` with `full_name`, `error_type`, `error_message`, `traceback`, `created_at`.
2. Flip the state row's `status` to `error`.
3. Re-raise the original exception so the caller sees it.

The errors table keeps full history across runs — joinable with the state table on `(since, until)` for per-interval inspection or on `run_id` for per-invocation lookups. The `full_name` column lets you `UNION ALL` across every model's errors table for a global view.

The one exception: if the staging flush already set state to `applied` (data is in target) and post-stage user code raises, we log the error but **do not** downgrade state to `error`. The write succeeded; the post-write code didn't.

## Upstreams

A model's `upstream` is a list of **contracts** on other models, each checked before a unit of work runs. An unsatisfied contract → `blocked`. See [Upstream & contracts](UPSTREAM.md) for the full picture.

### Library and state colocation

The library lives in `z_bollhav.model_library` in the **state DB**. View and `library=True` models without their own state-DSN fall back to `target.dsn_env_var` — which is fine for single-DB setups (the common case where state and target share one Postgres database). If you split state to a separate database, library-only models also need their target DSN to point at that same instance, since they have no `state.dsn_env_var` to redirect them.

## Disabling state entirely: `STATE_DISABLED`

Set `STATE_DISABLED=true` to force a pipeline to run with no state tracking, even when models declare `state=State(...)`. Useful for:

- Ad-hoc/dev runs against a fresh DB where the state tables don't exist
- Quickly running the write path in isolation
- Bypassing the state DB when you only want to test the read/write logic

When set: `@load_models` clears `state` and `target.staging` on every matched model, the state bootstrap and banner are skipped, `@state` becomes a passthrough, and `write()` uses the direct (non-staged) path. State tables aren't read from or written to during the run.

## Env vars (state-related)

| Variable | Default | Effect |
|---|---|---|
| `STATE_MODE` | `discover` | `discover` preserves `applied` rows on re-evaluation and adds new pending intervals as discovered; `bulldozer` resets every row to the freshly-computed status |
| `STATE_DISABLED` | `false` | When `true`, force no-state behavior on every matched model |
| `PEEK` | `false` | When `true`, run bootstrap + print state banner, then exit without invoking `main()` |
