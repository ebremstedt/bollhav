[← home](index.md)

# State

Per-model interval state. Opt in by setting `state=State(...)` on a model; bollhav records every interval's lifecycle in a per-model state table, and re-runs become resumable.

## Status values

The `status` column on `<state_schema>.<target_name>_state` is one of:

| Status | Meaning |
|---|---|
| `pending` | Queued to run. The user's loop iterates these. |
| `running` | Currently being processed. Set by `@state` immediately before invoking your `execute`. Visible in live dashboards. |
| `applied` | Completed successfully. Set by `@state` after a clean execute, or atomically by the staging flush. |
| `blocked` | Cannot run: an out-of-pipeline upstream isn't fulfilled. See `blocked_reason` for the [BLOCK CODE](BLOCK_CODES.md). |
| `error` | Execute raised. Full details (type, message, traceback) are in the sibling `_errors` table. Auto-retried on next run under `STATE_MODE=respect`. |

## Re-evaluation on rerun

Under `STATE_MODE=respect` (the default), `prefill` keeps `applied` rows untouched and re-evaluates everything else against the current upstream state. So:

- `pending` rows whose upstreams have since regressed → `blocked`
- `blocked` rows whose upstreams now satisfy → `pending`
- `error` rows → `pending` (automatic retry)
- `running` rows orphaned by a process crash → `pending` (automatic recovery)

Under `STATE_MODE=disrespect`, every row resets to the computed status regardless of prior value (`applied_at` cleared too).

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

If you want stricter "one whole-pipeline run at a time per model" semantics, `model_lock` is still available:

```python
from bollhav.model import model_lock, ModelLockedError

for model in models:
    try:
        with model_lock(model):
            for interval in model.intervals:
                execute(model=model, since=interval.since, until=interval.until)
    except ModelLockedError:
        logger.warning("%s is locked; skipping", model.target.full_name)
        continue
```

This takes one advisory lock keyed by the model's `full_name` and holds it for the entire loop. Useful when you want to prevent any parallel processing on a model, not just same-interval collisions.

## Errors

When an execute raises, `@state` does three things atomically:

1. Insert a row into `<state_schema>.<target_name>_errors` with `full_name`, `error_type`, `error_message`, `traceback`, `created_at`.
2. Flip the state row's `status` to `error`.
3. Re-raise the original exception so the caller sees it.

The errors table keeps full history across runs — joinable with the state table on `(since, until)` for per-interval inspection or on `run_id` for per-invocation lookups. The `full_name` column lets you `UNION ALL` across every model's errors table for a global view.

The one exception: if the staging flush already set state to `applied` (data is in target) and post-stage user code raises, we log the error but **do not** downgrade state to `error`. The write succeeded; the post-write code didn't.

## Disabling state entirely: `STATE_DISABLED`

Set `STATE_DISABLED=true` to force a pipeline to run with no state tracking, even when models declare `state=State(...)`. Useful for:

- Ad-hoc/dev runs against a fresh DB where the state tables don't exist
- Quickly running the write path in isolation
- Bypassing the state DB when you only want to test the read/write logic

When set: `@load_models` clears `state` and `target.staging` on every matched model, the state bootstrap and banner are skipped, `@state` becomes a passthrough, and `write()` uses the direct (non-staged) path. State tables aren't read from or written to during the run.

## Env vars (state-related)

| Variable | Default | Effect |
|---|---|---|
| `STATE_MODE` | `respect` | `respect` preserves `applied` rows on re-evaluation; `disrespect` resets every row to the new computed status |
| `STATE_DISABLED` | `false` | When `true`, force no-state behavior on every matched model |
| `PEEK` | `false` | When `true`, run bootstrap + print state banner, then exit without invoking `main()` |
