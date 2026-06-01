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

## Upstreams: views and library-only tables

A downstream model with `state=State(...)` declares its upstreams by full name. Bollhav's library tracks every registered model and answers the satisfaction question per upstream:

| Upstream kind | How it registers | What "satisfied" means |
|---|---|---|
| **TABLE with `state=State(...)`** | Automatic, on every run | An `applied` row in the upstream's state table matches or fully encapsulates the downstream's `(since, until)` window |
| **VIEW with `library=True`** | Opt-in via `Model(..., library=True)`. A view declared without it is still a valid bollhav model (gets `CREATE OR REPLACE VIEW`d each run) but isn't claimable as upstream. | The library row exists. Views are time-agnostic — once they're declared, every interval of the downstream is satisfied by their presence |
| **TABLE with `library=True`** | Opt-in via `Model(..., library=True)`. Useful for static lookup tables or externally-loaded tables that don't track state themselves but need to be claimable as upstream | The library row exists. Same presence-based rule as views |

### View example

```python
from bollhav.model import Model, Target, TargetSchema, ModelType, WriteMode

# Opt in with `library=True` so downstreams can claim this view.
v_orders_summary = Model(
    target=Target(
        name="v_orders_summary",
        schema=TargetSchema(name="warehouse"),
        model_type=ModelType.VIEW,
        write_mode=WriteMode.VIEW,
        dsn_env_var="TARGET_DSN",
    ),
    library=True,
)

# Downstream depends on the view; every interval is satisfied
# once the view-model has run once and registered.
enriched = Model(
    target=Target(name="enriched", ...),
    upstream=["warehouse.v_orders_summary"],
    state=State(),
    batching=Batch(...),
)
```

### `library=True` example

```python
# Static lookup table — written outside bollhav, has no state,
# but downstreams need to claim it as an upstream.
countries = Model(
    target=Target(
        name="countries",
        schema=TargetSchema(name="lookup"),
        dsn_env_var="TARGET_DSN",
    ),
    library=True,   # ← register-only opt-in
)
```

Once `countries` has appeared in any pipeline run, downstreams referencing `lookup.countries` are satisfied by its mere presence — they don't wait for an applied row that will never come.

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
