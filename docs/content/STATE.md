[Home](index.md) › [Model](MODEL.md) › **State**

# State

Per-model progress, tracked in a per-model state table. Opt in with `state=State(...)`; bollhav records each unit of work's lifecycle and re-runs become resumable. A unit is one window for a batched [temporal](TEMPORALITY.md) model, or the single one-shot row for a [timeless](TEMPORALITY.md) model (or an unbatched temporal one) — so every temporality carries state, not just windowed models.

## Status values

Each model's state lives in its own digest-named table in the one central bollhav schema — `z_bollhav` (prod) or `z_bollhav_<suffix>` (a suffixed run) — alongside the shared `library` and `errors` tables. The table name is a pure function of the model's canonical identity, so it never collides with the fixed `library` / `errors` names. The `status` column on it is one of:

| Status | Meaning |
|---|---|
| `pending` | Queued to run. The user's loop iterates these. |
| `running` | Currently being processed. Set by `@state` immediately before invoking your `execute`. Visible in live dashboards. |
| `applied` | Completed successfully. Set by `@state` after a clean execute, or atomically by the staging flush. |
| `blocked` | Cannot run: an out-of-pipeline upstream isn't fulfilled. See `blocked_reason` for the [BLOCK CODE](#block-codes). |
| `error` | Execute raised. Full details (type, message, traceback) are in the shared central `errors` table (`z_bollhav.errors`), keyed by `full_name`. Auto-retried on next run under `STATE_MODE=discover`. |

## Re-evaluation on rerun

Under `STATE_MODE=discover` (the default), `prefill` keeps `applied` rows untouched and re-evaluates everything else against the current upstream state. So:

- `pending` rows whose upstreams have since regressed → `blocked`
- `blocked` rows whose upstreams now satisfy → `pending`
- `error` rows → `pending` (automatic retry)
- `running` rows orphaned by a process crash → `pending` (automatic recovery)

Under `STATE_MODE=bulldozer`, every *existing* row resets to the computed status regardless of prior value (`applied_at` cleared too) — but the rows keep their `(since, until)` boundaries, so the chunk granularity is unchanged.

Under `STATE_MODE=nuke`, every state row for the model is **deleted** first, then `prefill` rediscovers intervals from scratch at the *current* chunk. Use it to **change chunk granularity** (e.g. hourly → monthly — `bulldozer` can't, since it only resets existing boundaries) or to wipe a stale backlog. It's destructive (applied history is lost, so the next run reprocesses everything — safe for MERGE / recreate writes, but `APPEND` would duplicate) and never fires during `DRY_STATE`. It does **not** touch the model's data, and it's scoped to the `TAGS`-matched models.

## Concurrency: per-interval advisory locks

`@state` takes a **per-interval** Postgres advisory lock — keyed by a hash of the `(model.full_name, since, until)` triple — for every interval it processes. Two workers running on the same model but different intervals don't conflict; two workers racing on the same interval — only one wins, the other silently skips that interval and moves on.

This means you can scale horizontally on the same model:

```bash
# Terminal 1
TAGS=[orders] python main.py

# Terminal 2 — same model, splits the work
TAGS=[orders] python main.py
```

Both bootstraps see the same pending rows; both loops try the same intervals; the lock ensures each interval is processed exactly once. No special code in your loop — the decorator handles it.

### Optional model-wide lock

The per-interval lock is the right default — two workers safely parallelize across different intervals of the same model. For stricter **"one whole-pipeline run at a time per model"** semantics (rare — usually only when interval ordering matters or your loop has cross-interval side effects), set `State(allow_concurrent_runs=False)`:

```python
from bollhav.model import Model, State

Model(state=State(allow_concurrent_runs=False), ...)
```

You don't take the lock yourself — `@model_lifecycle` does it for you. When `allow_concurrent_runs=False`, it takes a Postgres advisory lock keyed by the model's `full_name` at the start of the model's run and holds it (released in a `finally`) until the run ends, preventing any parallel processing on that model. If another run already holds it, the hook raises `ModelLockedError` — catch it around your `run_model(...)` call to skip:

```python
from bollhav.model import ModelLockedError

for run in runs:
    try:
        run_model(run, conn)          # @model_lifecycle takes the lock
    except ModelLockedError:
        logger.warning("%s is locked; skipping", run.model.target.full_name)
        continue
```

With the default `allow_concurrent_runs=True`, no model-wide lock is taken and only the per-interval locks apply.

## Errors

When an execute raises, `@state` does three things atomically:

1. Insert a row into the shared central errors table (`z_bollhav.errors`, or `z_bollhav_<suffix>.errors`) with `full_name`, `run_id`, `error_type`, `error_message`, `traceback`, `created_at`.
2. Flip the state row's `status` to `error`.
3. Re-raise the original exception so the caller sees it.

The errors table keeps full history across runs for every model — joinable with a model's state table on `(since, until)` for per-interval inspection or on `run_id` for per-invocation lookups. Because it's a single shared table, a global view is just `SELECT … FROM z_bollhav.errors` filtered (or grouped) by `full_name` — no `UNION` needed.

The one exception: if the staging flush already set state to `applied` (data is in target) and post-stage user code raises, we log the error but **do not** downgrade state to `error`. The write succeeded; the post-write code didn't.

## Upstreams

A model's `upstream` is a list of **contracts** on other models, each checked before a unit of work runs. An unsatisfied contract → `blocked`. Contracts are only enforced here, in the state machine — so `upstream` requires state (declaring it without `State(...)` raises). See [Upstream](UPSTREAM.md) for the full picture.

### Library and state colocation

The library lives in `z_bollhav.library` in the **state DB**. View and `library=True` models without their own state-DSN fall back to `target.dsn_env_var` — which is fine for single-DB setups (the common case where state and target share one Postgres database). If you split state to a separate database, library-only models also need their target DSN to point at that same instance, since they have no `state.dsn_env_var` to redirect them.

## Disabling state entirely: `STATE_DISABLED`

Set `STATE_DISABLED=true` to force a pipeline to run with no state tracking, even when models declare `state=State(...)`. Useful for:

- Ad-hoc/dev runs against a fresh DB where the state tables don't exist
- Quickly running the write path in isolation
- Bypassing the state DB when you only want to test the read/write logic

When set: `@load_models` clears `state` and `target.staging` on every matched model, the state bootstrap and banner are skipped, `@state` becomes a passthrough, and `write()` uses the direct (non-staged) path. State tables aren't read from or written to during the run.

## Recording an out-of-band load: `STATE_MARK_APPLIED`

`STATE_MARK_APPLIED=true` is the **complement of `STATE_DISABLED`**: it writes the **state** without running the **data**. It stamps the matched models' window intervals `applied` and exits — no read, no write, no MERGE, no target DDL.

The use case is exactly the pair: you loaded a table some other way (a `STATE_DISABLED` bulk load, a manual script, a one-off `INSERT`) and now want the state machine to *know* those intervals are done, so the daily incremental doesn't re-load them.

Where the four combinations sit:

| | state | data |
|---|---|---|
| normal run | ✅ written | ✅ run |
| `DRY_STATE` | ✅ prefilled / classified | ❌ |
| `STATE_DISABLED` | ❌ | ✅ run |
| **`STATE_MARK_APPLIED`** | ✅ **stamped applied** | ❌ |

**Scope.** It marks exactly the run's **`compute_intervals(run)`** — the `[BACKFILL_SINCE, BACKFILL_UNTIL)` (or `LATEST`) window, split by your chunk — and **nothing else**. Crucially it does *not* go through the normal "drain every actionable row" executor, so a leftover `pending` backlog from other intervals is **left untouched**. Match the chunk (`INTERVAL_OVERRIDE`) to how the data was actually loaded, since that's the grain of the rows it stamps.

```sh
# you loaded 2025-11-06 out of band; now record it as done:
export TAGS="[FactCaseVariableDevice]"
export BACKFILL_ENABLED=true
export BACKFILL_SINCE=2025-11-06T00:00:00
export BACKFILL_UNTIL=2025-11-07T00:00:00
export INTERVAL_OVERRIDE=@daily        # the grain it was loaded at
export STATE_MARK_APPLIED=true
python src/main.py
```

**It is an assertion, not a verification.** bollhav does *not* check the data is actually there — it records that you say it is. If you're wrong, downstreams gate on a claim that isn't true. `applied_at` is set to *now* (the claim time, not when the data really landed), so a downstream `Freshness` check sees "just loaded." It logs a `WARNING` per model and shows a `mark applied` banner in the run summary, since it's a deliberate override.

## Env vars (state-related)

| Variable | Default | Effect |
|---|---|---|
| `STATE_MODE` | `discover` | `discover` preserves `applied` rows on re-evaluation and adds new pending intervals as discovered; `bulldozer` resets every existing row to the freshly-computed status (boundaries kept); `nuke` deletes every row then re-prefills at the current chunk (for changing chunk granularity / wiping a backlog — destructive) |
| `STATE_DISABLED` | `false` | When `true`, force no-state behavior on every matched model (data without state) |
| `STATE_MARK_APPLIED` | `false` | When `true`, stamp the matched window's intervals `applied` without running them (state without data) — to record an out-of-band load. Scoped to `compute_intervals(run)`, never the backlog. An assertion, not a verification |
| `DRY_STATE` | `false` | When `true`, run the state bootstrap and print each model's resolved plan (would-run / applied / blocked), then exit without creating assets, writing data, or running model logic |

## Block codes

When bollhav inserts a state row as `blocked` instead of `pending`, the
`blocked_reason` column always starts with a stable code like
`STATE_001:`. Codes are namespaced by domain (`STATE`, future: `WRITE`,
`LIB`, etc.), permanent once assigned, and never renumbered — so you
can grep logs, build runbooks, and key alerts off them.

This page explains each code: what triggers it, what to do about it.

### STATE_001 — upstream not registered

**Trigger.** A staged, state-enabled model's `upstream` declares a model
that has never been seen by `@load_models`. The library
(`z_bollhav.library`) has no row for it, so bollhav can't reason
about whether the upstream has produced data for the requested
`(since, until)` window.

**Example reason text.**

```
STATE_001: upstream 'warehouse.orders' not registered
```

**Why this is conservative.** "Not registered" means *nobody has ever
run this model with `@load_models` against this state DB.* It's
distinct from "registered but not yet applied" (that's `STATE_002`) —
a missing registration is a louder signal.

**Remediation.**

- Run the upstream model in any pipeline that matches it. Once
  `@load_models` bootstraps it, the library row exists; the
  downstream's next bootstrap re-evaluates and flips the row from
  `blocked` to `pending` (under `STATE_MODE=discover`).
- Double-check the spelling of `upstream` in the downstream's
  `Model(...)` definition — the string must match the upstream's
  `Target.full_name` exactly (`<schema>.<name>`).
- If the upstream lives in a different state DB, see the note on
  cross-DB state in [STATE.md](#state). Today the library only
  resolves models in the same state DB.

### STATE_002 — upstream has no applied row covering this interval

**Trigger.** The upstream IS in the library, but its state table has
no row with `status='applied'` whose `(since, until)` either exactly
matches or fully encapsulates the downstream's interval.

**Example reason text.**

```
STATE_002: upstream 'warehouse.orders' has no applied row covering 2024-01-01T00:00:00+00:00 → 2024-01-02T00:00:00+00:00
```

**Encapsulation, not exact match.** A daily upstream applied row
(`2024-01-01 → 2024-01-02`) satisfies an hourly downstream's interval
(`2024-01-01T03:00 → 2024-01-01T04:00`) because the upstream window
fully covers the downstream window. This lets upstream and downstream
run at different cadences without coordination.

**Remediation.**

- Run the upstream over the missing window. Then re-run the
  downstream pipeline; blocked rows are re-evaluated under
  `STATE_MODE=discover` and flip to pending once their upstream
  satisfies.
- Check the upstream's state — it may have `pending` or `blocked`
  rows of its own for that window. The block propagates: a downstream
  can't unblock until the upstream is `applied`.
- If you intentionally want to run without the upstream, set
  `STATE_MODE=bulldozer` to reset the blocked rows back to pending
  on the downstream's next run. This bypasses the safety check; use
  with eyes open.

### Adding new codes

When a new block condition lands:

1. Add a value to `BlockCode` in [bollhav/model/state.py](../../bollhav/model/state.py).
2. Pick the next free number in the appropriate domain (`STATE_NNN`,
   or a new domain like `WRITE_NNN`). Never reuse a number.
3. Document the code on this page with the same shape: trigger,
   example, remediation.
4. Add a test pinning the `BlockCode.<NAME>.value` to its string so a
   refactor can't silently move it.
