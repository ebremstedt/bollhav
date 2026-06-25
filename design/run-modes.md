# Run modes redesign — window source × state mode

Status: **design, not built.** Captures a design conversation; no code committed for it
yet. (One small in-progress edit exists in `bollhav/model/window.py` — see "Current
state" at the bottom — that predates and must be reconciled with this note.)

## The problem

Today bollhav tangles two things and leaves a third unscoped:

1. **Window modes** (`LATEST_ENABLED` / `BACKFILL_ENABLED`, plus a tag-driven `reload`)
   and **`STATE_MODE`** (`discover` / `bulldozer` / `torch`) are presented as if they're
   the same kind of choice. They aren't.
2. **`latest` vs `backfill` is an artificial split** — both just resolve a `[since, until]`
   window from different sources.
3. **Execution is unscoped from the window.** `get_actionable_intervals` is
   `WHERE status <> 'applied'` with *no window filter*, so the run window only controls
   what gets *prefilled*, not what *runs*. A "latest" run silently drains the **entire
   backlog** (every non-applied row in history) → "runs far more than the user imagined."
   The window lies about scope. This is the core wart.
4. The default `STATE_MODE` is `discover` — which is exactly the *clever, unbounded* one,
   so it's a dangerous default.

## The model: two orthogonal axes

**Axis 1 — window source** (where `[since, until]` comes from). `explicit` is just an
*override* of the `contract` default:

```
since = explicit_since ?? contract.begin
until = explicit_until ?? contract.end ?? latest_complete_tick
```

- **explicit** — `BACKFILL_SINCE` / `BACKFILL_UNTIL`, operator-supplied
- **contract** — the model's `Contract(begin, end)` declared range
- **latest** — the latest complete tick (the clock)

`latest` / `backfill` / `reload` all collapse into this: they're one mechanism with
different bound-sources (`reload` = "backfill whose bounds come from the contract").

**Axis 2 — state mode** (invalidation): `bulldozer` (default) / `discover` / `torch`.

**The key change: execution is window-scoped.** A run executes exactly its window's
intervals (per mode), not the whole non-applied backlog. The window becomes an honest
bound. `get_actionable_intervals` must filter by the run window (the *only* exception is
no-window `discover` — see below).

These two axes are also orthogonal to `fixed_intervals` (grid vs coverage) — see
`flexible-intervals.md`. For flexible models, `bulldozer`/`torch` map to *uncover the
window* / *uncover all* (range subtraction) rather than status flips.

## Mode × window — the symmetry

The window requirement *is* the spec for each mode:

| mode | window | meaning |
|---|---|---|
| **bulldozer** (default) | **required** | reset & run *exactly* this window; leave everything else (old blocked/pending) untouched — it's not the run's job to drain history |
| **discover** | **optional** | window given → run the window's non-applied (skip applied); **no window → reconcile**: drain *all* outstanding in state (the deliberate "no dates, let state decide" catch-up) |
| **torch** | **forbidden** | always the contract range — wipe all + reseed contract = clean reload |

Notes:
- **bulldozer needs a window** (nothing to bulldoze otherwise) → no-window is an error.
- **discover no-window** does *not* prefill (no window to seed), so on **empty state** it's
  a no-op — correct, nothing outstanding. Seed the grid with a windowed run first; after
  that, no-window discover reconciles it. This is the answer to the original ask:
  "run with no dates, let state find the work" = `discover` + no window.
- **torch + a window is forbidden**: a windowed torch *orphans* history (wipe all, reseed
  only the window → the rest is forgotten), and "redo a specific window" is already what
  bulldozer does *safely*. So torch has exactly one coherent meaning: clean reload of the
  contract range. Raise loudly on torch + window: "torch reloads the contract range; use
  bulldozer to redo a window."

## Defaults

- bare `python main.py` (no overrides) → **bulldozer + latest** = run the latest complete
  tick (the cron default).
- `BACKFILL_SINCE` / `BACKFILL_UNTIL` → bulldozer + explicit range.
- `STATE_MODE=discover` + no window → reconcile state (the no-dates case).
- `STATE_MODE=torch` → clean reload of the contract.

## Why bulldozer is the default (not discover)

- With window-scoped execution, bulldozer = "do exactly this window, fresh, leave the rest"
  = **bounded, predictable**. A cron's latest tick is one interval, run once; a backfill
  range runs that range; nothing sneaks in.
- `discover` is the *clever* one (skip-applied + drain-backlog) — that's where the
  surprise lives, so it's **opt-in**.
- For an advancing cron tick, bulldozer == discover anyway (the tick is always new).
  bulldozer only differs on an *overlapping/applied* window — where you want the
  predictable "run what I named."
- "Leave things unrun/blocked" is the **feature** (bounded), not a bug.

## Decided

- **Default window-source = `latest`.** A bare run (no overrides) is `bulldozer + latest`
  → run the latest complete tick (the cron default). No "must choose a window" error.

## Open questions

- **torch over an open contract**: `contract.begin .. contract.end`, or
  `contract.begin .. latest_complete_tick` when `end` is open? (Leaning: latter — same as
  reload resolution.)
- **Does `explicit` survive?** Its only *unique* job is chunked **initial** backfill on
  empty state (the state-edit API — `state.write.reset_*` — already covers surgical re-runs
  of *existing* state). Keep for progressive historical loading, or drop and push
  everything through `contract` + state-edit?
- **discover no-window scope**: drains the model's own state table (per-run = one model),
  not cross-model. Confirm.

## Build order (NOT built)

1. **Window-scoped execution** — `get_actionable_intervals(window=…)` filters to
   `[since, until]` when a window is given; the no-window `discover` path keeps today's
   unscoped "drain all" behavior. This is the foundational change.
2. **Collapse window modes** — one window-source resolution
   (`explicit ?? contract ?? latest`); make `BACKFILL_UNTIL` optional
   (`?? contract.end ?? latest`). Replace/retire `LATEST_ENABLED` / `BACKFILL_ENABLED` and
   the tag-driven `reload` in favour of the unified source.
3. **Default `STATE_MODE` → bulldozer**; bulldozer scopes invalidation + execution to the
   window.
4. **Mode × window validation** — bulldozer requires a window; torch forbids one (uses the
   contract); discover optional.
5. **Docs + tests** — STATE.md, UPSTREAM.md ("two axes"), BATCH.md, the `load_models` env
   table; update/replace the mode tests.

## Current state of the tree (reconcile before continuing)

`bollhav/model/window.py` has a small **uncommitted, half-finished** edit from an earlier
aborted attempt at the "backfill `until` optional" piece: the `resolve_window` backfill
branch was changed to `window_until = until or contract.end or latest_complete_interval(...)`,
the `BackfillRequiresUntilError` import was dropped, and the docstring was updated — but the
error class itself was **not** removed and the two tests that assert it
(`test_model.py`, `test_batch.py`) were **not** updated, so the suite is red on that file.
Decide whether to revert that edit and redo it as part of step 2 above, or finish it.
Nothing else was implemented.
