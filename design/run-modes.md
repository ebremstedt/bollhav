# Run modes redesign — window source × state mode

Status: **implemented** (2026-06-25). The core is built and tested (suite green). The
design conversation below is preserved; what landed and what's still deferred is in
"Implementation status" near the bottom.

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

- **explicit** — `RUN_SINCE` / `RUN_UNTIL`, operator-supplied
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
| **torch** | **optional** | wipe *all* state; no window → run the whole contract; a window runs that slice now, the rest defers to a later discover run |

Notes:
- **bulldozer needs a window** (nothing to bulldoze otherwise) → no-window is an error.
- **discover no-window** does *not* prefill (no window to seed), so on **empty state** it's
  a no-op — correct, nothing outstanding. Seed the table with a windowed run first; after
  that, no-window discover reconciles it. This is the answer to the original ask:
  "run with no dates, let state find the work" = `discover` + no window.
- **torch + a window is allowed** (revised — see below). The wipe is *always total*
  (`torch_rows` deletes every row); the window only scopes what runs *now*. With no window
  a torch runs the whole contract (a clean full reload); with a window it runs that slice
  now and leaves the rest `pending` for a later `discover` run to drain.

  *History:* this was originally **forbidden** (a windowed torch was said to orphan the
  rest). That reasoning assumed *window-scoped prefill* — back then a windowed torch reseeded
  only the window. Once prefill changed to always **refill the whole contract** (see "Prefill
  vs invalidation"), the orphan case vanished: a windowed torch refills the whole contract
  `pending`, runs the window, and the remainder is simply outstanding, not lost. So
  `TorchWithWindowError` was removed and torch now resolves its window like the other modes.
  Use it for "re-load the whole model, prioritise this slice, defer the rest"; for a
  *surgical* "re-do only this slice later", the state-edit reset API (no wipe) is cleaner.

## Prefill vs invalidation — state mirrors the contract

A later refinement, and an important one: **prefill is decoupled from the run mode.**
Every run first *fills the state table with the contract* — one row per interval the
contract declares (`begin` → `end`, or the latest tick for an open contract), inserting
only the rows not already present (`ON CONFLICT (since, until) DO NOTHING`). This is mode-
and window-independent and incremental: the table is a complete, honest mirror of the
contract, and as the forward edge advances the new row just appears (prior runs laid down
the rest).

The mode then decides only **invalidation** — what to do with the window's *existing* rows,
layered on top of that constant prefill:

| mode | invalidation step |
|---|---|
| **bulldozer** | reset *the run window's* rows to `pending` (`reset_window`) |
| **discover** | nothing — run only what's still outstanding |
| **torch** | delete every row first (`torch_rows`); the prefill then refills all-`pending` |

So the modes share one prefill and differ in a single step — and a bulldozer over one day
never disturbs the rest of the contract (prefill touched the whole range, but the reset is
window-scoped). The pieces:

- `window.contract_intervals(run)` — the intervals the contract declares (vs
  `compute_intervals`, the run's window). Falls back to the run window when there's no
  `contract.begin`.
- `StateTable.prefill(...)` — insert-missing-preserve, mode-independent.
- `StateTable.reset_window(...)` — bulldozer's window-scoped reset.

Consequence to know: the *first* run on a fresh model with a long contract materializes the
entire declared range as `pending` (then runs only its window). The backlog is real and
visible from run one; drain it with a backfill or a no-window discover reconcile.

## Defaults

- bare `python main.py` (no overrides) → **bulldozer + latest** = run the latest complete
  tick (the cron default).
- `RUN_SINCE` / `RUN_UNTIL` → bulldozer + explicit range.
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

## Implementation status

**Built (suite green, 682 passed):**

1. ✅ **Window-scoped execution** — `get_actionable_intervals(window=…)` filters rows to
   `[since, until)` when a window is given; `window=None` keeps the "drain everything"
   reconcile path. Threaded `window=run.window` at the lifecycle bootstrap + e2e harness.
2. ✅ **Window-source collapse** — `RUN_UNTIL` is optional
   (`?? contract.end ?? latest tick`), so a no-dates backfill runs the contract range.
   `BackfillRequiresUntilError` removed.
3. ✅ **Defaults flipped** — `latest` is the default run mode (`_resolve_latest` defaults
   to `not backfill`; `_resolve_backfill_enabled` defaults False); `STATE_MODE` defaults
   to `bulldozer` (`_resolve_state_mode`).
4. ✅ **torch window** — a bare torch (no dates) reloads the whole contract range; a torch
   with a `RUN_SINCE/UNTIL` window runs *that slice* now (the wipe stays total, the
   remainder defers to discover). Implemented in `runtime._apply_to_model`. *(Superseded the
   earlier `TorchWithWindowError`, now removed — see "torch + a window is allowed" above.)*
5. ✅ **Tests** — `TestRunModeWindowMatrix` in `test_runtime.py` (the full mode × window
   resolution + torch guard), plus e2e `get_actionable_is_window_scoped` /
   `bulldozer_reruns_the_applied_window` / `discover_skips_the_applied_window`.
6. ✅ **Prefill decoupled from mode** — prefill fills the table with the contract
   (`StateTable.prefill` + `window.contract_intervals`), mode-independent and incremental;
   invalidation is a separate step (`reset_window` for bulldozer, `torch_rows` for torch).
   Tests: `prefill_fills_the_whole_contract_not_just_the_window` /
   `bulldozer_window_spares_the_rest_of_the_contract` (e2e) + `TestContractIntervals`
   (unit). See "Prefill vs invalidation" above.

**Deferred / not done:**

- **Retiring `LATEST_ENABLED` / `BACKFILL_ENABLED` flags + the tag-driven `reload`.** The
  flags still exist (latest is just their default now); `reload` (tag `[r:…]`) still works
  and is redundant with backfill-no-dates. Full removal of the old flag names is a
  follow-up cleanup.
- **The no-prefill "drain only existing rows" discover variant.** Today `discover` +
  contract-range reconciles the declared range (prefills it); the lighter "only run rows
  already in state, don't materialize new ones" path is not built.
- **Flexible-model mode mapping** — `bulldozer`/`torch` as *uncover the window / uncover
  all* (range subtraction) rather than status flips, for `fixed_intervals=False` models.
  See `flexible-intervals.md` + the `state.write.uncover_span` primitive.
- **Doc sweep** — STATE.md / UPSTREAM.md updated for the new default + window-scoping;
  BATCH.md not yet revisited.
