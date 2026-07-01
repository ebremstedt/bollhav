# Investigation findings: DimDeviceConnection contract pinned at 2026-06-01

**Symptom (state GUI):** `DimDeviceConnection` shows
`100% backfilled · fully backfilled · contract 2024-11-04 → 2026-06-01`,
even though the model is meant to roll its upper bound forward to the last
complete day. The upper bound is stuck at the month boundary 2026-06-01.

**Root cause (confirmed):** the contract end is *not declared* — it is inferred
from the data as `max(until)` of the state rows. A backfill run with a **monthly**
interval wrote applied coverage whose last interval ends exactly on the month
boundary `2026-06-01`, and nothing has materialized any rows past it, so the
inferred horizon snaps there and everything below it reads as 100% covered.

---

## 1. What DimDeviceConnection declares

`images/cha-src-entity-clean-v3/src/models/dim.py:669-674`

```python
DimDeviceConnection = _make_model(
    table="DimDeviceConnection",
    bounds_begin=datetime(2024, 11, 5, tzinfo=ZoneInfo("Europe/Stockholm")),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
    ...
)
```

Because `batching.interval` is set, `_make_model` (`dim.py:71-118`) builds a
bollhav-3 batch that is:

- **rolling / open contract** — `Contract(begin=bounds_begin)` with **no `end`** (`dim.py:77`)
- **daily chunk** — `_TimeChunking(chunk="@daily", window="@daily", ...)` (`dim.py:78-84`)
- **flexible** — `fixed_intervals=False` (`dim.py:81`)

It is one of the two batched dims (the other is `DimIntervention`) that ride with
the **daily-cha-facts** cronjob:
`TAGS="[fact & rst][DimIntervention & rst][DimDeviceConnection & rst]"`,
`LATEST_ENABLED=True`, `WINDOW_OVERRIDE=@daily`, `INTERVAL_OVERRIDE=@daily`,
`LOOKBACK_OVERRIDE=7`.

## 2. The "contract … → 2026-06-01" upper bound is inferred, not declared

The declared contract is just `Contract(begin, end)` with both optional
(`bollhav/model/contract.py:24-30`); for DimDeviceConnection `end is None`.

The string shown in the GUI comes from `get_gaps_grouped`
(`bollhav/postgres/state/read.py:346-516`). The horizon is
(`read.py:461-468`, verified):

```sql
SELECT begin::timestamptz,
       COALESCE(
         <declared_end>::timestamptz,
         (SELECT max(until) FROM <state> WHERE until IS NOT NULL),
         now()
       )
```

With `declared_end = NULL`, the upper bound = **`max(until)` over ALL rows**
(any status). The gap/coverage math, however, only counts **applied** rows
(`read.py:479`: `... WHERE status = 'applied' AND since IS NOT NULL`), and
`pct_covered = covered / contract_seconds` over `[begin, horizon)`
(`read.py:507-509`).

**Key implication:** if any row (even a `pending` one) existed with
`until > 2026-06-01`, the horizon would extend past it. It shows exactly
`2026-06-01`, so **no row — pending or applied — has been written past that
month boundary.** The monthly backfill's last interval ends there and nothing
since has materialized a row beyond it.

## 3. How a monthly backfill pins the bound

`INTERVAL_OVERRIDE` is honored *because the model is flexible*:
`_batching_with_overrides` (`bollhav/model/runtime.py:196-242`) **ignores**
`INTERVAL_OVERRIDE` on fixed models but **applies** it when
`fixed_intervals=False` (`runtime.py:211-224`). So a backfill run with
`INTERVAL_OVERRIDE=@monthly` sets `chunk="@monthly"`.

`@monthly` → cron `0 0 1 * *` (1st of each month, 00:00). `split_window`
(`bollhav/model/window.py:227-243`) then produces intervals on month-firsts:
…, `2026-05-01`, `2026-06-01`. The flexible prefill slices gaps by the *current
run's* chunk (`lifecycle.py:255-289` → `split_window(gap, chunk)`), so the
backfill laid down **monthly** rows, the last ending `until = 2026-06-01`, all
`applied`. Hence `max(until) = 2026-06-01`.

## 4. How the rolling "last complete day" normally works

For a flexible model the horizon is recomputed each run by
`resolve_window(..., reload=True)` → `_trailing_edge` (`window.py:114-130`):

```
floor = latest_complete_interval(chunk).until        # window.py:126-128
return floor if contract.end is None else min(end, floor)
```

With the daily chunk (`INTERVAL_OVERRIDE=@daily`),
`latest_complete_interval("@daily")` = the last complete **day**
(`window.py:79-101`), so a normal daily run rolls the edge forward one day at a
time and `max(until)` advances. Drivers: `WINDOW_OVERRIDE=@daily` (latest-mode
bite), `INTERVAL_OVERRIDE=@daily` (chunk/slice), `LOOKBACK_OVERRIDE=7`
(re-window the last 7 days for late data).

## 5. Why the daily cron hasn't self-healed it (less certain)

The daily cron runs in **latest** mode, whose run window is just the single
latest complete day (`window.py:204-207`), and `get_actionable_intervals` is
window-scoped (`state_table.py:561-609`, the `since >= %s AND until <= %s`
clause). So a latest run does not drain a historical backlog between
`2026-06-01` and today — it only acts within the latest-day window. Combined
with §2 (no rows exist past 2026-06-01), the horizon stays pinned.

> This §5 inference should be confirmed against the actual state rows — see the
> verification query below. The §1–§4 mechanism is solid and already fully
> explains the displayed `2026-06-01`.

## 6. Fix

Re-run **just** DimDeviceConnection as a **daily-grain backfill** over the stuck
span so daily `applied` rows reach the last complete day and the inferred
horizon rolls forward; the daily cron then keeps it advancing.

Suggested run (v3 image, targeted):

- `TAGS="[DimDeviceConnection & rst]"`
- `BACKFILL_ENABLED=true`, `LATEST_ENABLED=false`
- `RUN_SINCE=2026-06-01T00:00:00+02:00`, `RUN_UNTIL` empty (no-dates `until`
  infers the last complete day via `_trailing_edge`)
- `INTERVAL_OVERRIDE=@daily` (re-chunk to daily — allowed, flexible model)
- `STATE_MODE=bulldozer` — `clear_window` (`state_table.py:941-971`) straddle-
  splits/uncovers the stale monthly applied island and deletes stale pending
  rows in the window, then prefill re-covers it at daily grain.

Heavier alternative: `STATE_MODE=torch` with no window deletes all state and
reloads the whole contract `[2024-11-05, last complete day)` at daily grain
(`lifecycle.py:246-249`, `runtime.py:107-118`) — correct but reprocesses
everything.

Note: leave `Contract(end=None)` for a rolling model. Only pin a real
`Contract(end=...)` if you actually want a fixed cap.

## Verification query (run against the cha state Postgres)

Confirms whether the backlog past 2026-06-01 exists as pending rows (so it just
needs draining) or is absent (so latest runs aren't inserting forward):

```sql
-- state table name is z_bollhav_state.<slug>_<digest> for this model;
-- find it via z_bollhav.library WHERE full_name LIKE '%DimDeviceConnection%'.
SELECT status,
       count(*),
       min(since) AS first_since,
       max(until) AS last_until
FROM   <schema>.<state_table>
GROUP  BY status
ORDER  BY status;
```

If you see `applied` rows with `until = 2026-06-01` and no rows beyond it →
monthly island + no forward materialization (the §5 picture). If you see daily
`pending` rows in `[2026-06-01, today)` → they exist but aren't being drained.
Either way the §6 daily bulldozer backfill resolves it.
