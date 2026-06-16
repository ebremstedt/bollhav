# curfew — a wall-clock gate on when a model may run

A `Curfew` stops a model from running during certain hours and/or on certain
weekdays. It's checked **twice**: once up front in `@model_lifecycle` (an
early-out that skips the whole model before any setup), and again **per
interval** in `@execute_lifecycle`. Either way a skipped unit gets **no state
transition** — it stays `pending` for a later (post-curfew) run.

```python
from datetime import time
from zoneinfo import ZoneInfo
from bollhav.model import Curfew

# deny: don't run during the morning + afternoon peaks, on weekdays
Curfew(windows=[(time(9), time(11)), (time(14), time(16))], days={0,1,2,3,4},
       tz=ZoneInfo("Europe/Stockholm"))

# allow-window: run ONLY overnight (blocked the rest of the day)
Curfew(windows=[(time(22), time(6))], allowed=True)
```

| field | meaning |
|---|---|
| `windows` | list of `(start, end)` time-of-day pairs (unioned; `start > end` wraps midnight; **empty = whole day**) |
| `days` | weekdays it applies to, Mon=0…Sun=6 (`None` = every day) |
| `tz` | the wall clock windows/days are read in — `"22:00"` is meaningless without one |
| `allowed` | `False` (default) = **deny** when in effect; `True` = **allow only** when in effect |

The curfew is "in effect" when the weekday matches `days` **and** the time is in
a window — so `days` and hours combine.

## Presets

Named constructors for the common cases (each takes `tz=` and `allowed=`):

```python
Curfew.work_hours()       # don't run 09:00–17:00, any day
Curfew.business_hours()   # don't run 09:00–17:00 on weekdays (Mon–Fri)
Curfew.after_work()       # don't run 17:00–midnight
Curfew.overnight()        # don't run 22:00–06:00
Curfew.weekend()          # don't run Sat/Sun at all
Curfew.work_hours(allowed=True, tz=ZoneInfo("Europe/Stockholm"))  # run ONLY 09:00–17:00 Stockholm
```

## What happens when a curfew hits

Nothing exits, nothing errors. If the curfew is in effect when the model
**starts**, `@model_lifecycle` returns early — no lock, no `CREATE TABLE`, no
state bootstrap. If a model that started clear **crosses into** a curfew partway
through, the per-interval check in `@execute_lifecycle` stops it on the next
unit. Either way there's **no `running`/`applied`/`blocked`/`error` write** — the
work stays `pending`, nothing is half-written, and the process exits 0. The held
work runs on the **next invocation** after the window passes (your scheduler
re-runs the job) — a curfew skips an invocation, it does not pause-and-wait.

## What this demo shows

`main.py` runs the **same** `events` model twice, moving only the curfew window:

```
Run 1 — curfew covers now      → whole model skipped (model-level), 0 rows, no state table
Run 2 — curfew window away     → model runs fully, 30 rows, state {applied: 3}
```

Run 1 logs a single `curfew: skipping model …` and does no setup at all; Run 2
runs the work. Re-invoking after the window is how the held work gets done.

## Run it

```bash
export TARGET_DSN='postgresql://postgres:postgres@localhost:5432/postgres'
python main.py        # watch the INFO "curfew: skipping model …" line on Run 1
```

The window is computed relative to *now* (covering now for Run 1, six hours out
for Run 2), so the demo behaves identically whatever time you run it.

## Files

- [build_model.py](build_model.py) — builds the `events` model with a given curfew (the model is frozen, so each run builds a fresh one)
- [run_model.py](run_model.py) / [run_interval.py](run_interval.py) — the lifecycle loop; the curfew gate lives in both `@model_lifecycle` (early-out) and `@execute_lifecycle` (per interval)
- [mock_read.py](mock_read.py) — deterministic mock rows
- [main.py](main.py) — the two-run demo
