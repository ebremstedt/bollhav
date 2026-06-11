[Home](index.md) › [Model](MODEL.md) › **Curfew**

# Curfew

Wall-clock hours and/or weekdays a model must **not** run. Reach for it when a model shouldn't touch its source during business hours, weekends, or a maintenance window — set `curfew=Curfew(...)` and the framework skips the model while the curfew is in effect, leaving its work `pending` for a later run.

Models default to **no curfew** (`curfew=None`).

## Fields

| Field | Type | Default | Purpose |
|---|---|---|---|
| `windows` | `list[(time, time)]` | `[]` | Time-of-day `(start, end)` windows. Unioned; `start > end` wraps midnight. **Empty = the whole day.** |
| `days` | `set[int]` | `None` | Weekdays it applies to, Mon=0…Sun=6 (as `datetime.weekday()` / `calendar.MONDAY`). `None` = every day. |
| `tz` | `tzinfo` | `timezone.utc` | The wall clock `windows` / `days` are read in — `"22:00"` is meaningless without one. |
| `allowed` | `bool` | `False` | `False` = **deny** when in effect; `True` = **allow only** when in effect. |

## In effect

The curfew is **in effect** at a moment when **both** hold (read in `tz`):

- the weekday is in `days` (or `days is None`), **and**
- the time is in one of `windows` (or `windows` is empty → the whole day).

So `days` and hours combine — `business_hours` below is "weekdays **and** 09:00–17:00." `allowed` then decides what "in effect" means:

| `allowed` | Blocked when… | Reads as |
|---|---|---|
| `False` (default) | in effect | "don't run during these hours/days" |
| `True` | **not** in effect | "run **only** during these hours/days" |

```python
from datetime import time
from bollhav.model import Curfew

# deny weekday business hours (09:00–17:00, Mon–Fri)
Curfew(windows=[(time(9), time(17))], days={0, 1, 2, 3, 4})

# run ONLY overnight
Curfew(windows=[(time(22), time(6))], allowed=True)
```

## Presets

Named constructors for common curfews. Each takes `tz=` (default UTC) and `allowed=`, so `Curfew.work_hours(allowed=True)` means "run **only** 09:00–17:00."

| Preset | Blocks |
|---|---|
| `Curfew.work_hours()` | 09:00–17:00, every day |
| `Curfew.business_hours()` | 09:00–17:00, weekdays only |
| `Curfew.after_work()` | 17:00–midnight |
| `Curfew.overnight()` | 22:00–06:00 (across midnight) |
| `Curfew.weekend()` | all of Saturday and Sunday |

```python
Model(..., curfew=Curfew.business_hours(tz=ZoneInfo("Europe/Stockholm")))
```

Anything bespoke composes with the plain constructor — `Curfew(windows=..., days=..., tz=..., allowed=...)`.

## What happens when it hits

The curfew is checked twice, and a skipped unit gets **no state transition** — it stays exactly `pending`.

| Where | When | Effect |
|---|---|---|
| [Model lifecycle](MODEL_LIFECYCLE.md) | in effect as the model **starts** | early-out — no lock, no asset DDL, no state bootstrap |
| [Execute lifecycle](EXECUTE_LIFECYCLE.md) | a clear run **crosses into** it mid-way | stops cleanly on the next interval |

Nothing exits, errors, or waits. The process finishes normally having done no work for the skipped units; because the skip is *before* the [state machine](STATE.md) there's no `running` / `applied` / `blocked` / `error` write, and nothing is half-written. The held work runs on the **next invocation** after the window passes — a curfew skips an invocation, it does not pause-and-resume. (`blocked` is reserved for [upstream contracts](UPSTREAM.md); a curfew leaves the unit plain `pending`.)

## See also

- [Bounds](BOUNDS.md) — the historical *range* a model walks, vs curfew's wall-clock *gate* on when it may run
- [Chunking](CHUNKING.md) — the interval the curfew is re-checked per
- [State](STATE.md) — why a skipped unit staying `pending` resumes cleanly next run
- [Model lifecycle](MODEL_LIFECYCLE.md) · [Execute lifecycle](EXECUTE_LIFECYCLE.md) — where the two gates run

Source: [curfew.py](../../bollhav/model/curfew.py) · gates at [lifecycle.py:252](../../bollhav/model/lifecycle.py#L252) (model-level) and [lifecycle.py:390](../../bollhav/model/lifecycle.py#L390) (per interval).
