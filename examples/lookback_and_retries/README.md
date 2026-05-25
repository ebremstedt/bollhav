# lookback_and_retries

Two `Batch` features shown side by side:

- **`lookback=N`** — every interval is extended N cron-ticks backwards
  before it gets chunked. Used to reprocess recent history so late-
  arriving rows are not lost.
- **`retries=N`** — the model carries a retry count, but the retry loop
  itself is implemented by your `execute()` function. This example
  shows one way to wire it up.

One hourly model is defined in [src/models/late_events.py](src/models/late_events.py)
with `lookback=2` and `retries=3`. Bounds cover a 6-hour window so the
output is readable.

## Run it

```bash
TAGS="[events]" USE_SCHEMA_SUFFIX=false BACKFILL_ENABLED=true \
  BACKFILL_SINCE=2024-01-01T04:00:00Z BACKFILL_UNTIL=2024-01-01T06:00:00Z \
  python examples/lookback_and_retries/main.py
```

## What you'll see

The backfill window is `04:00 → 06:00` (2 hours), but `lookback=2`
pushes the start back to `02:00`, so we actually process **4 chunks**:

```
warehouse_events.late_events
  interval_expression : @hourly
  lookback         : 2
  retries          : 3
  chunks returned  : 4
  first chunk      : 2024-01-01 02:00:00+00:00 → 2024-01-01 03:00:00+00:00
  last chunk       : 2024-01-01 05:00:00+00:00 → 2024-01-01 06:00:00+00:00

  chunk 02:00 → 03:00
    attempt 1/4 failed: upstream hiccup (mock) — failure 1/2 for chunk 02:00 → 03:00 — retrying
    attempt 2/4 failed: upstream hiccup (mock) — failure 2/2 for chunk 02:00 → 03:00 — retrying
    wrote [N rows for 02:00→03:00] to warehouse_events.late_events
  chunk 03:00 → 04:00
    attempt 1/4 failed: ...
    ...
```

The mock read deliberately fails the first two attempts for each chunk.
With `retries=3` (so 4 total attempts), every chunk succeeds on its
third try.

## `lookback` in more detail

`lookback` is expressed in **cron ticks**, not wall-clock hours. The
tick size is derived from the batch expression:

| `interval_expression` | `lookback=2` means |
|---|---|
| `@hourly` | 2 hours earlier |
| `@daily` | 2 days earlier |
| `*/15 * * * *` | 30 minutes earlier |

This makes lookback composable with whatever chunk size you chose for a
given model — you write `lookback=2` once and it stays meaningful even
if the batch expression changes.

## `retries` in more detail

`model.batching.retries` is a plain attribute. bollhav never reads it
internally — the retry loop lives in your code. That's deliberate: the
right policy depends on the pipeline (immediate retry vs exponential
backoff, which exceptions are retryable, how to log failures, whether
to page someone on final failure).

See [execute.py](execute.py) for the simplest possible implementation:
a loop that retries `(retries + 1)` times and re-raises on the last
failure.

## Why lookback is dangerous with APPEND

This example uses `WriteMode.APPEND` to keep the config short, but in a
real pipeline that combination **creates duplicates**: every run re-
reads the same rows from the past N ticks and appends them again. For
real use, pair `lookback` with an idempotent write mode:

- `RECREATE_PARTITION` with a `partitioned_by` column
- `UPSERT_NO_DELETE` with a `unique=True` key
- `APPEND` + `truncate_table=True` (for small reference tables — wipes once before the load)

See [docs/WRITEMODES.md](../../docs/content/WRITEMODES.md) for
guidance on picking a write mode.
