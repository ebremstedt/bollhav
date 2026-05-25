# latest_window_vs_batch

A real-world scenario: the warehouse publishes **one full day** of upstream
data overnight, and analysts read it the next morning. We want a single
scheduled run to catch up on the whole day — but the fact table is too
large to load in one INSERT. Locks would be held too long, memory would
spike, and a failure halfway through would force reprocessing the full
day.

The solution uses both cron expressions on `Batch`:

| Field | Value | Role |
|---|---|---|
| `window_expression` | `@daily` | **outer scope** — one full day is what we catch up on |
| `interval_expression` | `*/15 * * * *` | **inner chunks** — split that day into 96 × 15-minute writes |

One run. One day of data. Ninety-six small, independent, retryable chunks.

## Run it

```bash
TAGS="[finance]" USE_SCHEMA_SUFFIX=false LATEST_ENABLED=true \
  python examples/latest_window_vs_batch/main.py
```

Example output:

```
warehouse_finance.fact_transactions
  window_expression : @daily
  interval_expression  : */15 * * * *
  chunks returned   : 96
  first three chunks:
    2026-04-16 00:00:00+00:00 → 2026-04-16 00:15:00+00:00
    2026-04-16 00:15:00+00:00 → 2026-04-16 00:30:00+00:00
    2026-04-16 00:30:00+00:00 → 2026-04-16 00:45:00+00:00
  last three chunks:
    2026-04-16 23:15:00+00:00 → 2026-04-16 23:30:00+00:00
    2026-04-16 23:30:00+00:00 → 2026-04-16 23:45:00+00:00
    2026-04-16 23:45:00+00:00 → 2026-04-17 00:00:00+00:00
```

## Why not just use `interval_expression="*/15 * * * *"` alone?

Without `window_expression`, latest mode returns **one** chunk — the last
completed 15-minute slice. You would need to schedule 96 separate invocations
per day to cover a whole day. `window_expression="@daily"` is what tells
bollhav: "the scope of one run is a full day — now chunk that scope
according to `interval_expression`."

## Why not just use `interval_expression="@daily"` alone?

Then latest mode returns one chunk covering a full day — a single
24-hour INSERT. For a large fact table that's exactly what we are trying
to avoid: long locks, big memory footprint, and an all-or-nothing failure
mode.

## Summary

```
window="@daily", batch="*/15 * * * *"  → 1 run, 96 chunks, daily scope, 15-min writes ✅
window unset,   batch="*/15 * * * *"   → 1 run, 1 chunk, last 15 minutes only
window unset,   batch="@daily"         → 1 run, 1 chunk, yesterday in one INSERT (heavy)
```

See [docs/PIPE.md](../../docs/content/PIPE.md#latest-mode) for the
full rules.
