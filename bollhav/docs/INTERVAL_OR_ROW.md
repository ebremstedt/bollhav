[back to README](../../README.md)

# Interval vs. row chunking

Every model chunks its work via `Batch(mode=...)`:

- **`ChunkMode.INTERVAL`** (default) — chunk by time via a cron `interval_expression`. Works in all run modes (latest, reload, backfill).
- **`ChunkMode.ROW`** — chunk by row count via `row.batch_size`. **Reload only** — latest and backfill raise at `infer_intervals()`.

## Default to INTERVAL

Works everywhere, pairs with latest/backfill/reload uniformly, everyone on the team can reason about "yesterday's chunk." If your data has usable timestamps and isn't wildly uneven across time, use INTERVAL and stop reading.

## When ROW is the right tool

1. **No usable timestamp.** Dimensional snapshots, CDC logs keyed by sequence ID, anything where `infer_intervals` would need a synthetic time column.
2. **Severe time skew.** One day in 2022 had 200× the rows of any other. INTERVAL reload blows up on that day; ROW keeps chunk size predictable.
3. **Fixed-size writes for throughput.** Columnar loaders, back-pressure-sensitive sinks, or per-batch-overhead-bound systems where chunk size is the tuning knob and time alignment is incidental.

## Constraints

- **Write mode**: `APPEND` or `UPSERT_NO_DELETE`. Truncate/recreate modes assume they see the whole dataset; partial row-batches don't fit.
- **Reload only**: latest/backfill raise at `infer_intervals()`.
- **`batch_size` ≤ 10000**: above that, per-chunk overhead isn't the bottleneck anymore.
- **`infer_intervals()` not usable for ROW reloads**: callers branch on `model.batching.mode` and drive row chunks themselves.

## Runtime override via tag

`r_row_<N>:` flips any INTERVAL-configured model into ROW for a single run:

```bash
TAGS="[r_row_500:facts_2022]" python pipeline.py
```

Model stays INTERVAL in static config; `apply_runtime_overrides` bakes the tag override into `batching.mode`/`batching.row.batch_size` on the returned copy so the run uses ROW with 500 rows/chunk. See [TAGS.md](TAGS.md) for the inverse (`r_interval_@...:`).

## TL;DR

| Situation | Use |
|---|---|
| Time-sliced data, normal volume | `INTERVAL` |
| No time column (dims, CDC) | `ROW` |
| INTERVAL hurts on one bad day | `INTERVAL` + `r_row_<N>:` tag, or switch to `ROW` |
| Throughput tuning | `ROW` |
| Need latest/backfill | `INTERVAL` |

Start with INTERVAL. Switch specific models to ROW only after measuring why.
