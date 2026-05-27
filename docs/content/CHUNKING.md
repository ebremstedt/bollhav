[← home](index.md)

# Chunking

Chunking is optional. Leave `batching=None` on the `Model` and bollhav treats the run as a single unit. Use this for small reference tables, one-shot loads, or anything where the dataset comfortably fits in one pass.

If you decide to chunk, every model chunks its work via `Batch(mode=...)`:

- **`ChunkMode.INTERVAL`** (default) — chunk by time via a cron `interval_expression`. Works in all run modes (latest, reload, backfill).
- **`ChunkMode.ROW`** — chunk by row count via `row.batch_size`. **Reload only** — latest and backfill raise when `model.intervals` is read.

!!! note "Proposed: `ChunkMode.ID`"
    Chunk by ranges of an integer/UUID identifier (`id_column`, `id_batch_size`) instead of time or row count. The use case is large dimensional tables with a monotonic primary key but no useful timestamp — INTERVAL doesn't apply, and ROW pages the whole table top-to-bottom every reload. ID-chunking would let you say "process IDs `0–10000`, then `10000–20000`, …" which is resumable and parallelizable per chunk.

    Not implemented. Open question: do we lean on the state table (each ID range = one state row, just like an interval) or invent a parallel mechanism. Track in a follow-up if/when there's a real model that needs it.

## When INTERVAL is the right tool

1. **Time-keyed data, reasonably even distribution.** Event streams, timestamped facts, anything with a usable `created_at` / `event_time` where rows are not wildly clustered into one window.
2. **You need latest or backfill mode.** Daily incrementals ("process yesterday") and historical catch-up runs ("everything from 2020 to now") both rely on time slicing. ROW is reload-only — if either of these matter, INTERVAL is the only option.
3. **Per-slice resumability.** With state tracking enabled, each `(since, until)` is recorded as its own row — a failed run picks up at exactly the interval that broke, without redoing applied work.

## When ROW is the right tool

1. **No usable timestamp.** Dimensional snapshots, CDC logs keyed by sequence ID, anything where `model.intervals` would need a synthetic time column.
2. **Severe time skew.** One day in 2022 had 200× the rows of any other. INTERVAL reload blows up on that day; ROW keeps chunk size predictable.
3. **Fixed-size writes for throughput.** Columnar loaders, back-pressure-sensitive sinks, or per-batch-overhead-bound systems where chunk size is the tuning knob and time alignment is incidental.

## Constraints

- **Write mode**: `APPEND` or `UPSERT_NO_DELETE`. Truncate/recreate modes assume they see the whole dataset; partial row-batches don't fit.
- **Reload only**: latest/backfill raise when `model.intervals` is read.
- **`batch_size` ≤ 10000**: above that, per-chunk overhead isn't the bottleneck anymore.
- **`model.intervals` not usable for ROW reloads**: callers branch on `model.batching.mode` and drive row chunks themselves.

## Override the chunking with a tag

If a model that's normally INTERVAL needs to run as ROW for one invocation — say, that one day in 2022 with 200× the rows — don't edit the model. Tag the run.

```bash
TAGS="[r_row_500:facts_2022]" python pipeline.py
```

What this says:

- `facts_2022` — the model to match
- `r_row_500:` — reload it in ROW mode, 500 rows per chunk

The model's definition in code is unchanged. The override only applies to this run.

The inverse exists too — `r_interval_@daily:` flips a ROW model to INTERVAL for one run. See [TAGS.md](TAGS.md) for the full prefix vocabulary.