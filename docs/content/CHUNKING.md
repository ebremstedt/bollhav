[← home](index.md)

# Chunking

Chunking is optional. Leave `batching=None` on the `Model` and bollhav treats the run as a single unit. Use this for small reference tables, one-shot loads, or anything where the dataset comfortably fits in one pass.

If you decide to chunk, configure a `Batch`:

```python
Batch(
    interval=IntervalChunks(expression="@daily"),  # the (since, until) windows
    size=5000,                                      # rows per read chunk
)
```

Two independent knobs:

- **`interval`** — a cron expression whose ticks define the `(since, until)` windows the model iterates. This is the **recovery unit**: with state tracking enabled, each `(since, until)` is recorded as its own row, so a failed run resumes at exactly the window that broke. Works in all run modes (latest, reload, backfill).
- **`size`** — the number of rows per read chunk, capped at 10000. Within a single interval, the read helpers slice the source into `size`-row frames. This is the **streaming unit** — it keeps memory bounded and gives staging fixed-size sub-batches to land.

These compose: "one day's worth of data, streamed in 5k-row batches" is `interval=@daily` + `size=5000`.

## interval vs size — which controls what

| Concern | Knob |
|---|---|
| Which slice of source a run processes | `interval` (the `(since, until)` window) |
| Crash/resume granularity (state, staging) | `interval` |
| How many rows are read/written at a time | `size` |
| Memory ceiling per read | `size` |

The framework hands `(since, until)` to your read function and loops over the intervals. **Honoring the bounds is the read function's job** — a time-windowed incremental filters `WHERE ts >= since AND ts < until`; a timeless full-reload (a dimension, a lookup) ignores the bounds, reads everything, and still streams it out in `size`-row chunks. There is no separate "row mode" — chunking by rows without a time axis is just a read function that ignores the interval and a single interval spanning your bounds.

## size is honored by the read helpers

`size` is consumed by the read helpers, which slice the source by it. If you hand-roll your own generator, you decide the chunk size — read `model.batching.size` yourself if you want it to match:

```python
def read(model, since, until):
    size = model.batching.size
    for frame in source_query(since, until).iter_slices(size):
        yield frame
```

## Constraints

- **Write mode**: `APPEND` or `UPSERT_NO_DELETE` when streaming sub-batches. Truncate/recreate modes assume they see the whole dataset; partial sub-batches don't fit.
- **`size` ≤ 10000**: above that, per-chunk overhead isn't the bottleneck anymore.

## Override the interval expression for one run

To change the interval expression at runtime without editing the model, use the pipe-level override:

```bash
INTERVAL_EXPRESSION_OVERRIDE="@hourly" python pipeline.py
```

Tags select and reload models — they don't carry chunking config. See [TAGS.md](TAGS.md) for the prefix vocabulary.
