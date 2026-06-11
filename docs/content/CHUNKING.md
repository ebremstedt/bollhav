[Home](index.md) › [Model](MODEL.md) › **Chunking**

# Chunking

Two independent knobs split work into manageable pieces. They compose — one slices *time*, the other slices *rows*.

| Knob | Field | Slices | Owned by |
|---|---|---|---|
| **Chunk** | `batching.time.chunk` | time → one `(since, until)` window per tick | the framework (loops `run.intervals`) |
| **Size** | `batching.size` | rows → frames of N rows within a window | your `read()` function |

The **interval is the unit of recovery** — state tracks `(since, until)`, and a crash reruns that window. **Size is the unit of streaming** — it bounds memory by yielding the window's rows in chunks. They're orthogonal: a daily interval can stream in 5 000-row frames, or a single window can be read whole.

## Chunk — slicing time

`batching=Batch(time=TimeChunking(chunk="@daily"))` produces one window per day across the model's run range (see [Bounds](BOUNDS.md) for backfill, or latest mode). The framework loops these windows; each is one [unit of work](KINDS.md) for an `interval`-kind model.

```python
batching=Batch(time=TimeChunking(chunk="@hourly"))
# bounds 2024-01-01..2024-01-03 → 48 hourly windows
```

A model with no batching has a single unit (`run.intervals == (None,)`) — that's a `MONOLITHIC` or `VIEW` [kind](KINDS.md), not an interval one.

## Size — slicing rows

`batching=Batch(time=..., size=5000)` is a hint your `read()` honors: produce each window's rows in `size`-row frames. The framework slices time; your read function slices rows.

```python
def read(run, interval) -> Generator[pl.DataFrame, None, None]:
    size = run.model.batching.size
    rows = fetch(interval.since, interval.until)
    for start in range(0, len(rows), size):
        yield pl.DataFrame(rows[start : start + size])
```

Each yielded frame is written (or staged) independently, so nothing accumulates in Python. With [Staging](STAGING.md), the frames land in a staging table and apply atomically per interval.

## Putting it together

A 3-day daily model with `size=5000` over 12 000 rows/day runs as **3 windows × (5000 + 5000 + 2000)** — three recovery units, nine write chunks. See [Batch](BATCH.md) for the full field reference.
