[Home](index.md) › [Model](MODEL.md) › **Batch**

# Batch

How a model's work gets split into chunks. A `Batch` holds a `time` chunking config (a `TimeChunking`), a row `size`, and `retries`:

```python
batching = Batch(
    time=TimeChunking(chunk="@daily"),   # time slicing — see below
    size=20000,                          # row slicing
)
```

The cron/timezone/lookback knobs live on `batching.time` (the `TimeChunking`); `size` and `retries` live on `batching` directly. For the bigger picture — intervals as the recovery unit and `size` as the streaming unit — see [Chunking](BATCH.md#chunking).

## time

Type: `TimeChunking` · Default: `TimeChunking()`

The time-chunking config. Its fields — `chunk`, `latest_window`, `tz`, `lookback`, and `flexibility` (a `ChunkFix` or `ChunkFlex`; `ChunkFlex` carries `floor_chunk`) — are documented below and accessed as `batching.time.chunk`, etc. The fixed-vs-flexible choice and its downstream consequences live in [Upstream](UPSTREAM.md).

## chunk (`batching.time.chunk`)

Type: `IntervalExpression` · Default: `"@daily"`

Chunk size — how a window is split into `TZInterval`s for processing. Accepts any cron expression or `@alias` (e.g. `@hourly`, `*/15 * * * *`). Overridable per run with `INTERVAL_OVERRIDE`.

## latest_window (`batching.time.latest_window`)

Type: `IntervalExpression | None` · Default: `None`

Scope for `latest` mode — "one of what" counts as the latest complete unit. When `None`, falls back to `chunk` (one chunk = one scope). **Consulted only in `latest` mode** — reload/backfill use explicit since/until, and the inferred trailing edge is governed by the model's `floor_chunk`, not this. Overridable per run with `WINDOW_OVERRIDE`.

See [chunk vs latest_window](#chunk-vs-latest_window) below for the worked diagrams.

## tz (`batching.time.tz`)

Type: `tzinfo` · Default: `timezone.utc`

Timezone used for interval resolution.

## lookback (`batching.time.lookback`)

Type: `int` · Default: `None`

Extends each interval's start backwards by N cron-ticks **of `chunk`**. Units are ticks, not calendar days/hours — e.g. with `chunk="*/15 * * * *"`, `lookback=5` is 75 minutes, not 5 days. See [Runtime overrides](DECORATORS.md#lookback).

## flexibility (`batching.time.flexibility`)

Type: `ChunkFix | ChunkFlex` · Default: `ChunkFix()`

The per-model attestation of what the chunk grid *is* — the model's state **identity** or a free slicing of a **coverage set**. See [Fixed vs flexible](UPSTREAM.md) for the full treatment and the downstream-contract consequences.

- **`ChunkFix()`** (default) — state is a grid: one durable `applied` row per chunk. The chunk is part of identity, so changing it needs `STATE_MODE=torch`, and the inferred trailing edge snaps to the latest complete **chunk**.
- **`ChunkFlex(floor_chunk=...)`** — state is a coverage set: work is the *gaps*, sliced by `chunk`, and the chunk is freely re-chunkable. Only a flexible model may carry `floor_chunk` (below) — because only a coverage set can hold a partial tail without forking its identity.

Two read-only helpers derive from it: `batching.time.is_flexible` (bool) and `batching.time.floor_chunk` (the grain the inferred trailing edge floors to — the `ChunkFlex`'s `floor_chunk` else `chunk` for flexible, `chunk` for fixed; **finer than `chunk` allows a partial last chunk**).

### `floor_chunk` (on `ChunkFlex`)

Type: `str | None` · Default: `None` (falls back to `chunk`)

The grain the *inferred* trailing edge floors to — its latest *complete* unit, falling back to `chunk` when unset (read the resolved value off `TimeChunking.floor_chunk`). When a run's window is implied (`reload`, or a no-dates backfill) and the contract is open-ended, the edge advances to the latest *complete* unit of `floor_chunk` instead of the chunk. Set it **finer than `chunk`** to allow a **partial last chunk**: `chunk="@monthly"` with `floor_chunk="@daily"` loads through the last complete **day**, so the current month rides as a partial snapped to a whole day, rather than stopping at the last complete *month*. Can be finer *or* coarser than `chunk` — `chunk="@hourly", floor_chunk="@daily"` processes hourly but only releases settled days (never a partial hour past the last full day). It's the **end** knob; `lookback` is the **start** knob. Not consulted in `latest` mode — that's `latest_window`'s job.

### Worked example: the trailing edge

```python
# `floor_chunk` — coarse slices, fresh edge. A MONTHLY chunk, but the
# inferred trailing edge snaps to the last complete DAY, so the current month
# loads as a partial ending on a whole-day boundary instead of being skipped
# until the whole month is over.
Batch(time=TimeChunking(
    chunk="@monthly",
    flexibility=ChunkFlex(floor_chunk="@daily"),
))
# open contract, today 2026-06-15 → window ends 2026-06-15 (not 2026-06-01)
# without floor_chunk it would end 2026-06-01 — all of June so far left out
```

## size (`batching.size`)

Type: `int` · Default: `20000`

Rows per read chunk, capped at 100000. Within a single interval, the read helpers slice the source into `size`-row frames — the streaming unit that bounds memory and gives staging fixed-size sub-batches. Independent of `chunk`, which controls the `(since, until)` recovery windows. See [Chunking](BATCH.md#chunking).

## retries (`batching.retries`)

Type: `int` · Default: `None`

Retry count on failure.

## `chunk` vs `latest_window`

Two cron expressions on `TimeChunking` that do different jobs:

- **`latest_window`** — the OUTER scope ("catch up on one full DAY")
- **`chunk`** — the INNER chunks ("split that into 15-min WRITES")

Think of the window as the period to cover and chunks as boxes that fill it:

**latest_window=@daily, chunk=`*/15 * * * *`** → 96 × 15-min boxes

```
┌──────────────────────────────────────────────────┐
│📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦│
│📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦│
│📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦│
│📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦│
└──────────────────────────────────────────────────┘
```

**latest_window=@daily, chunk=@hourly** → 24 × hourly boxes

```
┌──────────────────────────────────────────────────┐
│📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦│
└──────────────────────────────────────────────────┘
```

**latest_window unset (defaults to chunk), chunk=@hourly** → 1 × hourly box (13:00 → 14:00)

```
┌──┐
│📦│
└──┘
```

`latest_window` is consulted only in `latest` mode. For `reload`/`backfill` the since/until are explicit and the window is irrelevant.

```python
batching=Batch(
    time=TimeChunking(
        chunk="*/15 * * * *",   # chunk into 15-min pieces
        latest_window="@daily", # scope = one full day
    ),
)
```

When `LATEST_ENABLED=True`, the run's `intervals` hold 96 `TZInterval`s covering yesterday 00:00 → today 00:00.

## Chunking

Two independent knobs split work into manageable pieces — one slices *time*, the other slices *rows*. They compose.

| Knob | Field | Slices | Owned by |
|---|---|---|---|
| **Chunk** | `batching.time.chunk` | time → one `(since, until)` window per tick | the framework (loops `run.intervals`) |
| **Size** | `batching.size` | rows → frames of N rows within a window | your `read()` function |

The **interval is the unit of recovery** — state tracks `(since, until)`, and a crash reruns that window. **Size is the unit of streaming** — it bounds memory by yielding the window's rows in chunks. They're orthogonal: a daily interval can stream in 5 000-row frames, or a single window can be read whole.

### Chunk — slicing time

`batching=Batch(time=TimeChunking(chunk="@daily"))` produces one window per day across the model's run range (see [Contract](CONTRACT.md) for backfill, or latest mode). The framework loops these windows; each is one [unit of work](TEMPORALITY.md) for a batched [temporal](TEMPORALITY.md) model.

```python
batching=Batch(time=TimeChunking(chunk="@hourly"))
# contract 2024-01-01..2024-01-03 → 48 hourly windows
```

A model with no batching has a single unit (`run.intervals == (None,)`) — that's a [`TIMELESS`](TEMPORALITY.md) model (or an unbatched `TEMPORAL` one), not a windowed model.

### Size — slicing rows

`batching=Batch(time=..., size=5000)` is a hint your `read()` honors: produce each window's rows in `size`-row frames. The framework slices time; your read function slices rows.

```python
def read(run, interval) -> Generator[pl.DataFrame, None, None]:
    size = run.model.batching.size
    rows = fetch(interval.since, interval.until)
    for start in range(0, len(rows), size):
        yield pl.DataFrame(rows[start : start + size])
```

Each yielded frame is written (or staged) independently, so nothing accumulates in Python. With [Staging](TARGET.md#staging), the frames land in a staging table and apply atomically per interval.

### Putting it together

A 3-day daily model with `size=5000` over 12 000 rows/day runs as **3 windows × (5000 + 5000 + 2000)** — three recovery units, nine write chunks.
