[Home](index.md) › [Model](MODEL.md) › **Batch**

# Batch

How a model's work gets split into chunks. A `Batch` holds a `time` chunking config (a `TimeChunking`), a row `size`, and `retries`:

```python
batching = Batch(
    time=TimeChunking(chunk="@daily"),   # time slicing — see below
    size=20000,                          # row slicing
)
```

The cron/timezone/lookback knobs live on `batching.time` (the `TimeChunking`); `size` and `retries` live on `batching` directly. For the bigger picture — intervals as the recovery unit and `size` as the streaming unit — see [Chunking](CHUNKING.md).

## time

Type: `TimeChunking` · Default: `TimeChunking()`

The time-chunking config. Its fields — `chunk`, `window`, `tz`, `lookback` — are documented below and accessed as `batching.time.chunk`, etc.

## chunk (`batching.time.chunk`)

Type: `IntervalExpression` · Default: `"@daily"`

Chunk size — how a window is split into `TZInterval`s for processing. Accepts any cron expression or `@alias` (e.g. `@hourly`, `*/15 * * * *`). Overridable per run with `INTERVAL_EXPRESSION_OVERRIDE`.

## window (`batching.time.window`)

Type: `IntervalExpression | None` · Default: `None`

Scope for `latest` mode — "one of what" counts as the latest complete unit. When `None`, falls back to `chunk` (one chunk = one scope). Ignored in reload/backfill, which use explicit since/until. Overridable per run with `WINDOW_EXPRESSION_OVERRIDE`.

See [chunk vs window](#chunk-vs-window) below for the worked diagrams.

## tz (`batching.time.tz`)

Type: `tzinfo` · Default: `timezone.utc`

Timezone used for interval resolution.

## lookback (`batching.time.lookback`)

Type: `int` · Default: `None`

Extends each interval's start backwards by N cron-ticks **of `chunk`**. Units are ticks, not calendar days/hours — e.g. with `chunk="*/15 * * * *"`, `lookback=5` is 75 minutes, not 5 days. See [RUNTIME_OVERRIDES.md](RUNTIME_OVERRIDES.md#lookback).

## size (`batching.size`)

Type: `int` · Default: `20000`

Rows per read chunk, capped at 100000. Within a single interval, the read helpers slice the source into `size`-row frames — the streaming unit that bounds memory and gives staging fixed-size sub-batches. Independent of `chunk`, which controls the `(since, until)` recovery windows. See [Chunking](CHUNKING.md).

## retries (`batching.retries`)

Type: `int` · Default: `None`

Retry count on failure.

## `chunk` vs `window`

Two cron expressions on `TimeChunking` that do different jobs:

- **`window`** — the OUTER scope ("catch up on one full DAY")
- **`chunk`** — the INNER chunks ("split that into 15-min WRITES")

Think of the window as the period to cover and chunks as boxes that fill it:

**window=@daily, chunk=`*/15 * * * *`** → 96 × 15-min boxes

```
┌──────────────────────────────────────────────────┐
│📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦│
│📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦│
│📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦│
│📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦│
└──────────────────────────────────────────────────┘
```

**window=@daily, chunk=@hourly** → 24 × hourly boxes

```
┌──────────────────────────────────────────────────┐
│📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦│
└──────────────────────────────────────────────────┘
```

**window unset (defaults to chunk), chunk=@hourly** → 1 × hourly box (13:00 → 14:00)

```
┌──┐
│📦│
└──┘
```

`window` is consulted only in `latest` mode. For `reload`/`backfill` the since/until are explicit and the window is irrelevant.

```python
batching=Batch(
    time=TimeChunking(
        chunk="*/15 * * * *",   # chunk into 15-min pieces
        window="@daily",        # scope = one full day
    ),
)
```

When `LATEST_ENABLED=True`, the run's `intervals` hold 96 `TZInterval`s covering yesterday 00:00 → today 00:00.
