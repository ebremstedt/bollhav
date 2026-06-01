[← Model](MODEL.md)

# Batch

How a model's work gets split into chunks. The fields below control the interval expression, row `size`, timezone, lookback, and retry behavior. For the bigger picture — intervals as the recovery unit and `size` as the streaming unit — see [Chunking](CHUNKING.md).

## interval_expression

Type: `IntervalExpression` · Default: `"@daily"`

Chunk size — how an interval is split into `TZInterval`s for processing. Accepts any cron expression or `@alias` (e.g. `@hourly`, `*/15 * * * *`).

## window_expression

Type: `IntervalExpression | None` · Default: `None`

Scope for `latest` mode — "one of what" counts as the latest complete unit. When `None`, falls back to `interval_expression` (one chunk = one scope, pre-window behavior). Ignored in reload/backfill, which use explicit since/until.

See [interval_expression vs window_expression](#interval_expression-vs-window_expression) below for the worked diagrams.

## tz

Type: `tzinfo` · Default: `timezone.utc`

Timezone used for interval resolution.

## lookback

Type: `int` · Default: `None`

Extends each interval's start backwards by N cron-ticks **of the `interval_expression`**. Units are ticks, not calendar days/hours — e.g. with `interval_expression="*/15 * * * *"`, `lookback=5` is 75 minutes, not 5 days. See [RUNTIME_OVERRIDES.md](RUNTIME_OVERRIDES.md#lookback).

## size

Type: `int` · Default: `10000`

Rows per read chunk, capped at 10000. Within a single interval, the read helpers slice the source into `size`-row frames — the streaming unit that bounds memory and gives staging fixed-size sub-batches. Independent of `interval_expression`, which controls the `(since, until)` recovery windows. See [Chunking](CHUNKING.md).

## retries

Type: `int` · Default: `None`

Retry count on failure.

## `interval_expression` vs `window_expression`

Two cron expressions that do different jobs:

- **`window_expression`** — the OUTER scope ("catch up on one full DAY")
- **`interval_expression`** — the INNER chunks ("split that into 15-min WRITES")

Think of the window as the period to cover and intervals as boxes that fill it:

**window=@daily, interval=`*/15 * * * *`** → 96 × 15-min boxes

```
┌──────────────────────────────────────────────────┐
│📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦│
│📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦│
│📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦│
│📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦│
└──────────────────────────────────────────────────┘
```

**window=@daily, interval=@hourly** → 24 × hourly boxes

```
┌──────────────────────────────────────────────────┐
│📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦📦│
└──────────────────────────────────────────────────┘
```

**window unset (defaults to interval), interval=@hourly** → 1 × hourly box (13:00 → 14:00)

```
┌──┐
│📦│
└──┘
```

`window_expression` is consulted only in `latest` mode. For `reload`/`backfill` the since/until are explicit and the window is irrelevant.

```python
batching=Batch(
    interval_expression="*/15 * * * *",   # chunk into 15-min pieces
    window_expression="@daily",           # scope = one full day
)
```

When `LATEST_ENABLED=True`, `model.intervals` returns 96 `TZInterval`s covering yesterday 00:00 → today 00:00.
