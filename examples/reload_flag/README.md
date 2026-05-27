# reload_flag

The `r:` tag prefix flips matched models into **reload** mode. Reload uses
the model's declared `bounds.begin` / `bounds.end` as the interval,
bypassing both latest and backfill.

This is the "just reprocess this model's full declared history, right now"
escape hatch — usually triggered because upstream data was re-emitted, a
schema change invalidated historical rows, or a bug in a transform
produced wrong results.

One daily model is defined in [src/models/customer_dimension.py](src/models/customer_dimension.py),
with bounds `2024-01-01` → `2024-01-11`.

## Incremental run (no reload)

```bash
TAGS="[customers]" USE_SCHEMA_SUFFIX=false LATEST_ENABLED=true \
  python examples/reload_flag/main.py
```

Output:

```
warehouse_clean.customer_dimension
  reload flag     : False
  bounds          : 2024-01-01 00:00:00+00:00 → 2024-01-11 00:00:00+00:00
  chunks returned : 1
  first chunk     : 2026-04-16 00:00:00+00:00 → 2026-04-17 00:00:00+00:00
```

One chunk — the most recent complete day. `bounds` are ignored.

## Reload run (`r:` prefix)

```bash
TAGS="[r:customers]" USE_SCHEMA_SUFFIX=false LATEST_ENABLED=true \
  python examples/reload_flag/main.py
```

Output:

```
warehouse_clean.customer_dimension
  reload flag     : True
  bounds          : 2024-01-01 00:00:00+00:00 → 2024-01-11 00:00:00+00:00
  chunks returned : 10
  first chunk     : 2024-01-01 00:00:00+00:00 → 2024-01-02 00:00:00+00:00
  last chunk      : 2024-01-10 00:00:00+00:00 → 2024-01-11 00:00:00+00:00
```

Ten daily chunks covering the full declared bounds. Note `LATEST_ENABLED=true`
is still set — reload wins. `apply_runtime_overrides` computes
`directives.latest = latest and not directives.reload`, so reload
suppresses latest automatically.

## Reload vs backfill

| | reload (`r:` on tag) | backfill (`BACKFILL_SINCE`/`BACKFILL_UNTIL`) |
|---|---|---|
| Interval source | model's declared `bounds` | env-var window |
| Scope | per-model | whatever matches TAGS |
| Typical use | one-off "reprocess this model's history" | full pipeline bootstrap or historical recovery |

## Reload the sales domain only, leave everything else incremental

```bash
TAGS="[r:customers][all & not:customers]" USE_SCHEMA_SUFFIX=false \
  LATEST_ENABLED=true \
  python examples/reload_flag/main.py
```

Two groups:
- `[r:customers]` — matches `customer_dimension` and sets reload.
- `[all & not:customers]` — matches everything else and runs incrementally.

This pattern lets a single scheduled invocation reload a targeted subset
while everything else continues its normal incremental cadence.

## Changing how reload chunks

Plain `r:` uses whatever the model is statically configured with on `Batch`
(`mode`, `row.batch_size`, `interval.expression`). Two extended prefixes
override that at match time — `apply_runtime_overrides` bakes them into the
returned model's `batching`.

### `r_interval_@<alias>:` — force a different cadence

The customer dimension above is daily. Want to reprocess it in hourly
chunks instead for a one-off run? Override the batch expression at the
tag:

```bash
TAGS="[r_interval_@hourly:customers]" USE_SCHEMA_SUFFIX=false \
  python examples/reload_flag/main.py
```

The `reload` alias also works — `[reload_interval_@hourly:customers]`
does the same thing. Allowed aliases come from roskarl:
`@minutely`/`@minute`, `@hourly`/`@hour`, `@daily`/`@day`,
`@weekly`/`@week`, `@monthly`/`@month`.

For a cadence that doesn't have a named alias, configure it statically
on the model (`Batch(interval_expression="*/15 * * * *")`) and use plain
`r:` to reload, or override globally with
`INTERVAL_EXPRESSION_OVERRIDE="*/15 * * * *"` — arbitrary cron expressions
are intentionally not accepted inside tags.

### `r_row_<N>:` — reload by row count instead of time

For append-only models where time-based chunking doesn't match how the
data actually lands (e.g. you want to replay rows in 1000-row batches
for throttling, back-pressure, or because the table has no useful time
column), switch reload into ROW mode:

```bash
TAGS="[r_row_1000:my_append_table]" USE_SCHEMA_SUFFIX=false \
  python examples/reload_flag/main.py
```

Constraints:
- `WriteMode.APPEND` or `WriteMode.UPSERT_NO_DELETE` — truncate/recreate
  write modes reject ROW because they assume they see the whole dataset
  at once.
- `batch_size` is capped at 10000.
- `model.intervals` refuses to produce time chunks under ROW-mode
  reload — callers branch on `model.batching.mode` and use the
  row-batching execution path.

### Both work at group level too

```bash
TAGS="r_interval_@daily:[sales|finance]" python examples/reload_flag/main.py
TAGS="r_row_500:[append_models]"        python examples/reload_flag/main.py
```

Runtime wins over the model's static config — the same model can be
reloaded INTERVAL on Monday and ROW on Tuesday without code changes.
