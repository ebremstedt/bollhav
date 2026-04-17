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
is still set — reload wins. `runtime_override.apply_pipe` reads
`self.latest = pipe.latest.enabled and not self.reload`, so reload
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
