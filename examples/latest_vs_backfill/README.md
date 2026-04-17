# latest_vs_backfill

Shows how the **same model** produces different interval plans depending on
whether the pipe runs in **latest** mode or **backfill** mode.

One hourly model is defined in [src/models/page_views.py](src/models/page_views.py),
with `bounds` spanning 2024-01-01 → 2024-01-04.

## Latest mode

```bash
TAGS="[web]" USE_SCHEMA_SUFFIX=false LATEST_ENABLED=true \
  python examples/latest_vs_backfill/main.py
```

- Resolves **the most recent complete hour**, ending at the top of the current hour.
- The hour currently in progress is never returned — "complete" means fully elapsed.
- `bounds` are **ignored** in latest mode. Latest always looks at *now*.

Example output (time-of-day depends on when you run it):

```
warehouse_web.page_views
  batch_expression : @hourly
  bounds           : 2024-01-01 00:00:00+00:00 → 2024-01-04 00:00:00+00:00
  chunks returned  : 1
  first chunk      : 2026-04-17 12:00:00+00:00 → 2026-04-17 13:00:00+00:00
  last chunk       : 2026-04-17 12:00:00+00:00 → 2026-04-17 13:00:00+00:00
```

## Backfill mode (default)

```bash
TAGS="[web]" USE_SCHEMA_SUFFIX=false BACKFILL_ENABLED=true \
  BACKFILL_SINCE=2024-01-01T00:00:00Z BACKFILL_UNTIL=2024-01-04T00:00:00Z \
  python examples/latest_vs_backfill/main.py
```

- Uses the explicit `BACKFILL_SINCE` / `BACKFILL_UNTIL` window.
- Chunked by the model's `batch_expression` (`@hourly`) → **72 chunks** over the 3-day window.

Example output:

```
warehouse_web.page_views
  batch_expression : @hourly
  bounds           : 2024-01-01 00:00:00+00:00 → 2024-01-04 00:00:00+00:00
  chunks returned  : 72
  first chunk      : 2024-01-01 00:00:00+00:00 → 2024-01-01 01:00:00+00:00
  last chunk       : 2024-01-03 23:00:00+00:00 → 2024-01-04 00:00:00+00:00
```

## When to use which

| Mode | Use when |
|---|---|
| `LATEST_ENABLED=true` | scheduled incremental runs — "pick up whatever's complete and move on" |
| `BACKFILL_ENABLED=true` + since/until | bootstrapping a new model, reprocessing history, one-off remediation |

You can't set both at once — the pipe raises `ValueError` at startup if you try.
