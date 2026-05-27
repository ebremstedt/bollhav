[← home](index.md)

# Runtime overrides

Models are static definitions of *where* and *how* data flows. At run time you can override a few of those settings — pick a tag set, switch on latest/backfill mode, force a different schema suffix, etc. — without editing the model files.

The standard entry point is the `@load_models` decorator. It reads the env vars below, validates them, and hands you a list of models with the overrides already baked into `batching` / `target.schema` / `directives`.

```python
from bollhav.model import Model, load_models

@load_models
def main(models: list[Model], debug: bool) -> None:
    for model in models:
        for interval in model.intervals:
            execute(model, interval, ...)


if __name__ == "__main__":
    main()
```

For programmatic use (or tests) call `apply_runtime_overrides(...)` directly with explicit kwargs — same effect, no env reading.

## Env vars

| Variable | Type | Required | Description |
|---|---|---|---|
| **TAGS** | string | yes | Tag expression to filter models |
| **SCHEMA_SUFFIX** | string | yes | Suffix appended to schema name in non-production |
| **USE_SCHEMA_SUFFIX** | bool | no | Enables schema suffix, defaults to **True** |
| **DEBUG** | bool | no | Enables timestamped debug prints |
| **TIMEZONE_OVERRIDE** | string | no | IANA timezone (e.g. `Europe/Stockholm`) that overrides every model's timezone |
| **LATEST_ENABLED** | bool | no | Enables latest mode. Cannot be `True` along with **BACKFILL_ENABLED** |
| **BACKFILL_ENABLED** | bool | no | Enables backfill mode, defaults to **True** when **LATEST_ENABLED** is unset. Cannot be `True` along with **LATEST_ENABLED** |
| **BACKFILL_SINCE** | ISO 8601 datetime | yes (in backfill) | Start of backfill window |
| **BACKFILL_UNTIL** | ISO 8601 datetime | no | End of backfill window. Defaults to the latest complete interval end |
| **INTERVAL_EXPRESSION_OVERRIDE** | IntervalExpression | no | Overrides every model's `interval_expression` (chunk size, applies in all modes) |
| **WINDOW_EXPRESSION_OVERRIDE** | IntervalExpression | no | Overrides every model's `window_expression` (latest-mode scope). Errors at startup if set without **LATEST_ENABLED** |
| **LOOKBACK_OVERRIDE** | non-negative int | no | Overrides every model's `lookback`. Shifts each interval's `since` backwards by N cron-ticks of the (post-override) interval expression. Applies in latest, backfill, and reload modes |
| **UPSTREAM** | string | no | One of `enforce` (default), `ignore_views`, `ignore_completely`. Controls upstream-dependency enforcement |
| **DRY_RUN** | bool | no | When `True`, `@load_models` prints a concise summary of matched models and exits without invoking the wrapped function |
| **DRY_RUN_EXTRA** | bool | no | When `True`, same short-circuit but prints an exhaustive per-model block (schema, bounds, tags, source, upstream, …). Implies `DRY_RUN=true` |

## Latest mode

Resolves the most recent **complete** interval, then chunks it.

Two cron expressions matter here:

- **`window_expression`** defines the *scope* — "one of what" counts as the latest complete unit. Falls back to `interval_expression` when unset.
- **`interval_expression`** defines the *chunk size* — how the scope is split into `TZInterval`s.

### Examples (assume now = 2024-06-15 14:35 UTC)

| `window_expression` | `interval_expression` | Result |
|---|---|---|
| unset (falls back to interval) | `@hourly` | 1 chunk: **13:00 → 14:00** (one hour, one chunk) |
| `@daily` | `@hourly` | 24 chunks covering **Jun 14 00:00 → Jun 15 00:00** |
| `@daily` | `*/15 * * * *` | 96 fifteen-minute chunks covering **Jun 14 00:00 → Jun 15 00:00** |

The 14:00-15:00 hour (and Jun 15 day) is in progress and never included — "complete" means the entire window has passed.

## Lookback

`lookback` shifts each resolved interval's `since` **backwards by N cron-ticks of the (post-override) interval expression**. Units are *ticks of the interval expression*, not calendar days/hours — this is the most common footgun.

### Examples

| `interval_expression` | `lookback` | Effect on `since` |
|---|---|---|
| `@daily` (`0 0 * * *`) | `5` | back 5 days |
| `@hourly` (`0 * * * *`) | `5` | back 5 hours |
| `*/15 * * * *` | `5` | back 75 minutes (5 × 15 min) |
| `*/15 * * * *` | `480` | back 5 days (480 × 15 min) |

The cron expression used for the tick size is the one in effect at run time — i.e. after **INTERVAL_EXPRESSION_OVERRIDE** is applied. So if you set both `INTERVAL_EXPRESSION_OVERRIDE=*/15 * * * *` and `LOOKBACK_OVERRIDE=5`, you get 75 minutes back, not 5 days.

Applies uniformly in latest, backfill, and reload modes.

## Backfill mode (default)

Uses an explicit time window, chunked by the interval expression. If `BACKFILL_UNTIL` is unset, it defaults to the end of the latest complete interval.

## Dry run

`DRY_RUN=true` short-circuits `@load_models` after matching and resolving intervals. The wrapped `main()` is not invoked.

For each matched model, the summary shows:

- `cron` — the effective `interval_expression` (post-overrides)
- `window` — first-since → last-until of the resolved intervals
- `intervals` — how many will run

Models with `batching=None` show `batching: none (single unfiltered run)` instead.

```
── dry run ──────────────────────────────────────────────
 1 model matched, mode = backfill

▸ public.orders
    cron      : @daily
    window    : 2024-01-01T00:00:00+00:00 → 2024-01-11T00:00:00+00:00
    intervals : 10
──────────────────────────────────────────────────────────
```

## Timezone

Each model defines its own timezone via `Batch(tz=...)`, defaulting to UTC. The `TIMEZONE_OVERRIDE` env var replaces every model's timezone at runtime.

This affects:
- **Latest mode** — which hour/day boundary counts as "now"
- **Backfill mode** — replaces the timezone on `BACKFILL_SINCE` and `BACKFILL_UNTIL`

## `apply_runtime_overrides` programmatic form

```python
from bollhav.model import apply_runtime_overrides

models = apply_runtime_overrides(
    folder="src/models",
    tags="[customers]",
    schema_suffix="dev",
    latest=True,
    # backfill_since=..., backfill_until=...,
    # interval_expression_override="@hourly",
    # window_expression_override="@daily",
    # lookback_override=5,
    # tz_override=ZoneInfo("Europe/Stockholm"),
    # upstream_mode=UpstreamMode.IGNORE_VIEWS,
)
```

Same merging logic as `@load_models`, just no env reading. Useful in tests and programmatic invocations.
