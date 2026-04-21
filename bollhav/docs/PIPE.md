[back to README](../../README.md)

# Pipe

Standardizes code at the **pipe** level by preloading the variables below:

## General
| Variable | Type | Required | Description |
|---|---|---|---|
| **TAGS** | string | yes | Tag expression to filter models |
| **SCHEMA_SUFFIX** | string | yes | Suffix appended to schema name in non-production |
| **USE_SCHEMA_SUFFIX** | bool | no | Enables schema suffix, defaults to **True** |
| **DEBUG** | bool | no | Enables timestamped debug prints |
| **TIMEZONE_OVERRIDE** | string | no | IANA timezone (e.g. `Europe/Stockholm`) that overrides all model timezones |
| **LATEST_ENABLED** | bool | no | Enables latest mode, cannot be True along with **BACKFILL_ENABLED** |
| **BACKFILL_ENABLED** | bool | no | Enables backfill mode, defaults to **True** when **LATEST_ENABLED** is not set. Cannot be True along with **LATEST_ENABLED** |
| **BATCH_EXPRESSION_OVERRIDE** | IntervalExpression | no | Overrides the model's batch expression (chunk size, applies in all modes) |
| **WINDOW_EXPRESSION_OVERRIDE** | IntervalExpression | no | Overrides the model's window expression (latest-mode scope). Errors at startup if set without **LATEST_ENABLED** |
| **UPSTREAM** | string | no | One of `enforce` (default), `ignore_views`, `ignore_completely`. Controls how upstream dependencies are enforced by `match_models` |


## Latest mode

Resolves the most recent **complete** interval, then chunks it.

Two cron expressions are involved — the distinction matters in latest mode:

- **`window_expression`** defines the *scope* — "one of what" counts as the latest complete unit. Falls back to `batch_expression` when unset.
- **`batch_expression`** defines the *chunk size* — how the scope is split into `TZInterval`s.

### Examples (assume now = 2024-06-15 14:35 UTC)

| `window_expression` | `batch_expression` | Result |
|---|---|---|
| unset (falls back to batch) | `@hourly` | 1 chunk: **13:00 → 14:00** (one hour, one chunk) |
| `@daily` | `@hourly` | 24 chunks covering **Jun 14 00:00 → Jun 15 00:00** |
| `@daily` | `*/15 * * * *` | 96 fifteen-minute chunks covering **Jun 14 00:00 → Jun 15 00:00** |

The 14:00-15:00 hour (and Jun 15 day) is in progress and never included — "complete" means the entire window has passed.


## Backfill mode (default)

Uses an explicit time window, chunked by the batch expression. If `BACKFILL_UNTIL` is not set, it defaults to the end of the latest complete interval based on the model's batch expression and timezone.

| Variable | Type | Required | Description |
|---|---|---|---|
| **BACKFILL_SINCE** | ISO 8601 datetime | yes | Start of backfill window |
| **BACKFILL_UNTIL** | ISO 8601 datetime | no | End of backfill window. Defaults to the latest complete interval end |


## Timezone

Each model defines its own timezone via `Batch(tz=...)`, defaulting to UTC. The `TIMEZONE_OVERRIDE` env var overrides all model timezones at the pipe level when set.

This affects:
- **Latest mode** — which hour/day boundary counts as "now"
- **Backfill mode** — replaces the timezone on `BACKFILL_SINCE` and `BACKFILL_UNTIL`


## PipeConfig fields

| Field | Type | Description |
|---|---|---|
| `tags` | `str` | Tag expression from `TAGS` |
| `debug` | `bool` | From `DEBUG` |
| `schema_suffix` | `str` | From `SCHEMA_SUFFIX` |
| `use_schema_suffix` | `bool` | From `USE_SCHEMA_SUFFIX` |
| `tz_override` | `tzinfo \| None` | From `TIMEZONE_OVERRIDE` |
| `batch_expression_override` | `str \| None` | From `BATCH_EXPRESSION_OVERRIDE` |
| `window_expression_override` | `str \| None` | From `WINDOW_EXPRESSION_OVERRIDE` — only valid when `latest.enabled` |
| `upstream_mode` | `UpstreamMode` | From `UPSTREAM` (defaults to `ENFORCE`) |
| `latest.enabled` | `bool` | From `LATEST_ENABLED` |
| `backfill.enabled` | `bool` | From `BACKFILL_ENABLED` |
| `backfill.since` | `datetime \| None` | From `BACKFILL_SINCE` |
| `backfill.until` | `datetime \| None` | From `BACKFILL_UNTIL` |


## Example using the decorator

```python
from bollhav.pipe import PipeConfig, with_pipe_config

@with_pipe_config
def main(pipe: PipeConfig) -> None:
    do stuff
```
