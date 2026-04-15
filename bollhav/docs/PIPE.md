[back to README](..README.md)

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


## Latest mode

Resolves the most recent **complete** interval based on the batch expression and the current time.

The batch expression is resolved in this order:
1. `LATEST_BATCH_EXPRESSION` (pipe-level override)
2. The model's `Batch.batch_expression`

For example, with an hourly expression (`0 * * * *`) and current time 14:35 UTC:
- The 14:00-15:00 interval is still in progress, so it's skipped
- The interval **13:00-14:00** is returned as the latest complete interval

| Variable | Type | Required | Description |
|---|---|---|---|
| **LATEST_BATCH_EXPRESSION** | BatchExpression | no | Overrides the model's batch expression |


## Backfill mode

Uses an explicit time window, chunked by the batch expression.

| Variable | Type | Required | Description |
|---|---|---|---|
| **BACKFILL_SINCE** | ISO 8601 datetime | yes | Start of backfill window |
| **BACKFILL_UNTIL** | ISO 8601 datetime | yes | End of backfill window |
| **BACKFILL_BATCH_EXPRESSION** | BatchExpression | no | Overrides the model's batch expression |


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
| `latest.enabled` | `bool` | From `LATEST_ENABLED` |
| `latest.batch_expression` | `str \| None` | From `LATEST_BATCH_EXPRESSION` |
| `backfill.enabled` | `bool` | From `BACKFILL_ENABLED` |
| `backfill.batch_expression` | `str \| None` | From `BACKFILL_BATCH_EXPRESSION` |
| `backfill.since` | `datetime \| None` | From `BACKFILL_SINCE` |
| `backfill.until` | `datetime \| None` | From `BACKFILL_UNTIL` |


## Example using the decorator

```python
from bollhav.pipe import PipeConfig, with_pipe_config

@with_pipe_config
def main(pipe: PipeConfig) -> None:
    do stuff
```
