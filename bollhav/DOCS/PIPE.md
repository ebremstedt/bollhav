[README.md](..README.md)

# Pipe

Standardizes code at the **pipe** level by preloading the variables below:

## General
| Variable | Type | Required | Description |
|---|---|---|---|
| **TAGS** | string | yes | Tag expression to filter models |
| **SCHEMA_SUFFIX** | string | yes | Suffix appended to schema name in non-production |
| **USE_SCHEMA_SUFFIX** | bool | no | Enables schema suffix, defaults to **True** |
| **DEBUG** | bool | no | Enables timestamped debug prints |
| **LATEST_ENABLED** | bool | no | Enables cron-based interval resolution, cannot be True along with **BACKFILL_ENABLED** |
| **BACKFILL_ENABLED** | bool | no | Enables manual backfill interval, cannot be True along with **LATEST_ENABLED** |


## Latest
Required if LATEST_ENABLED=True

| Variable | Type | Description |
|---|---|---|
| **LATEST_CRON_BATCH** | CronBatch | CronBatch expression to chunk the data into intervals e.g. **0 * * * \*** |


## Backfill
Required if BACKFILL_ENABLED=True

| Variable | Type | Description |
|---|---|---|
| **BACKFILL_SINCE** | ISO 8601 datetime | Start of backfill window |
| **BACKFILL_UNTIL** | ISO 8601 datetime | End of backfill window |
| **BACKFILL_CRON_BATCH** | CronBatch | CronBatch expression to chunk the data into intervals e.g. **0 * * * \*** |


## PipeConfig fields

| Field | Type | Description |
|---|---|---|
| `tags` | `str` | Tag expression from `TAGS` |
| `debug` | `bool` | From `DEBUG` |
| `schema_suffix` | `str` | From `SCHEMA_SUFFIX` |
| `use_schema_suffix` | `bool` | From `USE_SCHEMA_SUFFIX` |
| `latest.enabled` | `bool` | From `LATEST_ENABLED` |
| `latest.cron_batch` | `str \| None` | From `LATEST_CRON_BATCH` |
| `latest.since` | `datetime \| None` | Resolved from `latest.cron_batch` |
| `latest.until` | `datetime \| None` | Resolved from `latest.cron_batch` |
| `backfill.enabled` | `bool` | From `BACKFILL_ENABLED` |
| `backfill.cron_batch` | `str \| None` | From `BACKFILL_CRON_BATCH` |
| `backfill.since` | `datetime \| None` | From `BACKFILL_SINCE` |
| `backfill.until` | `datetime \| None` | From `BACKFILL_UNTIL` |


## Example using the decorator

```python
from bollhav.pipe import PipeConfig, with_pipe_config

@with_pipe_config
def main(pipe: PipeConfig) -> None:
    do stuff
```
