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
| **LATEST_CRONBATCH** | CronBatch | CronBatch expression to chunk the data into intervals **0 * * * *** |



## Backfill
Required if BACKFILL_ENABLED=True
| Variable | Type | Description |
|---|---|---|
| **BACKFILL_SINCE** | ISO 8601 datetime | Start of backfill window |
| **BACKFILL_UNTIL** | ISO 8601 datetime | End of backfill window |
| **BACKFILL_CRONBATCH** | CronBatch | CronBatch expression to chunk the data into intervals **0 * * * *** |




## Example using the decorator

```python
from bollhav.pipe import PipeConfig, with_pipe_config

@with_pipe_config
def main(pipe: PipeConfig) -> None:
    do stuff
```
