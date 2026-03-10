[README.md](README.md)

# Environment

Standardizes code at the **pipeline level**!

## Environment Variables

| Variable | Type | Required | Description |
|---|---|---|---|
| `TAGS` | string | yes | Tag expression to filter models |
| `SCHEMA_SUFFIX` | string | yes | Suffix appended to schema name in non-production |
| `PRODUCTION` | bool | no | Disables schema suffix, defaults to `false` |
| `DEBUG` | bool | no | Enables timestamped debug prints |
| `CRON_ENABLED` | bool | no | Enables cron-based interval resolution |
| `CRON_EXPRESSION` | cron string | if cron enabled | Cron expression, e.g. `0 * * * *` |
| `BACKFILL_ENABLED` | bool | no | Enables manual backfill interval |
| `BACKFILL_SINCE` | ISO 8601 | if backfill enabled | Start of backfill window |
| `BACKFILL_UNTIL` | ISO 8601 | if backfill enabled | End of backfill window |

`CRON_ENABLED` and `BACKFILL_ENABLED` cannot both be true.

## Schema Suffix

Non-production runs must set `SCHEMA_SUFFIX` to a non-empty value. Production runs must leave it empty (or it will be overwritten to `""`).

```
PRODUCTION=false  SCHEMA_SUFFIX=erik20260310  →  schema_intelligence_raw_sos_erik20260310
PRODUCTION=true   SCHEMA_SUFFIX=             →  schema_intelligence_raw_sos
```

## Usage

```python
from roskarl.marshal import with_env_config
from bollhav.environment import EnvConfig

@with_env_config
def main(env: EnvConfig) -> None:
    env.debugprint("starting...")
    print(env.backfill.since)
```
