# Every feature, one model

One model that uses everything a model can declare: a staged, keyed, upserted
Postgres table with four columns, sliced into days on a Stockholm clock, with
state, two gated upstreams, a curfew, extra tags, owners and a Teams channel.
Each field links to its own page from the [model overview](../model/README.md).

```python title="models.py"
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from bollhav.channels import MicrosoftTeams
from bollhav.model import (
    Batch,
    ChunkFlex,
    Contact,
    Contract,
    Curfew,
    Database,
    Detail,
    Freshness,
    FreshnessScope,
    Mention,
    Model,
    Notifications,
    People,
    Source,
    SourceModel,
    State,
    StateMode,
    Tags,
    Target,
    Temporality,
    TimeChunking,
    UpstreamContract,
    WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.postgres.staging import PostgresStaging

STOCKHOLM = ZoneInfo("Europe/Stockholm")

daily_orders = Model(
    description="One row per customer and day: how many orders, and what they came to.",
    target=Target(
        name="daily_orders",
        schema="marts",
        catalog="shop",
        database=Database.POSTGRES,
        write_mode=WriteMode.UPSERT_NO_DELETE,
        staging=PostgresStaging(),
        columns=[
            PostgresColumn("day", data_type=PostgresType.DATE, nullable=False, primary_key=True, description="Stockholm time"),
            PostgresColumn("customer_id", data_type=PostgresType.BIGINT, nullable=False, primary_key=True),
            PostgresColumn("order_count", data_type=PostgresType.INTEGER, nullable=False),
            PostgresColumn("total", data_type=PostgresType.NUMERIC, nullable=False, precision=12, scale=2),
        ],
    ),
    temporality=Temporality.TEMPORAL,
    contract=Contract(begin=datetime(2024, 1, 1, tzinfo=STOCKHOLM)),
    batching=Batch(
        time=TimeChunking(
            chunk="@daily",
            tz=STOCKHOLM,
            lookback=2,
        ),
        size=50_000,
        retries=2,
    ),
    state=State(mode=StateMode.DISCOVER, allow_concurrent_runs=False),
    curfew=Curfew(windows=[(time(2), time(4))], tz=STOCKHOLM),
    tagging=Tags(tags={"finance", "daily"}),
    people=People(
        owners=(Contact("Maja Lindqvist", "maja.lindqvist@example.com"),),
        maintainers=(Contact("Jonas Ek", "jonas.ek@example.com"), Contact("Priya Nair", "priya.nair@example.com")),
        creators=(Contact("Amira Haddad"),),
    ),
    notifications=Notifications(
        on_failure=MicrosoftTeams(webhook_env="TEAMS_WEBHOOK_FINANCE"),
        detail=Detail.MODELS,
        mention=Mention.ON_FAILURE,
    ),
    enabled=True, 
)
```

Constructed, it is `shop.marts.daily_orders`, and its tags come out as `all`,
`daily`, `daily_orders`, `finance`, `marts`, `marts.daily_orders`, `orders`,
`shop` and `shop.marts.daily_orders`: the two declared ones plus everything
[passive tagging](../model/tagging.md) derives from the name.
