[Home](index.md) › **Examples** › **Model example**

# The model example

A **Model** is a pure data object. It declares *what* your data looks like and
*where* it goes — and deliberately contains **no execution logic**. The work
that fills it lives in a separate `execute` function (see the
[decorator example](EXAMPLES.md)).

## A basic model

The minimal shape: where the data goes and what it looks like. A whole-table
(`TIMELESS`) model — no batching, no state, no upstreams.

```python title="models.py"
from bollhav.model import Database, Model, Target, Temporality, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType


app_config = Model(
    temporality=Temporality.TIMELESS,
    target=Target(
        name="app_config",
        schema="warehouse",
        catalog="demo",
        database=Database.POSTGRES,
        write_mode=WriteMode.APPEND,
        columns=[
            PostgresColumn("key", PostgresType.TEXT, nullable=False),
            PostgresColumn("value", PostgresType.TEXT, nullable=False),
        ],
    ),
)
```

## An advanced model — staging, state, and upstreams

A time-windowed (`TEMPORAL`) table built incrementally. It **stages** each
interval and merges into the target, tracks every unit of work with **state**,
and gates itself on other models with **upstream contracts**. State and
upstreams go hand in hand: the contracts are resolved against the upstreams'
own state rows.

```python title="models.py"
from datetime import datetime, timezone

from bollhav.model import (
    Batch, Contract, Database, Model, Source, SourceModel, Staging, State,
    Tags, Target, Temporality, TimeChunking, UpstreamContract, WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType


daily_summary = Model(
    target=Target(
        name="daily_summary",
        schema="warehouse",
        catalog="demo",
        database=Database.POSTGRES,
        write_mode=WriteMode.APPEND,
        staging=Staging(),
        columns=[
            PostgresColumn("day", PostgresType.TIMESTAMPTZ, nullable=False),
            PostgresColumn("order_count", PostgresType.BIGINT, nullable=False),
            PostgresColumn("total", PostgresType.NUMERIC, nullable=False),
        ],
    ),
    temporality=Temporality.TEMPORAL,
    state=State(),
    batching=Batch(time=TimeChunking(chunk="@daily")),
    contract=Contract(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    upstream=[
        Source("demo.warehouse.orders",     type=SourceModel(), contract=UpstreamContract.WINDOW),
        Source("demo.warehouse.customers",  type=SourceModel(), contract=UpstreamContract.WHOLE),
        Source("demo.warehouse.app_config", type=SourceModel(), contract=UpstreamContract.WHOLE),
    ],
    tagging=Tags(tags={"demo"}),
)
```

Every part is a concept of its own:

| Field | What it declares | |
|---|---|---|
| `target` | where the data goes — table, columns, write mode, staging | [Target](TARGET.md) · [Write modes](MODES.md) |
| `temporality` | time-windowed (`TEMPORAL`) vs whole-table (`TIMELESS`) | [Temporality](TEMPORALITY.md) |
| `state` | track each unit of work (`pending → running → applied`) | [State](STATE.md) |
| `batching` | how the run window is chunked into intervals | [Batch](BATCH.md) |
| `contract` | the bounds of what this model covers | [Contract](CONTRACT.md) |
| `upstream` | gate on other models, each with an `UpstreamContract` level | [Upstream](UPSTREAM.md) |
| `tagging` | tags for `TAGS=` run selection and lineage | [Tagging](TAGGING.md) |

Each upstream contract is checked per unit of work by `@execute_lifecycle`; an
unsatisfied one leaves the unit `blocked` until the upstream catches up. See the
[decorator example](EXAMPLES.md) for how a model is actually run.
