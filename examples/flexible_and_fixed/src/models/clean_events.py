"""clean_events — a FLEXIBLE interval table.

`fixed_intervals=False` is the attestation: this model's output for a time
range is **invariant to how that range is partitioned**. That's true here —
it's a pure window-local clean (filter + reshape of `raw_events` rows in the
window), with no aggregation. Two requirements come with the attestation:

  1. window-decomposable query — yes (per-row filter/map), and
  2. an **idempotent write** — so re-covering a range can't duplicate. Hence
     `WriteMode.UPSERT_NO_DELETE`, keyed on `id`. (`APPEND` would duplicate on
     re-cover — never pair it with `fixed_intervals=False`.)

It gates `raw_events` with `ENCAPSULATE` (coverage), which works against any
fixed-or-flexible upstream. Being flexible itself, *its* exact-grain rows are
not durable — so a downstream must use `ENCAPSULATE`, not `EXACT`
(see `daily_report`, and the README's forbidden-combo note).
"""

from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    Contract,
    Database,
    Model,
    Source,
    SourceModel,
    Staging,
    State,
    Tags,
    Target,
    Temporality,
    TimeChunking,
    UpstreamContract,
    WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType


clean_events = Model(
    target=Target(
        name="clean_events",
        schema="lake",
        catalog="demo",
        database=Database.POSTGRES,
        write_mode=WriteMode.UPSERT_NO_DELETE,  # idempotent — required for flexible
        dsn_env_var="TARGET_DSN",
        staging=Staging(),
        columns=[
            PostgresColumn(
                name="id",
                data_type=PostgresType.BIGINT,
                nullable=False,
                primary_key=True,  # identity + merge key for the idempotent upsert
            ),
            PostgresColumn(
                name="device_id", data_type=PostgresType.BIGINT, nullable=False
            ),
            PostgresColumn(
                name="value", data_type=PostgresType.NUMERIC, nullable=False
            ),
            PostgresColumn(
                name="event_time", data_type=PostgresType.TIMESTAMPTZ, nullable=False
            ),
        ],
    ),
    temporality=Temporality.TEMPORAL,
    state=State(),
    # The attestation: state is a coverage set, not a grid. Re-chunk freely.
    batching=Batch(time=TimeChunking(chunk="@daily", fixed_intervals=False)),
    contract=Contract(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    upstream=[
        Source(
            "demo.lake.raw_events",
            type=SourceModel(),
            contract=UpstreamContract.ENCAPSULATE,  # coverage of the window
        ),
    ],
    tagging=Tags(tags={"demo"}),
)
