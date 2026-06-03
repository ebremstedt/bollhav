"""daily_summary — an INTERVAL table (staging + state) that declares
upstream **contracts** on all three kinds.

This is the point of the example. Its `upstream` is a list of contracts,
each checked by the upstream's shape before an interval is allowed to run:

  * IntervalContract("warehouse.orders")    — satisfied when orders has an
                                              applied state row covering this
                                              summary interval's window.
  * ViewContract("warehouse.customers")     — satisfied when the customers
                                              view exists (its row is applied).
  * MonolithicContract("warehouse.app_config")
                                            — satisfied when app_config has
                                              been loaded (its row is applied).

At each interval `@execute_lifecycle` checks every contract. If any is
unsatisfied the interval is marked `blocked` (with a reason naming each
missing upstream) and skipped; when all are satisfied, it runs. A declared
contract whose upstream was never registered is a hard error, not a skip.
"""

from datetime import datetime, timezone

from bollhav.model import (
    Batch,
    Bounds,
    Database,
    IntervalChunks,
    IntervalContract,
    Model,
    MonolithicContract,
    Staging,
    State,
    Tags,
    Target,
    TargetSchema,
    ViewContract,
    WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType


daily_summary = Model(
    target=Target(
        name="daily_summary",
        schema=TargetSchema(name="warehouse"),
        database=Database.POSTGRES,
        write_mode=WriteMode.APPEND,
        dsn_env_var="TARGET_DSN",
        staging=Staging(),
        columns=[
            PostgresColumn(
                name="day", data_type=PostgresType.TIMESTAMPTZ, nullable=False
            ),
            PostgresColumn(
                name="order_count", data_type=PostgresType.BIGINT, nullable=False
            ),
            PostgresColumn(
                name="total", data_type=PostgresType.NUMERIC, nullable=False
            ),
        ],
    ),
    state=State(),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    upstream=[
        IntervalContract("warehouse.orders"),
        ViewContract("warehouse.customers"),
        MonolithicContract("warehouse.app_config"),
    ],
    tagging=Tags(tags={"demo"}),
)
