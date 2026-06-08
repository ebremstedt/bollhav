"""A fully-declared model — every kind of input in one `upstream` list.

Illustrative only: shows the shape of `upstream: list[Source]`. It is NOT
wired into the runnable `staging_state_contracts` demo (that's why it lives
in the examples root rather than under `src/models/`, which is auto-loaded).

A model has ONE inputs list, `upstream`. Each entry is a `Source` with two
independent dials:

  type      — WHAT it is, and its read config:
                • SourceModel — relational (a managed model, an external
                  table, or a view via query=)
                • SourceFile  — a file
                • SourceApi   — an HTTP API
  contract  — whether it GATES. A Source carrying a contract is a managed
              upstream the state machine waits for (`applied` before this
              model runs). No contract ⇒ ungated, assumed always present.
              A contract is only valid on a SourceModel.

`model.ref(name)` resolves any SourceModel input into a `FROM` — suffix-aware
when it's gated (moves across dev / prod / PR), literal when it isn't (an
external table at a fixed location). Files and APIs aren't SQL-addressable.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Generator

import polars as pl

from bollhav.model import (
    Batch,
    Bounds,
    Database,
    IntervalChunks,
    IntervalContract,
    Kind,
    Model,
    Source,
    SourceApi,
    SourceModel,
    State,
    Tags,
    Target,
    ViewContract,
    WriteMode,
)
from bollhav.postgres import PostgresColumn, PostgresType


# fmt: off
order_enriched = Model(
    target=Target(
        name="order_enriched",
        schema="warehouse",
        catalog="demo",  # required on database-backed models (full identity)
        database=Database.POSTGRES,
        write_mode=WriteMode.APPEND,
        dsn_env_var="TARGET_DSN",
        columns=[
            PostgresColumn(name="id", data_type=PostgresType.BIGINT, nullable=False),
            PostgresColumn(name="customer_id", data_type=PostgresType.BIGINT, nullable=False),
            PostgresColumn(name="customer_name", data_type=PostgresType.TEXT, nullable=False),
            PostgresColumn(name="total", data_type=PostgresType.NUMERIC, nullable=False),
            PostgresColumn(name="order_date", data_type=PostgresType.TIMESTAMPTZ, nullable=False),
        ],
    ),
    kind=Kind.INTERVAL,
    state=State(),
    batching=Batch(interval=IntervalChunks(expression="@daily")),
    bounds=Bounds(
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 4, tzinfo=timezone.utc),
    ),
    upstream=[
        Source("demo.warehouse.orders", type=SourceModel(), contract=IntervalContract()),  # gated upstream (interval)
        Source("demo.warehouse.customers", type=SourceModel(), contract=ViewContract()),    # gated upstream (view)
        Source("raw.orders_export", type=SourceModel(dsn_env_var="RAW_DSN")),                # ungated source (read input)
        Source("crm.contacts_api", type=SourceApi(base_url="https://crm/api")),              # ungated source (API, lineage only)
    ],
    tagging=Tags(tags={"demo"}),
)
# fmt: on


def read(model: Model, interval) -> Generator[pl.DataFrame, None, None]:
    """Join a gated upstream against an ungated source — to show how `ref()`
    resolves each:

        model.ref("demo.warehouse.orders")  -> gated   -> suffix-aware, catalog
                                               dropped ("warehouse_pr12"."orders")
        model.ref("raw.orders_export")       -> ungated -> literal
                                               ("raw"."orders_export")
    """
    query = f"""
        SELECT
            o.id,
            o.customer_id,
            c.name AS customer_name,
            o.total,
            o.order_date
        FROM {model.ref("raw.orders_export")} o              -- ungated source (literal)
        JOIN {model.ref("demo.warehouse.customers")} c       -- gated upstream (suffixed)
            ON c.customer_id = o.customer_id
        JOIN {model.ref("demo.warehouse.orders")} k          -- gated upstream (suffixed)
            ON k.id = o.id
        WHERE o.order_date >= %(since)s
          AND o.order_date <  %(until)s
    """
    # A real read() would execute `query` against RAW_DSN and yield chunks;
    # this illustration just shows the resolved identifiers.
    print(query)
    yield pl.DataFrame()
