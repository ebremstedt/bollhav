"""Tiny deterministic mock data per model, keyed on the table name.

`read(model, interval)` yields `polars.DataFrame` chunks for the model's
unit of work — bollhav's `write()` consumes the generator directly. The
`customers` view never calls this (it has nothing to write).
"""

from __future__ import annotations

import random
from datetime import timezone
from typing import Generator

import polars as pl

from bollhav.model import ModelRun


def read(run: ModelRun, interval) -> Generator[pl.DataFrame, None, None]:
    name = run.model.target.name
    if name == "orders":
        yield from _orders(interval)
    elif name == "app_config":
        yield from _app_config()
    elif name == "daily_summary":
        yield from _daily_summary(interval)
    else:  # pragma: no cover — every non-view model above has a branch
        raise ValueError(f"no mock read for {name!r}")


def _orders(interval) -> Generator[pl.DataFrame, None, None]:
    """~12 orders in the interval window, in two chunks (shows sub-batched
    staging)."""
    since = interval.since.astimezone(timezone.utc)
    rng = random.Random(int(since.timestamp()))
    base_id = int(since.timestamp())
    rows = [
        {
            "id": base_id + i,
            "customer_id": rng.randint(1, 20),
            "total": round(rng.uniform(5.0, 500.0), 2),
            "order_date": since,
        }
        for i in range(12)
    ]
    schema = {
        "id": pl.Int64,
        "customer_id": pl.Int64,
        "total": pl.Float64,
        "order_date": pl.Datetime("us", "UTC"),
    }
    yield pl.DataFrame(rows[:6], schema=schema)
    yield pl.DataFrame(rows[6:], schema=schema)


def _app_config() -> Generator[pl.DataFrame, None, None]:
    """The whole config table, loaded once (monolith)."""
    yield pl.DataFrame(
        [
            {"key": "retention_days", "value": "30"},
            {"key": "currency", "value": "USD"},
            {"key": "region", "value": "eu"},
        ],
        schema={"key": pl.Utf8, "value": pl.Utf8},
    )


def _daily_summary(interval) -> Generator[pl.DataFrame, None, None]:
    """One summary row per interval (a real pipeline would aggregate the
    upstreams; the contracts guarantee they're ready)."""
    since = interval.since.astimezone(timezone.utc)
    rng = random.Random(int(since.timestamp()) ^ 0x5EED)
    yield pl.DataFrame(
        [
            {
                "day": since,
                "order_count": rng.randint(8, 12),
                "total": round(rng.uniform(100.0, 2000.0), 2),
            }
        ],
        schema={
            "day": pl.Datetime("us", "UTC"),
            "order_count": pl.Int64,
            "total": pl.Float64,
        },
    )
