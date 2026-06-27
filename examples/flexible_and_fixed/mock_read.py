"""Tiny deterministic mock data per model, keyed on the table name.

`read(run, interval)` yields `polars.DataFrame` chunks for the model's unit of
work — bollhav's `write()` consumes the generator directly.
"""

from __future__ import annotations

import random
from datetime import timezone
from typing import Generator

import polars as pl

from bollhav.model import ModelRun

_EVENT_SCHEMA = {
    "id": pl.Int64,
    "device_id": pl.Int64,
    "value": pl.Float64,
    "event_time": pl.Datetime("us", "UTC"),
}


def read(run: ModelRun, interval) -> Generator[pl.DataFrame, None, None]:
    name = run.model.target.name
    if name == "raw_events":
        yield from _events(interval)
    elif name == "clean_events":
        # A pure window-local clean: same rows, re-emitted (idempotent upsert).
        yield from _events(interval)
    elif name == "daily_report":
        yield from _daily_report(interval)
    elif name == "audit":
        yield from _audit(interval)
    else:  # pragma: no cover
        raise ValueError(f"no mock read for {name!r}")


def _events(interval) -> Generator[pl.DataFrame, None, None]:
    """~10 events in the interval window, deterministic per day."""
    since = interval.since.astimezone(timezone.utc)
    rng = random.Random(int(since.timestamp()))
    base_id = int(since.timestamp())
    rows = [
        {
            "id": base_id + i,
            "device_id": rng.randint(1, 20),
            "value": round(rng.uniform(0.0, 100.0), 2),
            "event_time": since,
        }
        for i in range(10)
    ]
    yield pl.DataFrame(rows, schema=_EVENT_SCHEMA)


def _daily_report(interval) -> Generator[pl.DataFrame, None, None]:
    """One aggregated row per interval (the contract guarantees clean_events
    covers this window)."""
    since = interval.since.astimezone(timezone.utc)
    rng = random.Random(int(since.timestamp()) ^ 0x5EED)
    yield pl.DataFrame(
        [
            {
                "day": since,
                "event_count": rng.randint(8, 12),
                "total": round(rng.uniform(100.0, 2000.0), 2),
            }
        ],
        schema={
            "day": pl.Datetime("us", "UTC"),
            "event_count": pl.Int64,
            "total": pl.Float64,
        },
    )


def _audit(interval) -> Generator[pl.DataFrame, None, None]:
    """One audit row per interval — stamped once raw_events' exact-grain row
    for this day is applied."""
    since = interval.since.astimezone(timezone.utc)
    yield pl.DataFrame(
        [{"day": since, "checked": True}],
        schema={"day": pl.Datetime("us", "UTC"), "checked": pl.Boolean},
    )
