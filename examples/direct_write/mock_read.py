"""Tiny deterministic mock data per model, keyed on the table name.

`read(run, interval)` yields `polars.DataFrame` chunks for the model's unit of
work — bollhav's `write()` consumes the generator directly. Rows are stamped
inside the interval's window so RECREATE_PARTITION's DELETE bounds line up.
"""

from __future__ import annotations

import random
from datetime import timezone
from typing import Generator

import polars as pl

from bollhav.model import ModelRun


def read(run: ModelRun, interval) -> Generator[pl.DataFrame, None, None]:
    name = run.model.target.name
    if name == "events":
        yield from _events(interval)
    elif name == "metrics":
        yield from _metrics(interval)
    else:  # pragma: no cover
        raise ValueError(f"no mock read for {name!r}")


def _events(interval) -> Generator[pl.DataFrame, None, None]:
    """~10 events in the interval window, in two chunks (direct APPEND)."""
    since = interval.since.astimezone(timezone.utc)
    rng = random.Random(int(since.timestamp()))
    base = int(since.timestamp())
    rows = [
        {
            "id": base + i,
            "kind": rng.choice(["click", "view", "purchase"]),
            "event_time": since,
        }
        for i in range(10)
    ]
    schema = {"id": pl.Int64, "kind": pl.Utf8, "event_time": pl.Datetime("us", "UTC")}
    yield pl.DataFrame(rows[:5], schema=schema)
    yield pl.DataFrame(rows[5:], schema=schema)


def _metrics(interval) -> Generator[pl.DataFrame, None, None]:
    """A few metric rows stamped inside the window (direct RECREATE_PARTITION)."""
    since = interval.since.astimezone(timezone.utc)
    rng = random.Random(int(since.timestamp()) ^ 0x5EED)
    yield pl.DataFrame(
        [
            {"metric_time": since, "name": n, "value": round(rng.uniform(1, 100), 2)}
            for n in ("rows_in", "rows_out", "errors")
        ],
        schema={
            "metric_time": pl.Datetime("us", "UTC"),
            "name": pl.Utf8,
            "value": pl.Float64,
        },
    )
