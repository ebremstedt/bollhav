"""Tiny deterministic mock data for the `events` model.

`read(run, interval)` yields `polars.DataFrame` chunks for the interval's
window — bollhav's `write()` consumes the generator directly.
"""

from __future__ import annotations

import random
from datetime import timezone
from typing import Generator

import polars as pl

from bollhav.model import ModelRun


def read(run: ModelRun, interval) -> Generator[pl.DataFrame, None, None]:
    """~10 events stamped inside the interval window (direct APPEND)."""
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
    yield pl.DataFrame(rows, schema=schema)
