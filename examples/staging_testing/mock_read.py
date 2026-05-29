"""Mock data generator for the staging-testing example.

Yields a `Generator[pl.DataFrame, None, None]` — bollhav's `write()`
consumes generators directly, and the staged path COPYs each yielded
chunk straight into the staging table without holding it all in memory.

For each (since, until) interval, we generate ~5000 rows split into
2000-row chunks (so 3 chunks per interval, last one partial). This
demonstrates sub-batched writes within a single interval — the whole
point of staging.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Generator

import polars as pl


ROWS_PER_INTERVAL = 5000
CHUNK_SIZE = 2000


def read(since: datetime, until: datetime) -> Generator[pl.DataFrame, None, None]:
    """Generate `ROWS_PER_INTERVAL` deterministic-ish rows for the
    `(since, until)` window, yielded in `CHUNK_SIZE` chunks."""
    rng = random.Random(int(since.timestamp()))  # stable per-interval
    interval_span = (until - since).total_seconds()

    # Use the interval start as a numeric offset to keep ids unique
    # across intervals (avoid PK collisions when APPENDing).
    base_id = int(since.timestamp())

    rows_remaining = ROWS_PER_INTERVAL
    chunk_index = 0
    while rows_remaining > 0:
        n = min(CHUNK_SIZE, rows_remaining)
        rows = [
            {
                "id": base_id + chunk_index * CHUNK_SIZE + i,
                "customer_id": rng.randint(1, 500),
                "total": round(rng.uniform(5.0, 500.0), 2),
                "order_date": since + timedelta(seconds=rng.uniform(0, interval_span)),
            }
            for i in range(n)
        ]
        yield pl.DataFrame(
            rows,
            schema={
                "id": pl.Int64,
                "customer_id": pl.Int64,
                "total": pl.Float64,
                "order_date": pl.Datetime("us", "UTC"),
            },
        )
        rows_remaining -= n
        chunk_index += 1
