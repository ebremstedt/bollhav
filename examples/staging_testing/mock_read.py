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

import os
import random
from datetime import datetime, timedelta, timezone
from typing import Generator

import polars as pl


ROWS_PER_INTERVAL = int(os.environ.get("ROWS_PER_INTERVAL", "500"))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "200"))


def read(since: datetime, until: datetime) -> Generator[pl.DataFrame, None, None]:
    """Generate `ROWS_PER_INTERVAL` deterministic-ish rows for the
    `(since, until)` window, yielded in `CHUNK_SIZE` chunks.

    Set `FAIL_ON=YYYY-MM-DD` to make the read raise for an interval
    starting on that day — used to exercise the error path."""
    # Normalize to UTC so date math doesn't land on DST transitions
    # (psycopg returns timestamps in the session's local timezone,
    # which can be Stockholm or similar where 2024-03-31 02:30 doesn't
    # exist due to spring-forward).
    since = since.astimezone(timezone.utc)
    until = until.astimezone(timezone.utc)

    fail_on = os.environ.get("FAIL_ON")
    if fail_on and since.date().isoformat() == fail_on:
        raise RuntimeError(
            f"mock_read: simulated failure for interval starting {fail_on}"
        )

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
