"""Mock data generator — same shape as `staging_testing/mock_read.py`.

Generates synthetic rows in chunked DataFrames for the (since, until)
window. Used by both `orders` and `high_value_sums` — they share the
column layout to keep this example tight.
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
    """Yield `ROWS_PER_INTERVAL` deterministic-ish rows for the window
    `[since, until)`, split into `CHUNK_SIZE` chunks."""
    since = since.astimezone(timezone.utc)
    until = until.astimezone(timezone.utc)

    rng = random.Random(int(since.timestamp()))
    interval_span = (until - since).total_seconds()
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
