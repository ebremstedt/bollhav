"""Source read — yields one interval's rows in `batching.size` chunks.

The framework slices time (the daily intervals); this function slices
rows. Each yielded frame is bulk-inserted into the staging table by
`@execute_lifecycle`; once the generator drains, the hook MERGEs staging
into the target and drops it.

`sale_id` is derived from the day so multi-day runs and reruns don't
collide on the BIGINT primary key (and so a rerun MERGEs the same keys
rather than inserting duplicates).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Generator

import polars as pl

from bollhav.model import Model

ROWS_PER_DAY = 5_000


def _rows_for_interval(since: datetime, until: datetime) -> list[dict]:
    span = (until - since).total_seconds()
    base_id = int(since.timestamp())  # unique per-day offset
    step = span / ROWS_PER_DAY
    start = since.replace(tzinfo=None)  # DATETIME2 is offset-naive
    return [
        {
            "sale_id": base_id + i,
            "sold_at": start + timedelta(seconds=step * i),
            "amount": round(10 + (i % 100) * 1.5, 2),
        }
        for i in range(ROWS_PER_DAY)
    ]


def read(model: Model, interval) -> Generator[pl.DataFrame, None, None]:
    size = model.batching.size
    rows = _rows_for_interval(interval.since, interval.until)
    for start in range(0, len(rows), size):
        chunk = rows[start : start + size]
        print(f"      read chunk: {len(chunk):>5} rows  (size={size})", flush=True)
        yield pl.DataFrame(
            chunk,
            schema={
                "sale_id": pl.Int64,
                "sold_at": pl.Datetime(time_unit="us"),
                "amount": pl.Float64,
            },
        )
