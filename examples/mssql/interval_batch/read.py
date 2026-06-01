"""Source read — the half of chunking the framework does NOT own.

`read(model, since, until)` is handed one interval window. It is the
read function's job to:

  1. honor the window — produce only the rows for `[since, until)`, and
  2. honor `model.batching.size` — yield those rows in size-row frames.

The framework slices time (the intervals); this function slices rows.
Neither `stage()` nor the runner imposes a row-chunk size — the size
lives on the model and the read function reads it. That's the
"honored by the read helpers" contract: go through a helper that reads
`model.batching.size` and the batching is correct for free.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Generator

import polars as pl

from bollhav.model import Model

ROWS_PER_DAY = 12_000


def _rows_for_interval(since: datetime, until: datetime) -> list[dict]:
    """All rows for one day. `event_id` is derived from the day so reruns
    and multi-day runs don't collide on the BIGINT primary key."""
    span = (until - since).total_seconds()
    base_id = int(since.timestamp())  # unique per-day offset
    step = span / ROWS_PER_DAY
    # DATETIME2 has no offset — feed naive timestamps to match the
    # tz-naive polars Datetime column below.
    start = since.replace(tzinfo=None)
    return [
        {
            "event_id": base_id + i,
            "event_time": start + timedelta(seconds=step * i),
            "payload": f"event-{base_id + i}",
        }
        for i in range(ROWS_PER_DAY)
    ]


def read(
    model: Model, since: datetime, until: datetime
) -> Generator[pl.DataFrame, None, None]:
    size = model.batching.size
    rows = _rows_for_interval(since, until)
    for start in range(0, len(rows), size):
        chunk = rows[start : start + size]
        print(f"      read chunk: {len(chunk):>5} rows  (size={size})", flush=True)
        yield pl.DataFrame(
            chunk,
            schema={
                "event_id": pl.Int64,
                "event_time": pl.Datetime(time_unit="us"),
                "payload": pl.Utf8,
            },
        )
