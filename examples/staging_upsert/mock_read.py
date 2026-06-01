"""Mock data generator for the staging_upsert example.

Each interval represents one day of order updates. Within a single
interval, the SAME order id appears multiple times with the status
progressing: pending → processing → shipped → delivered. This models
a real CDC stream where you see every state transition.

For upsert-on-staging to be interesting, chunks deliberately:

  1. Carry duplicates WITHIN a single chunk (e.g. chunk emits order
     #1 as pending and as processing — staging.write_mode=UPSERT
     collapses these to the latest).
  2. Carry duplicates ACROSS chunks (e.g. chunk 1 sees order #1
     as pending, chunk 2 sees the same id as shipped).

With `staging.write_mode=UPSERT_NO_DELETE`, the staging table only
ever holds one row per order id — the most-recent status. With
`target.write_mode=UPSERT_NO_DELETE`, the final apply MERGEs that
deduped staging into the target.

If you flipped `staging.write_mode` back to APPEND, staging would
accumulate ALL versions of every order. The target MERGE would still
run, but with N times as many rows scanned — and behavior on duplicate
source rows for the same key is implementation-defined (in Postgres
INSERT ... ON CONFLICT, the LAST row wins arbitrarily; this is a
correctness footgun in some MERGE dialects).
"""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta, timezone
from typing import Generator

import polars as pl


ORDERS_PER_INTERVAL = int(os.environ.get("ORDERS_PER_INTERVAL", "300"))
UPDATES_PER_ORDER = int(os.environ.get("UPDATES_PER_ORDER", "4"))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "200"))


STATUS_PROGRESSION = ["pending", "processing", "shipped", "delivered"]


def read(since: datetime, until: datetime) -> Generator[pl.DataFrame, None, None]:
    """Generate a CDC-style stream for the (since, until) interval.

    Each order id gets `UPDATES_PER_ORDER` rows showing status
    progression. All rows for the interval are interleaved randomly
    and emitted in `CHUNK_SIZE` chunks. The same order id can — and
    will — appear in multiple chunks, and sometimes multiple times
    within the same chunk.
    """
    since = since.astimezone(timezone.utc)
    until = until.astimezone(timezone.utc)

    fail_on = os.environ.get("FAIL_ON")
    if fail_on and since.date().isoformat() == fail_on:
        raise RuntimeError(
            f"mock_read: simulated failure for interval starting {fail_on}"
        )

    rng = random.Random(int(since.timestamp()))
    interval_span = (until - since).total_seconds()
    base_id = int(since.timestamp())

    # Build all rows for the interval first, with each order id appearing
    # UPDATES_PER_ORDER times. The status field walks the progression
    # in order, but the rows are then shuffled — so a single chunk may
    # see (id=1, pending) and (id=1, shipped) in any order.
    rows = []
    for order_idx in range(ORDERS_PER_INTERVAL):
        order_id = base_id + order_idx
        customer_id = rng.randint(1, 200)
        total = round(rng.uniform(5.0, 500.0), 2)
        # `t` increases monotonically per update so the LATEST row by
        # `updated_at` wins when the upserts collapse.
        for step in range(UPDATES_PER_ORDER):
            offset = (order_idx * UPDATES_PER_ORDER + step) / (
                ORDERS_PER_INTERVAL * UPDATES_PER_ORDER
            )
            updated_at = since + timedelta(seconds=offset * interval_span)
            rows.append(
                {
                    "id": order_id,
                    "customer_id": customer_id,
                    "total": total,
                    "status": STATUS_PROGRESSION[step % len(STATUS_PROGRESSION)],
                    "updated_at": updated_at,
                }
            )

    rng.shuffle(rows)

    # Yield in CHUNK_SIZE chunks.
    for start in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[start : start + CHUNK_SIZE]
        yield pl.DataFrame(
            chunk,
            schema={
                "id": pl.Int64,
                "customer_id": pl.Int64,
                "total": pl.Float64,
                "status": pl.Utf8,
                "updated_at": pl.Datetime("us", "UTC"),
            },
        )
