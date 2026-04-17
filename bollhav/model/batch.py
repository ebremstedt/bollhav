import logging
from dataclasses import dataclass
from datetime import datetime, timezone, tzinfo

from icron import croniter
from bollhav.model.intervals import TZInterval
from roskarl import BatchExpression, BatchExpressionExtended

logger = logging.getLogger(__name__)

_CRON_ALIASES = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}


def _resolve_cron(expr: str) -> str:
    return _CRON_ALIASES.get(expr, expr)


def _resolve_cron_interval(
    expression: str, tz: tzinfo = timezone.utc
) -> tuple[datetime, datetime]:
    now = datetime.now(tz=tz)
    probe = croniter(expression, now)
    tick1 = probe.get_next(datetime)
    tick2 = probe.get_next(datetime)
    interval_size = tick2 - tick1
    it = croniter(expression, now - (interval_size * 3))
    prev, curr = None, None
    while True:
        tick = it.get_next(datetime)
        if tick >= now:
            break
        prev, curr = curr, tick
    return prev, curr


def _chunk_interval(cron: str, incoming_interval: TZInterval) -> list[TZInterval]:
    it = croniter(cron, incoming_interval.since)
    outgoing_intervals: list[TZInterval] = []
    current = incoming_interval.since
    while True:
        tick = it.get_next(datetime)
        if tick >= incoming_interval.until:
            break
        outgoing_intervals.append(TZInterval(current, tick))
        current = tick
    if current < incoming_interval.until:
        outgoing_intervals.append(TZInterval(current, incoming_interval.until))
    return outgoing_intervals


@dataclass
class Batch:
    """
    Controls how a time interval is split into smaller chunks for processing.

    `batch_expression` is a cron expression that defines the chunk size. For example,
    "0 * * * *" means each chunk is one hour, "0 0 * * *" means one day.
    Can be overridden at runtime via the pipe's batch expression.

    `window_expression` is a cron expression that defines the scope to catch up on
    in `latest` mode — i.e. "one of what" counts as the latest complete unit.
    Defaults to `batch_expression` when unset (so one chunk = one window, the
    original behaviour). Only consulted in `latest` mode — for reload/backfill
    since/until are explicit and the window is irrelevant. Can be overridden at
    runtime via the pipe's window expression.

        window_expression = the OUTER scope   ("one full DAY")
        batch_expression  = the INNER chunks  ("in 15-min WRITES")

        assume now = 2024-06-15 14:35 UTC in all three cases

        window="@daily", batch="*/15 * * * *"
          Jun 14 00:00                                    Jun 15 00:00
          ┌───────────────────── yesterday ─────────────────────┐
          │░│░│░│░│░│░│░│░│░│░│░│░│░│░│░│░│░│░│░│░│ ...  │░│░│░│
          └─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴──────┴─┴─┴─┘
           00:00 00:15 00:30 00:45 01:00 01:15      23:15 23:30 23:45
           → 96 × 15-min chunks covering Jun 14 00:00 → Jun 15 00:00

        window="@daily", batch="@hourly"
          Jun 14 00:00                                    Jun 15 00:00
          ┌───────────────────── yesterday ─────────────────────┐
          │     │     │     │     │     │     │ ... │     │     │
          └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘
           00:00 01:00 02:00 03:00 04:00 05:00       22:00 23:00
           → 24 × hourly chunks covering Jun 14 00:00 → Jun 15 00:00

        window unset (== batch), batch="@hourly"
          13:00                        14:00   [now 14:35]  15:00
          ┌──── last complete hour ────┐
          │░░░░░░░░░░░░░░░░░░░░░░░░░░░│
          └───────────────────────────┘
           → 1 × hourly chunk covering 13:00 → 14:00 (pre-window default)

    `tz` is the timezone used for interval resolution. Defaults to UTC.
    Can be overridden at runtime via `TIMEZONE_OVERRIDE`.

    `lookback` extends the start of the interval backwards by N cron-ticks.
    If the cron is hourly and lookback is 3, processing starts 3 hours before
    the interval's natural start. Useful for reprocessing recent history to
    account for late-arriving data.

    `retries` is the number of times a failed chunk should be retried.
    """

    batch_expression: BatchExpression | BatchExpressionExtended = "@daily"
    window_expression: BatchExpression | BatchExpressionExtended | None = None
    tz: tzinfo = timezone.utc
    lookback: int | None = None
    retries: int | None = None
