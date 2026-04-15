import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo

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


def _chunk_interval(cron: str, since: datetime, until: datetime) -> list[TZInterval]:
    it = croniter(cron, since)
    intervals: list[TZInterval] = []
    current = since
    while True:
        tick = it.get_next(datetime)
        if tick >= until:
            break
        intervals.append(TZInterval(current, tick))
        current = tick
    if current < until:
        intervals.append(TZInterval(current, until))
    return intervals


@dataclass
class Batch:
    """
    Controls how a time interval is split into smaller chunks for processing.

    `batch_expression` is a cron expression that defines the chunk size. For example,
    "0 * * * *" means each chunk is one hour, "0 0 * * *" means one day.
    Can be overridden at runtime via the pipe's batch expression.

    `tz` is the timezone used for interval resolution. Defaults to UTC.
    Can be overridden at runtime via `TIMEZONE_OVERRIDE`.

    `lookback` extends the start of the interval backwards by N cron-ticks.
    If the cron is hourly and lookback is 3, processing starts 3 hours before
    the interval's natural start. Useful for reprocessing recent history to
    account for late-arriving data.

    `retries` is the number of times a failed chunk should be retried.
    """

    batch_expression: BatchExpression | BatchExpressionExtended = "@daily"
    tz: tzinfo = timezone.utc
    lookback: int | None = None
    retries: int | None = None
