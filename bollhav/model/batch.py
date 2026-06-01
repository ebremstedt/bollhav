import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, tzinfo

from icron import croniter
from bollhav.model.intervals import TZInterval
from roskarl import IntervalExpression, IntervalExpressionExtended
from roskarl.cron import INTERVAL_EXPRESSION_SHORTCUTS

logger = logging.getLogger(__name__)

_CRON_ALIASES = INTERVAL_EXPRESSION_SHORTCUTS

MAX_BATCH_SIZE = 10000


def validate_batch_size(batch_size: int, source: str) -> None:
    """Raise if `batch_size` exceeds the hard cap. `source` names where the
    value came from for the error message (e.g. 'Batch.size')."""
    if batch_size > MAX_BATCH_SIZE:
        raise ValueError(
            f"{source} batch_size={batch_size} exceeds max {MAX_BATCH_SIZE}"
        )


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
    # Loop invariant: cron is seeded `interval_size * 3` before now,
    # so at least 2 ticks have been consumed before the break and both
    # `prev` and `curr` are populated.
    assert prev is not None and curr is not None
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
class IntervalChunks:
    """
    Time-interval chunking config.

    `expression` is a cron expression that defines the chunk size. For example,
    "0 * * * *" means each chunk is one hour, "0 0 * * *" means one day.
    Can be overridden at runtime via the pipe's batch expression env var.

    `window_expression` defines the scope to catch up on in `latest` mode —
    i.e. "one of what" counts as the latest complete unit. Defaults to
    `expression` when unset. Only consulted in `latest` mode.

    `tz` is the timezone used for interval resolution. Defaults to UTC.

    `lookback` extends the start of the interval backwards by N cron-ticks,
    useful for reprocessing recent history to account for late-arriving data.
    """

    expression: IntervalExpression | IntervalExpressionExtended = "@daily"
    window_expression: IntervalExpression | IntervalExpressionExtended | None = None
    tz: tzinfo = timezone.utc
    lookback: int | None = None


@dataclass
class Batch:
    """
    Controls how a model's work is chunked

    `interval` holds the time-interval chunking config — the cron
    expression whose ticks define the `(since, until)` windows the model
    iterates. Always present.

    `size` is the number of rows per read chunk, capped at
    `MAX_BATCH_SIZE` (10000). The framework hands `(since, until)` to the
    user's read function; the row-level sub-batching within an interval
    is honored by the read helpers, which slice the source by `size`.

    `retries` is the number of times a failed chunk should be retried.
    """

    interval: IntervalChunks = field(default_factory=IntervalChunks)
    size: int = 10000
    retries: int | None = None

    def __post_init__(self) -> None:
        validate_batch_size(self.size, "Batch.size")
