import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, tzinfo
from enum import Enum

from icron import croniter
from bollhav.model.intervals import TZInterval
from roskarl import IntervalExpression, IntervalExpressionExtended
from roskarl.cron import INTERVAL_EXPRESSION_SHORTCUTS

logger = logging.getLogger(__name__)

_CRON_ALIASES = INTERVAL_EXPRESSION_SHORTCUTS

MAX_BATCH_SIZE = 10000


def validate_batch_size(batch_size: int, source: str) -> None:
    """Raise if `batch_size` exceeds the hard cap. `source` names where the
    value came from for the error message (e.g. 'RowChunks', 'r_row_ tag')."""
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


class ChunkMode(Enum):
    INTERVAL = "INTERVAL"
    ROW = "ROW"


@dataclass
class IntervalChunks:
    """
    Time-interval chunking config — used when `Batch.mode == INTERVAL`.

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
class RowChunks:
    """
    Row-count chunking config — used when `Batch.mode == ROW`.

    `batch_size` is the number of rows per chunk. Capped at
    `MAX_BATCH_SIZE` (10000). Compatible with `WriteMode.APPEND` and
    `WriteMode.UPSERT_NO_DELETE` — other write modes are rejected at
    `Model` construction time because they assume they see the whole
    dataset in one shot.
    """

    batch_size: int = 10000

    def __post_init__(self) -> None:
        validate_batch_size(self.batch_size, "RowChunks")


@dataclass
class Batch:
    """
    Controls how a model's work is chunked.

    `mode` selects between time-interval chunking (INTERVAL, the default)
    and row-count chunking (ROW). A model with `mode=ROW` can only be
    reloaded — latest and backfill runs require INTERVAL mode.

    `interval` holds the time-interval chunking config. Always present;
    read when `mode == INTERVAL` and when reload uses interval chunking.

    `row` holds the row-count chunking config. Always present; read when
    `mode == ROW` or when a tag override forces row-mode reload.

    `retries` is the number of times a failed chunk should be retried.
    Applies to both modes.
    """

    mode: ChunkMode = ChunkMode.INTERVAL
    interval: IntervalChunks = field(default_factory=IntervalChunks)
    row: RowChunks = field(default_factory=RowChunks)
    retries: int | None = None
