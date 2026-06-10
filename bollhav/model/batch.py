import logging
from dataclasses import dataclass, field
from datetime import timezone, tzinfo

from roskarl import IntervalExpression, IntervalExpressionExtended

logger = logging.getLogger(__name__)

MAX_BATCH_SIZE = 100000


def validate_batch_size(batch_size: int, source: str) -> None:
    """Raise if `batch_size` exceeds the hard cap. `source` names where the
    value came from for the error message (e.g. 'Batch.size')."""
    if batch_size > MAX_BATCH_SIZE:
        raise ValueError(
            f"{source} batch_size={batch_size} exceeds max {MAX_BATCH_SIZE}"
        )


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
    `MAX_BATCH_SIZE` (100000). The framework hands `(since, until)` to the
    user's read function; the row-level sub-batching within an interval
    is honored by the read helpers, which slice the source by `size`.

    `retries` is the number of times a failed chunk should be retried.
    """

    interval: IntervalChunks = field(default_factory=IntervalChunks)
    size: int = 20000
    retries: int | None = None

    def __post_init__(self) -> None:
        validate_batch_size(self.size, "Batch.size")
