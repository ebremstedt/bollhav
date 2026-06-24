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
class TimeChunking:
    """
    Time-chunking config.

    `chunk` is a cron expression that defines the chunk size. For example,
    "0 * * * *" means each chunk is one hour, "0 0 * * *" means one day.
    Can be overridden at runtime via the pipe's batch expression env var.

    `window` defines the scope to catch up on in `latest` mode — i.e.
    "one of what" counts as the latest complete unit. Defaults to `chunk`
    when unset. Only consulted in `latest` mode.

    `tz` is the timezone used for interval resolution. Defaults to UTC.

    `lookback` extends the start of the interval backwards by N cron-ticks,
    useful for reprocessing recent history to account for late-arriving data.

    `fixed_intervals` declares whether this chunk grid is the model's state
    *identity* (`True`, the default and today's only behavior) or a free
    slicing of a coverage set (`False`). When `True`, state is a grid — one
    row per `(since, until)` chunk; the chunk is part of identity, so changing
    it requires a state reset (`STATE_MODE=nuke`), and downstreams may gate
    `EXACT` on it. `False` is an **attestation** that the model's output is
    invariant to how time is partitioned (its query is window-decomposable AND
    its write is order-independent/idempotent), allowing variable-grain
    coverage-based state.

    NOTE: only the `True` path exists today. The coverage engine, the
    `EXACT`/`APPEND` guards, and range locking that `False` implies are not yet
    built — see `design/flexible-intervals.md`. The field is declared now so it
    is plumbed (it carries through the runtime rebuild); setting `False` has no
    effect yet.
    """

    chunk: IntervalExpression | IntervalExpressionExtended = "@daily"
    window: IntervalExpression | IntervalExpressionExtended | None = None
    tz: tzinfo = timezone.utc
    lookback: int | None = None
    fixed_intervals: bool = True


@dataclass
class Batch:
    """
    Controls how a model's work is chunked

    `time` holds the time-chunking config (a `TimeChunking`) — the cron
    expression whose ticks define the `(since, until)` windows the model
    iterates. Always present.

    `size` is the number of rows per read chunk, capped at
    `MAX_BATCH_SIZE` (100000). The framework hands `(since, until)` to the
    user's read function; the row-level sub-batching within an interval
    is honored by the read helpers, which slice the source by `size`.

    `retries` is the number of times a failed chunk should be retried.
    """

    time: TimeChunking = field(default_factory=TimeChunking)
    size: int = 20000
    retries: int | None = None

    def __post_init__(self) -> None:
        validate_batch_size(self.size, "Batch.size")
