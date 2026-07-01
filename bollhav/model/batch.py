import logging
from dataclasses import dataclass, field
from datetime import timezone, tzinfo
from roskarl import IntervalExpression, IntervalExpressionExtended


logger = logging.getLogger(__name__)

MAX_BATCH_SIZE = 100000


class BatchSizeExceedsMaxError(ValueError):
    """Batch.size exceeded the hard cap `MAX_BATCH_SIZE`"""

    def __init__(self, source: str, batch_size: int, max_batch_size: int) -> None:
        super().__init__(
            f"{source} batch_size={batch_size} exceeds max {max_batch_size}"
        )


def validate_batch_size(batch_size: int, source: str) -> None:
    """Raise if `batch_size` exceeds the hard cap. `source` names where the
    value came from for the error message (e.g. 'Batch.size')."""
    if batch_size > MAX_BATCH_SIZE:
        raise BatchSizeExceedsMaxError(source, batch_size, MAX_BATCH_SIZE)


@dataclass(frozen=True)
class ChunkFix:
    """Flexibility attestation: the chunk grid **is** the model's state identity —
    one row per `(since, until)` tick. The chunk is part of identity, so changing
    it requires a state reset (`STATE_MODE=torch`), and downstreams may gate
    `EXACT` on it. The inferred trailing edge snaps to the latest complete
    *chunk*: a fixed grid can't carry a partial tail row without forking its
    identity. The default flexibility.

    (Empty marker for now — fixed-grid-only knobs can land here later.)"""


@dataclass(frozen=True)
class ChunkFlex:
    """ChunkFlex promises the model's output doesn't depend on how time is sliced 
    into chunks — so the chunk is just a convenience, not the state's identity. 
    That lets bollhav keep state as a set of covered ranges, do its work by filling 
    the gaps, and re-chunk freely between runs."""

    floor_chunk: IntervalExpression | IntervalExpressionExtended | None = None


@dataclass
class TimeChunking:
    """`chunk` is a cron expression that defines the chunk size. For example,
    "0 * * * *" means each chunk is one hour, "0 0 * * *" means one day.
    Can be overridden at runtime via the pipe's batch expression env var. Used by
    both ChunkFix and ChunkFlex: for a fixed grid it *is* the state identity; for a
    flexible model it's just how coverage gaps are sliced into work units.

    `latest_window` defines the catch-up bite in `latest` mode — "one of what" a
    latest run loads. Defaults to `chunk` when unset. **Only consulted in `latest`
    mode**.

    `tz` is the timezone used for interval resolution. Defaults to UTC.

    `lookback` extends the start of the interval backwards by N cron-ticks,
    useful for reprocessing recent history to account for late-arriving data.

    `flexibility` (`ChunkFix` — default — or `ChunkFlex`) declares whether
    this chunk grid is the model's state *identity* or a free slicing of a
    coverage set. See those classes. `floor_chunk` lives on `ChunkFlex`
    because only a flexible model can carry a partial tail.
    """

    chunk: IntervalExpression | IntervalExpressionExtended = "@daily"
    latest_window: IntervalExpression | IntervalExpressionExtended | None = None
    tz: tzinfo = timezone.utc
    lookback: int | None = None
    flexibility: ChunkFix | ChunkFlex = field(default_factory=ChunkFix)

    @property
    def is_flexible(self) -> bool:
        return isinstance(self.flexibility, ChunkFlex)

    @property
    def floor_chunk(self) -> IntervalExpression | IntervalExpressionExtended:
        """For models with fixed interval, do not allow partials. For models with
        flexible intervals, we can allow partials, but not of a granularity lower
        than this floor.

            chunk = @monthly, "now" = mid-June

            floor = chunk  (@monthly, or fixed)
              [ Apr ][ May ]              → whole chunks only; June is dropped

            floor = @daily  (finer than chunk)
              [ Apr ][ May ][ Jun 1-15 )  → partial last chunk, cut on a whole
                                            day — never finer than the floor
        """
        f = self.flexibility
        if isinstance(f, ChunkFlex):
            return f.floor_chunk or self.chunk
        return self.chunk


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
