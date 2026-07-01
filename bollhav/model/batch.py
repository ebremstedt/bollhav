import logging
from dataclasses import dataclass, field
from datetime import timezone, tzinfo
from roskarl import IntervalExpression, IntervalExpressionExtended


logger = logging.getLogger(__name__)

MAX_BATCH_SIZE = 100000


# ── errors ──


class BatchSizeExceedsMaxError(ValueError):
    """A `Batch.size` (or another batch size) exceeded the hard cap
    `MAX_BATCH_SIZE`. `source` names where the value came from for the
    message (e.g. `Batch.size`)."""

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
    """Flexibility attestation: the chunk is a free *slicing* of a coverage set,
    NOT identity — a promise that the model's output is invariant to how time is
    partitioned (its query is window-decomposable AND its write is
    order-independent/idempotent). State is the set of covered ranges; work is the
    *gaps* in coverage, sliced by `chunk`. Because partitioning is semantically
    irrelevant, the chunk is freely re-chunkable and the inferred trailing edge
    MAY stop at a partial chunk.

    `no_partial_below` is the **completeness grain** that partial tail snaps to:
    the latest *complete* unit of this grain. Defaults to `chunk`. e.g. a
    `@monthly` chunk with `no_partial_below="@daily"` loads through the last
    complete day — the current month as a partial, snapped to a whole day —
    instead of stopping at the last complete month. Can be finer *or* coarser than
    `chunk`. Flexible-only: only a coverage set can carry the off-grid partial
    tail an unaligned grain would produce; a fixed grid would fork its identity.
    It's the end knob; `lookback` (on `TimeChunking`) is the start knob.

    The coverage engine is live; the `APPEND` guard and overlap locking it
    implies are still deferred — see `design/flexible-intervals.md`."""

    no_partial_below: IntervalExpression | IntervalExpressionExtended | None = None


@dataclass
class TimeChunking:
    """
    Time-chunking config.

    `chunk` is a cron expression that defines the chunk size. For example,
    "0 * * * *" means each chunk is one hour, "0 0 * * *" means one day.
    Can be overridden at runtime via the pipe's batch expression env var. Used by
    both flexibilities: for a fixed grid it *is* the state identity; for a
    flexible model it's just how coverage gaps are sliced into work units.

    `latest_window` defines the catch-up bite in `latest` mode — "one of what" a
    latest run loads. Defaults to `chunk` when unset. **Only consulted in `latest`
    mode**; the inferred-window trailing edge (reload / no-dates backfill) is
    governed by the flexibility's completeness grain, not this.

    `tz` is the timezone used for interval resolution. Defaults to UTC.

    `lookback` extends the start of the interval backwards by N cron-ticks,
    useful for reprocessing recent history to account for late-arriving data.

    `flexibility` (`ChunkFix` — default — or `ChunkFlex`) declares whether
    this chunk grid is the model's state *identity* or a free slicing of a
    coverage set. See those classes. `no_partial_below` lives on `ChunkFlex`
    because only a flexible model can carry a partial tail.
    """

    chunk: IntervalExpression | IntervalExpressionExtended = "@daily"
    latest_window: IntervalExpression | IntervalExpressionExtended | None = None
    tz: tzinfo = timezone.utc
    lookback: int | None = None
    flexibility: ChunkFix | ChunkFlex = field(default_factory=ChunkFix)

    @property
    def is_flexible(self) -> bool:
        """Whether state is a coverage set (`ChunkFlex`) rather than a fixed
        grid (`ChunkFix`)."""
        return isinstance(self.flexibility, ChunkFlex)

    @property
    def completeness_grain(self) -> IntervalExpression | IntervalExpressionExtended:
        """Grain the *inferred* (reload / no-dates backfill) trailing edge floors
        on. Fixed → `chunk` (a partial tail is illegal). Flexible → declared
        `no_partial_below`, else `chunk`. Deliberately independent of
        `latest_window` — that's a different mode."""
        f = self.flexibility
        if isinstance(f, ChunkFlex):
            return f.no_partial_below or self.chunk
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
