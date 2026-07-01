"""Window calculation + splitting.

Two responsibilities, both pure (apart from the clock read in `latest` /
open-ended `reload` modes):

* `resolve_window` — turn a model's `contract` + the run instruction
  (reload / latest / backfill) into the single `(since, until)` window a run
  targets, lookback already applied.
* `split_window` — chop one resolved window into cron-tick-sized
  `TZInterval`s. Knows nothing about modes.

`Model` carries the resolved `window` and only calls `split_window`; the
calculation lives here so the model stays free of window logic.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone, tzinfo
from collections.abc import Iterable
from typing import TYPE_CHECKING

from icron import croniter
from bollhav.model.intervals import TZInterval
from bollhav.model.temporality import Temporality
from roskarl.cron import INTERVAL_EXPRESSION_SHORTCUTS

if TYPE_CHECKING:
    from bollhav.model.batch import Batch
    from bollhav.model.contract import Contract
    from bollhav.model.modelrun import ModelRun


# ── errors ──


class CronSeedingInvariantError(RuntimeError):
    """The cron iterator returned a tick `>= now` within its first two steps,
    violating the seeding invariant that at least two ticks precede `now`.
    Signals an internal bug in window resolution, not bad config. `cron` is
    the resolved cron expression.

    Subclasses `RuntimeError` directly (not `ValueError`) so it keeps its
    original catch semantics."""

    def __init__(self, cron: str) -> None:
        super().__init__(
            f"cron seeding invariant violated for {cron!r}: the iterator "
            f"returned a tick >= now within the first two steps"
        )


class ReloadRequiresContractBeginError(ValueError):
    """A `reload` window was requested but `contract.begin` isn't set — reload
    spans `contract.begin` .. (`contract.end` or latest tick), so a begin is
    required. `name` is the model name."""

    def __init__(self, name: str) -> None:
        super().__init__(f"reload requires contract.begin to be set on model {name!r}")


class BackfillRequiresSinceError(ValueError):
    """A backfill window was requested with no `since` — backfill needs an
    explicit start. Set `contract.begin` on the model or pass `--since` at
    runtime. `name` is the model name."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"backfill requires a since value — set contract.begin on model "
            f"{name!r} or pass --since at runtime"
        )


_CRON_ALIASES = INTERVAL_EXPRESSION_SHORTCUTS


def _resolve_cron(expr: str) -> str:
    return _CRON_ALIASES.get(expr, expr)


def latest_complete_interval(expression: str, tz: tzinfo = timezone.utc) -> TZInterval:
    """The most recent fully-elapsed cron interval as a TZInterval — e.g. at
    14:35 with an hourly expression, `13:00-14:00` (14:00-15:00 is still in
    progress). Pure apart from the `datetime.now()` read. Accepts `@`-aliases."""
    cron = _resolve_cron(expression)
    now = datetime.now(tz=tz)
    probe = croniter(cron, now)
    tick1 = probe.get_next(datetime)
    tick2 = probe.get_next(datetime)
    interval_size = tick2 - tick1
    it = croniter(cron, now - (interval_size * 3))
    prev, curr = None, None
    while True:
        tick = it.get_next(datetime)
        if tick >= now:
            break
        prev, curr = curr, tick
    # Loop invariant: cron is seeded `interval_size * 3` before now, so at
    # least 2 ticks have been consumed before the break and both `prev` and
    # `curr` are populated.
    if prev is None or curr is None:
        raise CronSeedingInvariantError(cron)
    return TZInterval(prev, curr)


def window_holds_a_grid_cell(window: TZInterval, chunk_expr: str) -> bool:
    """Whether at least one whole `chunk` grid cell `[tick, next_tick)` fits fully
    inside `window`. A fixed grid runs whole cells only, so a window that holds
    none — narrower than the chunk, or misaligned — has nothing valid to run.

    `icron.croniter` only walks forward (`get_next` is strictly greater), so probe
    1µs before `since` to get the first tick *at or after* it, then the tick after
    that: the cell `[first, second)` sits fully inside iff `second <= until`."""
    cron = _resolve_cron(chunk_expr)
    first = croniter(cron, window.since - timedelta(microseconds=1)).get_next(datetime)
    second = croniter(cron, first).get_next(datetime)
    return second <= window.until


def _apply_lookback(expression: str, since: datetime, lookback: int) -> datetime:
    """Shift `since` back by `lookback` cron ticks (for reprocessing recent
    history to absorb late-arriving data). `expression` must be resolved."""
    it = croniter(expression, since)
    tick1 = it.get_next(datetime)
    tick2 = it.get_next(datetime)
    tick_size = tick2 - tick1
    return since - (tick_size * lookback)


def _trailing_edge(contract: "Contract", batching: "Batch", tz: tzinfo) -> datetime:
    """The *inferred* end of a run window (reload / no-dates backfill):

      * open contract → floor to the latest *complete* unit of the model's
        `floor_chunk`. So a flexible model whose `floor_chunk` is finer than its
        chunk trails off mid-chunk on a partial — e.g. `@monthly` chunk with
        `floor_chunk="@daily"` runs through the last complete day, the current
        month as a partial.
      * explicit end  → `min(end, floor)`: a closed (past) end is its own value;
        a *future* end is clamped to the floor — state windows on the update
        watermark, so there's no data past the clock to load anyway.

    Pure apart from the clock read in the floor."""
    floor = latest_complete_interval(batching.time.floor_chunk, tz).until
    end = contract.end
    return floor if end is None else min(end, floor)


# An unbatched model has no chunk, so an open temporal contract has no grain to
# floor its trailing edge on. Default to whole completed days — coarse enough
# that a daily-reloaded dim's span advances at most once per run.
_UNBATCHED_TRAILING_GRAIN = "@daily"


def resolve_window(
    batching: "Batch | None",
    contract: "Contract",
    *,
    temporality: Temporality = Temporality.TEMPORAL,
    latest: bool = False,
    reload: bool = False,
    since: datetime | None = None,
    until: datetime | None = None,
    name: str = "",
) -> TZInterval | None:
    """Resolve the single time window a run targets, from the model's
    `batching` + `contract` and the run instruction. The result already
    accounts for `lookback`, so the caller only has to `split_window` it.

    With no `batching` the model runs once, unfiltered, recorded as a single
    state row. A TEMPORAL model with a `contract.begin` spans `[begin, end]`
    (an open end falls back to the latest complete day — an unbatched model has
    no chunk to floor on); that spanning row is what a downstream gates WINDOW
    against and what `uncovered_gaps` counts as coverage. A TIMELESS or
    range-less model has no window → `None` → a NULL-window one-shot row.

    For a batched model, three mutually-exclusive modes, in precedence order:

        reload   — `contract.begin` .. (`contract.end` or the latest complete tick)
        latest   — the latest complete `window` tick
        backfill — `since` (or `contract.begin`) .. `until` (or `contract.end`,
                   or the latest complete tick)

    `until` is optional: a no-dates backfill runs the contract's declared range —
    the same window `reload` resolves (reload is just a backfill whose bounds
    come from the contract rather than explicit dates). `since` still needs a
    source — `RUN_SINCE` or `contract.begin`.

    Pure apart from the clock read in `latest` / open-ended `reload` / a
    no-`until` backfill / an open temporal unbatched contract."""
    if batching is None:
        # Unbatched: the model runs once over its whole declared range, recorded
        # as a single state row spanning it so a downstream can gate WINDOW (its
        # window must fall inside the span) and `uncovered_gaps` sees coverage.
        # Only a TEMPORAL model with a `begin` has a range to span; a TIMELESS /
        # range-less model has no window → None → a NULL-window one-shot row.
        if temporality is not Temporality.TEMPORAL or contract.begin is None:
            return None
        # Closed contract → [begin, end]. Open contract (no end) → [begin, the
        # latest complete day]: with no chunk to floor on, the trailing edge
        # floors on a default daily grain. Without this an open-contract
        # temporal oneshot fell through to None and registered a NULL-window
        # row, which `uncovered_gaps` (WHERE since IS NOT NULL) ignores — so the
        # whole contract reported as one uncovered gap.
        if contract.end is not None:
            return TZInterval(contract.begin, contract.end)
        edge = latest_complete_interval(
            _UNBATCHED_TRAILING_GRAIN, contract.begin.tzinfo or timezone.utc
        ).until
        return TZInterval(contract.begin, edge)

    expr = batching.time.chunk
    tz = batching.time.tz

    if reload:
        if contract.begin is None:
            raise ReloadRequiresContractBeginError(name)
        window_since = contract.begin
        window_until = _trailing_edge(contract, batching, tz)
    elif latest:
        window_expr = batching.time.latest_window or expr
        interval = latest_complete_interval(window_expr, tz)
        window_since, window_until = interval.since, interval.until
    else:
        window_since = since or contract.begin
        if window_since is None:
            raise BackfillRequiresSinceError(name)
        # `until` is optional — when absent, the trailing edge is inferred (the
        # contract's end, or the latest complete unit of the `floor_chunk`
        # for an open contract). A no-dates backfill thus runs the declared
        # range, the same window reload resolves.
        window_until = (
            until if until is not None else _trailing_edge(contract, batching, tz)
        )

    if batching.time.lookback:
        window_since = _apply_lookback(
            _resolve_cron(expr), window_since, batching.time.lookback
        )
    return TZInterval(window_since, window_until)


def split_window(window: TZInterval, expression: str) -> list[TZInterval]:
    """Split one resolved window into cron-tick-sized TZIntervals. Knows
    nothing about modes — it just walks `expression`'s ticks across the
    window. Accepts `@`-aliases."""
    cron = _resolve_cron(expression)
    it = croniter(cron, window.since)
    intervals: list[TZInterval] = []
    current = window.since
    while True:
        tick = it.get_next(datetime)
        if tick >= window.until:
            break
        intervals.append(TZInterval(current, tick))
        current = tick
    if current < window.until:
        intervals.append(TZInterval(current, window.until))
    return intervals


def split_at_times(interval: TZInterval, points: Iterable[datetime]) -> list[TZInterval]:
    """Split `interval` at each point in `points` that falls *strictly inside* it,
    returning the pieces left-to-right. A point on `since`/`until`, or outside the
    interval, is ignored — so no empty piece is produced; with no interior point
    the interval comes back unchanged, as a one-element list.

        cut [since, until) at interior points a and b → three pieces:

            since     a            b           until
            |---------|------------|-----------|
            [since, a)   [a, b)      [b, until)
              piece 1     piece 2       piece 3

    Each piece is a `TZInterval`; together they tile the original exactly (no gap,
    no overlap). A piece can be **any width** — the cut points are arbitrary —
    unlike a `chunk`, which is a uniform cron-grid cell (every chunk the same
    cadence). So splitting a whole-month unit at a mid-month edge yields two
    *unequal* pieces (see the diagram: the three pieces have different widths).

    Used to force extra boundaries — a run window's edges — into a set of
    chunk-sliced units so none straddles them: `get_actionable_intervals` only
    returns units fully inside the window, so a chunk coarser than the window
    would otherwise overshoot the edge and never become actionable."""
    cuts = sorted(p for p in set(points) if interval.since < p < interval.until)
    if not cuts:
        return [interval]
    pieces: list[TZInterval] = []
    start = interval.since
    for p in cuts:
        pieces.append(TZInterval(start, p))
        start = p
    pieces.append(TZInterval(start, interval.until))
    return pieces


def compute_intervals(run: "ModelRun") -> tuple[TZInterval, ...] | tuple[None]:
    """Split a run's resolved `window` into its interval contract, splitting by
    the model's `batching.time.chunk`. Assign the result to
    `run.intervals`.

    `(None,)` when the run has no `window` (a timeless model, or a temporal one
    with no declared range — runs once, unfiltered, one NULL-window row). An
    unbatched temporal run with a resolved `[begin, end]` window yields that
    single window unsplit; a batched run splits its window into chunks."""
    model = run.model
    if run.window is None:
        return (None,)
    if model.batching is None:
        return (run.window,)
    return tuple(split_window(run.window, model.batching.time.chunk))


def contract_intervals(run: "ModelRun") -> tuple[TZInterval, ...] | tuple[None]:
    """Every interval the **contract declares** — the full set of `(since,
    until)` rows that *should* exist, from `contract.begin` to `contract.end`
    (or the latest complete tick for an open contract), split by the model's
    chunk. This is what *prefill* materializes, independent of the run's
    window/mode: state stays complete against the contract, and as the
    contract's forward edge advances the new ticks appear here. The storage
    layer inserts only the rows not already present (incremental — prior runs
    laid down the rest).

    Contrast `compute_intervals`, which returns just *this run's* window. Falls
    back to that window when there's nothing to declare against — an unbatched
    model, or a batched one with no `contract.begin`."""
    model = run.model
    if model.batching is not None and model.contract.begin is not None:
        contract_window = resolve_window(
            model.batching, model.contract, reload=True, name=model.target.full_name
        )
        if contract_window is not None:
            return tuple(split_window(contract_window, model.batching.time.chunk))
    return compute_intervals(run)
