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

from datetime import datetime, timezone, tzinfo
from typing import TYPE_CHECKING

from icron import croniter
from bollhav.model.intervals import TZInterval
from bollhav.model.messages.error import (
    BackfillRequiresSinceError,
    CronSeedingInvariantError,
    ReloadRequiresContractBeginError,
)
from roskarl.cron import INTERVAL_EXPRESSION_SHORTCUTS

if TYPE_CHECKING:
    from bollhav.model.batch import Batch
    from bollhav.model.contract import Contract
    from bollhav.model.modelrun import ModelRun

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


def _apply_lookback(expression: str, since: datetime, lookback: int) -> datetime:
    """Shift `since` back by `lookback` cron ticks (for reprocessing recent
    history to absorb late-arriving data). `expression` must be resolved."""
    it = croniter(expression, since)
    tick1 = it.get_next(datetime)
    tick2 = it.get_next(datetime)
    tick_size = tick2 - tick1
    return since - (tick_size * lookback)


def _trailing_edge(contract: "Contract", batching: "Batch", tz: tzinfo) -> datetime:
    """The *inferred* end of a run window (reload / no-dates backfill), factoring
    in `contract.end` and the `TimeChunking` trailing-edge knobs:

      * open contract               → the **completeness floor**: the latest
        *complete* unit of `no_partial_below` (defaulting to `chunk`). So a
        coarse chunk can trail off at a finer boundary (e.g. `@yearly` chunk,
        `no_partial_below="@daily"` → through the last complete day).
      * explicit end + `future_data` → the end, honoured literally — it may lead
        the clock (forecasts / booked-ahead schedules).
      * explicit end, otherwise     → `min(end, floor)` — a *future* end is
        clamped to the clock (no empty future periods); a *past* end is already
        smaller, so it wins.

    Pure apart from the clock read in the floor."""
    floor_expr = batching.time.no_partial_below or batching.time.chunk
    floor = latest_complete_interval(floor_expr, tz).until
    end = contract.end
    if end is None:
        return floor
    if batching.time.future_data:
        return end
    return min(end, floor)


def resolve_window(
    batching: "Batch | None",
    contract: "Contract",
    *,
    latest: bool = False,
    reload: bool = False,
    since: datetime | None = None,
    until: datetime | None = None,
    name: str = "",
) -> TZInterval | None:
    """Resolve the single time window a run targets, from the model's
    `batching` + `contract` and the run instruction. The result already
    accounts for `lookback`, so the caller only has to `split_window` it.

    Returns `None` when there is no `batching` — the model runs once,
    unfiltered. Three mutually-exclusive modes, in precedence order:

        reload   — `contract.begin` .. (`contract.end` or the latest complete tick)
        latest   — the latest complete `window` tick
        backfill — `since` (or `contract.begin`) .. `until` (or `contract.end`,
                   or the latest complete tick)

    `until` is optional: a no-dates backfill runs the contract's declared range —
    the same window `reload` resolves (reload is just a backfill whose bounds
    come from the contract rather than explicit dates). `since` still needs a
    source — `BACKFILL_SINCE` or `contract.begin`.

    Pure apart from the clock read in `latest` / open-ended `reload` / a
    no-`until` backfill."""
    if batching is None:
        # Unbatched: the model runs once over its whole declared range. A
        # temporal model with a closed contract window [begin, end] records a
        # single state row spanning it, so a downstream can gate WINDOW on it
        # (its window must fall inside the range). A timeless / range-less model
        # has no window → None → a NULL-window one-shot row.
        if contract.begin is not None and contract.end is not None:
            return TZInterval(contract.begin, contract.end)
        return None

    expr = batching.time.chunk
    tz = batching.time.tz

    if reload:
        if contract.begin is None:
            raise ReloadRequiresContractBeginError(name)
        window_since = contract.begin
        window_until = _trailing_edge(contract, batching, tz)
    elif latest:
        window_expr = batching.time.window or expr
        interval = latest_complete_interval(window_expr, tz)
        window_since, window_until = interval.since, interval.until
    else:
        window_since = since or contract.begin
        if window_since is None:
            raise BackfillRequiresSinceError(name)
        # `until` is optional — when absent, the trailing edge is inferred (the
        # contract's end, or the latest complete unit of the completeness grain
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
