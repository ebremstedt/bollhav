"""Window calculation + splitting.

Two responsibilities, both pure (apart from the clock read in `latest` /
open-ended `reload` modes):

* `resolve_window` — turn a model's `bounds` + the run instruction
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
from roskarl.cron import INTERVAL_EXPRESSION_SHORTCUTS

if TYPE_CHECKING:
    from bollhav.model.batch import Batch
    from bollhav.model.bounds import Bounds
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
        raise RuntimeError(
            f"cron seeding invariant violated for {cron!r}: the iterator "
            f"returned a tick >= now within the first two steps"
        )
    return TZInterval(prev, curr)


def _apply_lookback(expression: str, since: datetime, lookback: int) -> datetime:
    """Shift `since` back by `lookback` cron ticks (for reprocessing recent
    history to absorb late-arriving data). `expression` must be resolved."""
    it = croniter(expression, since)
    tick1 = it.get_next(datetime)
    tick2 = it.get_next(datetime)
    tick_size = tick2 - tick1
    return since - (tick_size * lookback)


def resolve_window(
    batching: "Batch | None",
    bounds: "Bounds",
    *,
    latest: bool = False,
    reload: bool = False,
    since: datetime | None = None,
    until: datetime | None = None,
    name: str = "",
) -> TZInterval | None:
    """Resolve the single time window a run targets, from the model's
    `batching` + `bounds` and the run instruction. The result already accounts
    for `lookback`, so the caller only has to `split_window` it.

    Returns `None` when there is no `batching` — the model runs once,
    unfiltered. Three mutually-exclusive modes, in precedence order:

        reload   — `bounds.begin` .. (`bounds.end` or the latest complete tick)
        latest   — the latest complete `window_expression` tick
        backfill — `since` (or `bounds.begin`) .. `until` (required)

    Pure apart from the clock read in `latest` / open-ended `reload`."""
    if batching is None:
        return None

    expr = batching.interval.expression
    tz = batching.interval.tz

    if reload:
        if bounds.begin is None:
            raise ValueError(
                f"reload requires bounds.begin to be set on model {name!r}"
            )
        window_since = bounds.begin
        window_until = bounds.end or latest_complete_interval(expr, tz).until
    elif latest:
        window_expr = batching.interval.window_expression or expr
        interval = latest_complete_interval(window_expr, tz)
        window_since, window_until = interval.since, interval.until
    else:
        window_since = since or bounds.begin
        if window_since is None:
            raise ValueError(
                f"backfill requires a since value — set bounds.begin on model "
                f"{name!r} or pass --since at runtime"
            )
        if until is None:
            raise ValueError(
                f"backfill requires an explicit until on model {name!r} — set "
                f'BACKFILL_UNTIL. Backfill means a specific window; for "to the '
                f'latest complete tick" use latest mode, for "to bounds.end" use '
                f"reload mode."
            )
        window_until = until

    if batching.interval.lookback:
        window_since = _apply_lookback(
            _resolve_cron(expr), window_since, batching.interval.lookback
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
    the model's `batching.interval.expression`. Assign the result to
    `run.intervals`.

    `(None,)` when the run has no `batching`/`window` (monolithic / view — runs
    once, unfiltered). A batched model always has a `window`, since `runtime`
    resolves one for every interval model it builds."""
    model = run.model
    if model.batching is None or run.window is None:
        return (None,)
    return tuple(split_window(run.window, model.batching.interval.expression))
