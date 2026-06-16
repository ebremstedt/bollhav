from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum


class UpstreamContract(str, Enum):
    """How ready an upstream must be before a downstream unit may run.

    A weak→strong **ladder** — each level implies the ones below it
    (``WHOLE`` ⟹ ``THROUGH`` ⟹ ``WINDOW`` ⟹ ``EXISTS``).
    You pick *how strict*; the upstream's **shape** (interval vs whole-table) is
    read from the library at check time, so you never restate it. A source
    without a contract is ungated (never waited on); to gate, name a level::

        Source("warehouse.orders", type=SourceModel(...), contract=UpstreamContract.WINDOW)
        Source("warehouse.orders", ..., contract=UpstreamContract.WHOLE)

    ``EXISTS``
        The upstream is a registered model. No window, no applied state — just
        "it's a known model". Gives run-ordering + a managed lineage edge
        without waiting for any data. The only level a **windowless** consumer
        (view / monolithic) can use to depend on an *interval* upstream without
        waiting (the stricter levels make it wait for the whole upstream).

    ``WINDOW`` (the usual choice)
        The upstream window that lines up with **my** window is ``applied`` — a
        coarser upstream window that *contains* mine counts too (a daily upstream
        covers an hourly consumer). This is the per-window, **pipelining** level:
        my window N runs as soon as the upstream's matching window lands, with no
        regard for the upstream's other windows.

        Resolves by shape: for a **whole-table** upstream (one existence row), or
        a **windowless** consumer reading *all* of an interval upstream, "the
        data I read" is the whole thing — so this means the existence row /
        every interval is applied.

    ``THROUGH``
        Every upstream interval **up to and including my window** is ``applied``
        — a gap-free *prefix*. For **additive / cumulative** windowed models,
        where window N sums history 1..N: ``WINDOW`` (only N) would
        undercount; ``WHOLE`` would over-wait. ``THROUGH`` is
        anchored to my window, so it still pipelines and — unlike ``WHOLE``
        — stays satisfiable while the upstream grows past me. Differs from
        ``WHOLE`` only when a consumer runs **behind** a moving upstream
        (backfill / catch-up).

    ``WHOLE``
        **Every** upstream interval is ``applied`` (and at least one is) — the
        whole upstream, in absolute terms (not relative to my window). For
        aggregates over all of it, snapshots, exports. On a continuously-growing
        upstream it's satisfied whenever the upstream has caught up to its latest
        *elapsed* tick (the in-progress tick isn't in state, so there's no
        permanent pending tail).
    """

    EXISTS = "exists"
    WINDOW = "window"
    THROUGH = "through"
    WHOLE = "whole"


class FreshnessScope(str, Enum):
    """Which of the upstream's relevant applied rows must be recent.

    The contract level (``WINDOW`` / ``THROUGH`` / ``WHOLE`` / ``timeless``)
    already selects *which* rows count as "the data I read"; the scope then says
    *how much of that selection* has to be fresh:

    ``LATEST``
        The most recently applied row in the selection is within the age. "Has
        the upstream been refreshed recently / is it keeping up at the head?" —
        the natural choice for a continuously-growing table.

    ``ALL``
        *Every* applied row in the selection is within the age (the oldest one
        is). "Was the whole thing rebuilt recently?" — for full snapshots /
        reference tables that should be wholly re-derived each load.
    """

    LATEST = "latest"
    ALL = "all"


@dataclass(frozen=True)
class Freshness:
    """A consumer-declared freshness policy on a gated `Source`.

    Orthogonal to the `UpstreamContract` *level* (which decides completeness):
    freshness adds a recency bound on top, checked only once the level's
    completeness is satisfied. `within` is the maximum age; `scope` picks
    `LATEST` (newest applied row recent) vs `ALL` (every relevant applied row
    recent). The age is measured against the upstream's `applied_at` — a
    producer-side, shared timestamp — so different consumers can demand
    different freshness off the same upstream load. Not valid with `EXISTS`
    (which never inspects state)."""

    within: timedelta
    scope: FreshnessScope = FreshnessScope.LATEST


@dataclass(frozen=True)
class UpstreamCheck:
    """The verdict of checking a model's gated upstreams for one unit of work.

    Every gated upstream is checked — the verdict doesn't short-circuit
    on the first failure — and `blockers` holds one short descriptor per
    unsatisfied upstream, shaped `upstream 'name' (kind)` (empty when all
    are satisfied). `satisfied` is the all-clear; `reason` composes the
    single, concise `blocked_reason` string the state row stores: one
    STATE code over the whole list, with no per-upstream repetition of the
    window (which already lives on the row's `since`/`until`)."""

    blockers: tuple[str, ...] = ()

    @property
    def satisfied(self) -> bool:
        return not self.blockers

    @property
    def reason(self) -> str | None:
        if not self.blockers:
            return None
        from bollhav.model.state import BlockCode, format_block_reason

        return format_block_reason(
            BlockCode.UPSTREAM_NOT_SATISFIED, ", ".join(self.blockers)
        )


__all__ = [
    "UpstreamContract",
    "UpstreamCheck",
    "Freshness",
    "FreshnessScope",
]
