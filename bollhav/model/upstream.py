from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum


class UpstreamContract(str, Enum):
    """How ready an upstream must be before a downstream unit may run.

    You pick *how strict*; the upstream's **shape** (interval vs whole-table) is
    read from the library at check time, so you never restate it. A source
    without a contract is ungated (never waited on); to gate, name a level:

        Source("warehouse.orders", type=SourceModel(...), contract=UpstreamContract.ENCAPSULATE)
        Source("warehouse.orders", ..., contract=UpstreamContract.WHOLE)

    Five levels. Three are **window-scoped** — they consult only the upstream
    rows touching *my* `[since, until)`, nothing outside it — and differ only in
    the grain they accept; two widen the scope past my window:

    ``EXISTS``
        The upstream is a registered model. No window, no applied state — just
        "it's a known model". Gives run-ordering + a managed lineage edge
        without waiting for any data. The only level a **windowless** consumer
        (view / oneshot) can use to depend on an *interval* upstream without
        waiting (the stricter levels make it wait for the whole upstream).

    ``EXACT`` (window-scoped)
        Exactly **my** window — one applied upstream row with the *same*
        `(since, until)`. A coarser row that merely *contains* my window (a
        weekly backfill spanning my day) does **not** count, and neither does a
        union of finer rows. The strict choice when the upstream is aggregated at
        *my grain* and that alignment is load-bearing: you want that day's own
        aggregate row, not a coarser one that lumped several of my windows
        together.

    ``ENCAPSULATE`` (window-scoped — the usual choice)
        **My** window's data is present — the applied upstream rows *cover*
        `[since, until)` by any combination: a single coarser row that contains
        it (a daily upstream covering my hour), the exact-grain row, or a
        gap-free **union** of finer rows (24 hourly rows tiling my day). The
        per-window, **pipelining** level: my window N runs as soon as its slice
        of the upstream lands, regardless of the upstream's grain or its other
        windows. Nothing outside `[since, until)` is consulted.

    ``THROUGH`` (prefix-scoped)
        Every upstream interval **up to and including my window** is ``applied``
        — a gap-free *prefix*. For **additive / cumulative** windowed models,
        where window N sums history 1..N: ``ENCAPSULATE`` (only my window) would
        undercount; ``WHOLE`` would over-wait. ``THROUGH`` is anchored to my
        window's `until`, so it still pipelines and — unlike ``WHOLE`` — stays
        satisfiable while the upstream grows past me. Unlike the window-scoped
        levels, a hole *anywhere before* my window blocks it.

    ``WHOLE`` (whole-upstream)
        **Every** upstream interval is ``applied`` (and at least one is) — the
        whole upstream, in absolute terms (not relative to my window). For
        aggregates over all of it, snapshots, exports. On a continuously-growing
        upstream it's satisfied whenever the upstream has caught up to its latest
        *elapsed* tick (the in-progress tick isn't in state, so there's no
        permanent pending tail).

    Strength ladder (each implies those below): ``WHOLE`` ⟹ ``THROUGH`` ⟹
    ``ENCAPSULATE`` ⟹ ``EXISTS``, with ``EXACT`` ⟹ ``ENCAPSULATE`` as the
    stricter window-scoped sibling (an exact-grain row also covers the window).
    """

    EXISTS = "exists"
    EXACT = "exact"
    ENCAPSULATE = "encapsulate"
    THROUGH = "through"
    WHOLE = "whole"


class FreshnessScope(str, Enum):
    """Which of the upstream's relevant applied rows must be recent.

    The contract level (``ENCAPSULATE`` / ``THROUGH`` / ``WHOLE`` / ``timeless``)
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
