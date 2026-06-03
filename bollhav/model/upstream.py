"""Upstream dependency declarations.

A model's `upstream` is a list of dependencies on other models. Each
entry is a `Contract` — a *declared* dependency that says, by the
upstream's shape, how satisfaction is checked. (A bare string is also
accepted for backwards compatibility: a name-only dependency whose kind
is inferred from the upstream's own registration.)

The three contract kinds mirror the three model kinds:

* `IntervalContract` — the upstream is an interval table; satisfied when
  it has an applied state row covering the downstream's window.
* `ViewContract` — the upstream is a view; satisfied when the view exists.
* `MonolithicContract` — the upstream is a monolithic (whole-table)
  model; satisfied when the whole table has been loaded.

`kind` returns the string the state backend keys its satisfaction check
on (`"interval"` | `"view"` | `"monolithic"`) — the same vocabulary as
`Model.kind` and the library's `kind` column.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Contract:
    """A declared dependency on an upstream model, by full target name.

    Use a concrete subclass (`IntervalContract` / `ViewContract` /
    `MonolithicContract`) — it picks how the dependency is satisfied. The
    base class is abstract: it carries the name but has no satisfaction
    semantics of its own."""

    name: str

    @property
    def kind(self) -> str:
        raise NotImplementedError(
            "Contract is abstract — use IntervalContract, ViewContract, or "
            "MonolithicContract, which each declare a `kind`."
        )


@dataclass(frozen=True)
class IntervalContract(Contract):
    """Upstream is an interval table. Satisfied when the upstream has an
    applied state row whose window covers the downstream interval (a
    daily-cadence upstream thus covers an hourly downstream)."""

    @property
    def kind(self) -> str:
        return "interval"


@dataclass(frozen=True)
class ViewContract(Contract):
    """Upstream is a view. Satisfied when the view exists (its single
    existence row is applied) — the downstream window is irrelevant."""

    @property
    def kind(self) -> str:
        return "view"


@dataclass(frozen=True)
class MonolithicContract(Contract):
    """Upstream is a monolithic (whole-table) model. Satisfied when the
    whole table has been loaded (its single whole-table row is applied in state) —
    the downstream window is irrelevant."""

    @property
    def kind(self) -> str:
        return "monolithic"


@dataclass(frozen=True)
class UpstreamCheck:
    """The verdict of checking a model's upstreams for one unit of work.

    Every declared upstream is checked — the verdict doesn't short-circuit
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
    "Contract",
    "IntervalContract",
    "ViewContract",
    "MonolithicContract",
    "UpstreamCheck",
]
