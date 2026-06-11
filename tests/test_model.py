from datetime import datetime, timezone

import pytest

from bollhav.model.batch import Batch
from bollhav.model.bounds import Bounds
from bollhav.model.model import Model
from bollhav.model.kind import Kind
from bollhav.model.target import Target
from bollhav.model.modelrun import ModelRun
from bollhav.model.window import compute_intervals, resolve_window, split_window


def make_model(**overrides) -> Model:
    # Default to kind=INTERVAL with an explicit Batch() so model.intervals
    # tests exercise the time-chunking path (None would short-circuit to
    # [None]). INTERVAL requires batching, so default that too — unless a
    # test overrides kind, in which case respect it.
    overrides.setdefault("kind", Kind.INTERVAL)
    if overrides["kind"] is Kind.INTERVAL:
        overrides.setdefault("batching", Batch())
    return Model(
        target=overrides.pop("target", Target(name="orders")),
        **overrides,
    )


def test_model_stores_fields():
    m = make_model()
    assert m.target.name == "orders"
    assert m.source_names == []


def test_model_exposes_sub_configs():
    from bollhav.model.target import Target
    from bollhav.model.bounds import Bounds

    m = make_model()
    assert isinstance(m.target.schema, str)
    assert isinstance(m.target, Target)
    assert isinstance(m.batching, Batch)
    assert isinstance(m.bounds, Bounds)
    assert isinstance(m.tags, set)


def test_batching_defaults_to_none_when_unspecified():
    """No batching kwarg = no chunking. model.intervals returns [None].
    A whole-table load with no batching is kind=MONOLITHIC."""
    m = Model(target=Target(name="orders"), kind=Kind.MONOLITHIC)
    assert m.batching is None
    assert compute_intervals(ModelRun(model=m)) == (None,)


def test_reload_without_bounds_raises():
    with pytest.raises(ValueError, match="reload requires bounds.begin"):
        resolve_window(Batch(), Bounds(), reload=True)


def test_backfill_without_since_or_bounds_raises():
    with pytest.raises(ValueError, match="backfill requires a since value"):
        resolve_window(Batch(), Bounds())


def test_backfill_falls_back_to_bounds_for_since_but_requires_until():
    """`since` falls back to `bounds.begin` when no `since` is given — the
    contract since day one. `until` has no silent fallback in backfill mode;
    it must be supplied (i.e. `BACKFILL_UNTIL`)."""
    bounds = Bounds(begin=datetime(2026, 1, 1, tzinfo=timezone.utc))
    with pytest.raises(ValueError, match="backfill requires an explicit until"):
        resolve_window(Batch(), bounds)

    # Supply until → window resolves with bounds.begin as since.
    window = resolve_window(
        Batch(), bounds, until=datetime(2026, 1, 3, tzinfo=timezone.utc)
    )
    intervals = split_window(window, Batch().time.chunk)
    assert len(intervals) > 0
    assert intervals[0].since == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert intervals[-1].until == datetime(2026, 1, 3, tzinfo=timezone.utc)


class TestModelKind:
    """`Model.kind` (a `Kind` enum: INTERVAL | MONOLITHIC | VIEW) is the
    single source of truth for a model's unit of work — how many state
    rows it has and how an upstream contract checks it. It's set
    explicitly on every Model (no default) and the is_* bools derive
    from it."""

    def test_interval_model_kind(self):
        m = make_model()  # make_model defaults to kind=INTERVAL + Batch()
        assert m.kind is Kind.INTERVAL
        assert m.kind.value == "interval"
        assert m.is_kind_interval is True
        assert m.is_table is True
        assert m.is_view is False
        assert m.is_kind_monolithic is False

    def test_monolithic_model_kind(self):
        m = Model(target=Target(name="app_config"), kind=Kind.MONOLITHIC)
        assert m.kind is Kind.MONOLITHIC
        assert m.kind.value == "monolithic"
        assert m.is_kind_monolithic is True
        assert m.is_table is True
        assert m.is_view is False

    def test_view_model_kind(self):
        m = Model(target=Target(name="customers"), kind=Kind.VIEW)
        assert m.kind is Kind.VIEW
        assert m.kind.value == "view"
        assert m.is_view is True
        assert m.is_kind_view is True
        assert m.is_table is False
        assert m.is_kind_monolithic is False

    def test_kind_is_required(self):
        # `kind` is keyword-only and required — omitting it is a TypeError.
        with pytest.raises(TypeError):
            Model(target=Target(name="orders"))

    def test_interval_without_batching_raises(self):
        # An interval model's unit of work is a time window, so it must
        # carry batching — caught at construction.
        with pytest.raises(ValueError, match="INTERVAL"):
            Model(target=Target(name="orders"), kind=Kind.INTERVAL)

    def test_monolithic_with_batching_raises(self):
        # A monolithic model has no interval windows — the two are
        # mutually exclusive and caught at construction.
        with pytest.raises(ValueError, match="MONOLITHIC"):
            Model(
                target=Target(name="app_config"),
                kind=Kind.MONOLITHIC,
                batching=Batch(),
            )

    def test_view_with_batching_raises(self):
        with pytest.raises(ValueError, match="VIEW"):
            Model(
                target=Target(name="customers"),
                kind=Kind.VIEW,
                batching=Batch(),
            )


class TestUpstreamRequiresState:
    """A **gated** upstream (a Source carrying a contract) is only enforced by
    the state machine, so a model with one must also be state-tracked — else
    the contract would silently never run. Ungated sources need no state."""

    def test_gated_upstream_without_state_raises(self):
        from bollhav.model.source import Source, SourceModel
        from bollhav.model.upstream import IntervalContract

        with pytest.raises(ValueError, match="gated upstream"):
            make_model(
                upstream=[
                    Source(
                        "warehouse.orders",
                        type=SourceModel(),
                        contract=IntervalContract(),
                    )
                ]
            )

    def test_ungated_source_without_state_is_fine(self):
        from bollhav.model.source import Source, SourceModel

        m = make_model(upstream=[Source("raw.landing", type=SourceModel())])
        assert m.source_names == ["raw.landing"]

    def test_gated_upstream_with_state_is_fine(self):
        from bollhav.model.source import Source, SourceModel
        from bollhav.model.state import State
        from bollhav.model.upstream import ViewContract

        m = make_model(
            upstream=[
                Source(
                    "warehouse.customers", type=SourceModel(), contract=ViewContract()
                )
            ],
            state=State(),
        )
        assert m.upstream_names == ["warehouse.customers"]

    def test_no_upstream_no_state_is_fine(self):
        m = make_model()
        assert m.upstream_names == []
        assert m.inputs_known is False
