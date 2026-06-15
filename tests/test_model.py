from datetime import datetime, timezone

import pytest

from bollhav.model.batch import Batch
from bollhav.model.contract import Contract
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
    overrides.setdefault("kind", Kind.TEMPORAL)
    if overrides["kind"] is Kind.TEMPORAL:
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
    from bollhav.model.contract import Contract

    m = make_model()
    assert isinstance(m.target.schema, str)
    assert isinstance(m.target, Target)
    assert isinstance(m.batching, Batch)
    assert isinstance(m.contract, Contract)
    assert isinstance(m.tags, set)


def test_batching_defaults_to_none_when_unspecified():
    """No batching kwarg = no chunking. model.intervals returns [None].
    A whole-table load with no batching is kind=MONOLITHIC."""
    m = Model(target=Target(name="orders"), kind=Kind.TIMELESS)
    assert m.batching is None
    assert compute_intervals(ModelRun(model=m)) == (None,)


def test_reload_without_bounds_raises():
    with pytest.raises(ValueError, match="reload requires contract.begin"):
        resolve_window(Batch(), Contract(), reload=True)


def test_backfill_without_since_or_bounds_raises():
    with pytest.raises(ValueError, match="backfill requires a since value"):
        resolve_window(Batch(), Contract())


def test_backfill_falls_back_to_bounds_for_since_but_requires_until():
    """`since` falls back to `contract.begin` when no `since` is given — the
    contract since day one. `until` has no silent fallback in backfill mode;
    it must be supplied (i.e. `BACKFILL_UNTIL`)."""
    contract = Contract(begin=datetime(2026, 1, 1, tzinfo=timezone.utc))
    with pytest.raises(ValueError, match="backfill requires an explicit until"):
        resolve_window(Batch(), contract)

    # Supply until → window resolves with contract.begin as since.
    window = resolve_window(
        Batch(), contract, until=datetime(2026, 1, 3, tzinfo=timezone.utc)
    )
    intervals = split_window(window, Batch().time.chunk)
    assert len(intervals) > 0
    assert intervals[0].since == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert intervals[-1].until == datetime(2026, 1, 3, tzinfo=timezone.utc)


class TestModelKind:
    """`Model.kind` (a `Kind` enum: INTERVAL | MONOLITHIC | VIEW) is the
    single source of truth for a model's unit of work — how many state
    rows it has and how an upstream contract checks it. It defaults to
    TEMPORAL and the is_* bools derive from it."""

    def test_temporal_model_kind(self):
        m = make_model()  # make_model defaults to kind=TEMPORAL + Batch()
        assert m.kind is Kind.TEMPORAL
        assert m.kind.value == "temporal"
        assert m.is_temporal is True
        assert m.is_table is True
        assert m.is_view is False
        assert m.is_timeless is False

    def test_timeless_model_kind(self):
        m = Model(target=Target(name="app_config"), kind=Kind.TIMELESS)
        assert m.kind is Kind.TIMELESS
        assert m.kind.value == "timeless"
        assert m.is_timeless is True
        assert m.is_table is True
        assert m.is_view is False

    def test_view_model_kind(self):
        m = Model(target=Target(name="customers"), kind=Kind.TIMELESS, view=True)
        assert m.kind is Kind.TIMELESS
        assert m.kind.value == "timeless"
        assert m.is_view is True
        assert m.is_timeless is True
        assert m.is_table is False

    def test_kind_defaults_to_temporal(self):
        # `kind` is keyword-only and defaults to TEMPORAL (the common case).
        m = Model(target=Target(name="orders"))
        assert m.kind is Kind.TEMPORAL

    def test_temporal_without_batching_is_allowed(self):
        # A temporal model has a time axis but need not be batched — it can
        # load its whole [begin, end] range in a single run.
        m = Model(target=Target(name="orders"), kind=Kind.TEMPORAL)
        assert m.batching is None
        assert m.is_temporal is True

    def test_timeless_with_batching_raises(self):
        # A timeless model has no time axis, so it can't be windowed.
        with pytest.raises(ValueError, match="has batching"):
            Model(
                target=Target(name="app_config"),
                kind=Kind.TIMELESS,
                batching=Batch(),
            )

    def test_timeless_with_contract_window_raises(self):
        # A timeless model has no time axis to bound.
        from datetime import datetime, timezone

        from bollhav.model import Contract

        with pytest.raises(ValueError, match="begin/end"):
            Model(
                target=Target(name="app_config"),
                kind=Kind.TIMELESS,
                contract=Contract(begin=datetime(2024, 1, 1, tzinfo=timezone.utc)),
            )

    def test_view_with_batching_raises(self):
        # A view isn't materialized per-window, so it can't be batched.
        with pytest.raises(ValueError, match="view but has batching"):
            Model(
                target=Target(name="customers"),
                view=True,
                batching=Batch(),
            )

    def test_temporal_view_is_allowed(self):
        # A view may be TEMPORAL: its Contract declares the range it covers,
        # recorded as a single (unbatched) state row. No batching.
        from datetime import datetime, timezone

        from bollhav.model import Contract

        m = Model(
            target=Target(name="customers"),
            kind=Kind.TEMPORAL,
            view=True,
            contract=Contract(
                begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end=datetime(2024, 2, 1, tzinfo=timezone.utc),
            ),
        )
        assert m.is_view is True
        assert m.is_temporal is True
        assert m.batching is None

    def test_timeless_view_is_allowed(self):
        # A view may also be TIMELESS — existence only, no range.
        m = Model(target=Target(name="customers"), kind=Kind.TIMELESS, view=True)
        assert m.is_view is True
        assert m.is_timeless is True


class TestUpstreamRequiresState:
    """A **gated** upstream (a Source carrying a contract) is only enforced by
    the state machine, so a model with one must also be state-tracked — else
    the contract would silently never run. Ungated sources need no state."""

    def test_gated_upstream_without_state_raises(self):
        from bollhav.model.source import Source, SourceModel
        from bollhav.model.upstream import UpstreamContract

        with pytest.raises(ValueError, match="gated upstream"):
            make_model(
                upstream=[
                    Source(
                        "warehouse.orders",
                        type=SourceModel(),
                        contract=UpstreamContract.WINDOW,
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
        from bollhav.model.upstream import UpstreamContract

        m = make_model(
            upstream=[
                Source(
                    "warehouse.customers",
                    type=SourceModel(),
                    contract=UpstreamContract.WINDOW,
                )
            ],
            state=State(),
        )
        assert m.upstream_names == ["warehouse.customers"]

    def test_no_upstream_no_state_is_fine(self):
        m = make_model()
        assert m.upstream_names == []
        assert m.inputs_known is False
