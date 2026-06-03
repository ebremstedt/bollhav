from datetime import datetime, timezone

import pytest

from bollhav.model.batch import Batch
from bollhav.model.bounds import Bounds
from bollhav.model.model import Model
from bollhav.model.model_type import ModelType
from bollhav.model.target import Target


def make_model(**overrides) -> Model:
    # Default to an explicit Batch() so model.intervals tests exercise
    # the time-chunking path (None would short-circuit to [None]).
    overrides.setdefault("batching", Batch())
    return Model(
        target=overrides.pop("target", Target(name="orders")),
        source=overrides.pop("source", None),
        **overrides,
    )


def test_model_stores_fields():
    m = make_model()
    assert m.target.name == "orders"
    assert m.source is None


def test_model_exposes_sub_configs():
    from bollhav.model.target_schema import TargetSchema
    from bollhav.model.target import Target
    from bollhav.model.bounds import Bounds

    m = make_model()
    assert isinstance(m.target.schema, TargetSchema)
    assert isinstance(m.target, Target)
    assert isinstance(m.batching, Batch)
    assert isinstance(m.bounds, Bounds)
    assert isinstance(m.tags, set)


def test_batching_defaults_to_none_when_unspecified():
    """No batching kwarg = no chunking. model.intervals returns [None]."""
    m = Model(target=Target(name="orders"))
    assert m.batching is None
    assert m.compute_intervals() == (None,)


def test_intervals_reload_without_bounds_raises():
    m = make_model()
    m.directives.reload = True
    with pytest.raises(ValueError, match="reload requires bounds.begin"):
        m.compute_intervals()


def test_intervals_backfill_without_since_or_bounds_raises():
    m = make_model()
    with pytest.raises(ValueError, match="backfill requires a since value"):
        m.compute_intervals()


def test_intervals_backfill_falls_back_to_bounds_for_since_but_requires_until():
    """`since` still falls back to `bounds.begin` when `directives.since`
    is unset — that's been the contract since day one. `until` no
    longer has a silent fallback in backfill mode; it must be set
    via `directives.until` (i.e. `BACKFILL_UNTIL`)."""
    m = make_model(bounds=Bounds(begin=datetime(2026, 1, 1, tzinfo=timezone.utc)))
    with pytest.raises(ValueError, match="backfill requires an explicit until"):
        _ = m.compute_intervals()

    # Set until → backfill window resolves cleanly using bounds.begin
    # for the since side and directives.until for the until side.
    m.directives.until = datetime(2026, 1, 3, tzinfo=timezone.utc)
    intervals = m.compute_intervals()
    assert len(intervals) > 0
    assert intervals[0].since == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert intervals[-1].until == datetime(2026, 1, 3, tzinfo=timezone.utc)


class TestModelKind:
    """`Model.kind` ('interval' | 'monolithic' | 'view') drives how many
    state rows a model has and how an upstream contract checks it. It's
    derived from is_view / is_monolithic, with interval (batched) the
    default."""

    def test_batched_model_is_interval(self):
        m = make_model()  # make_model defaults to Batch()
        assert m.kind == "interval"
        assert m.is_view is False
        assert m.is_monolithic is False

    def test_unbatched_table_is_interval_by_default(self):
        # No batching and not explicitly monolithic → still 'interval'.
        m = Model(target=Target(name="orders"))
        assert m.kind == "interval"

    def test_monolithic_model_is_monolithic(self):
        m = Model(target=Target(name="app_config"), monolithic=True)
        assert m.kind == "monolithic"
        assert m.is_monolithic is True
        assert m.is_view is False

    def test_view_model_is_view(self):
        m = Model(target=Target(name="customers", model_type=ModelType.VIEW))
        assert m.kind == "view"
        assert m.is_view is True
        assert m.is_monolithic is False

    def test_monolithic_with_batching_raises(self):
        # A monolithic model has no interval windows — the two are
        # mutually exclusive and caught at construction.
        with pytest.raises(ValueError, match="monolithic"):
            Model(
                target=Target(name="app_config"),
                monolithic=True,
                batching=Batch(),
            )

    def test_view_cannot_be_monolithic(self):
        with pytest.raises(ValueError, match="VIEW"):
            Model(
                target=Target(name="customers", model_type=ModelType.VIEW),
                monolithic=True,
            )
