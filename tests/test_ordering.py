from unittest.mock import MagicMock
import pytest

from bollhav.model.ordering import topological_sort, UpstreamMode
from bollhav.model.write_modes import WriteMode


def make_model(
    name: str, upstream: list[str] | None = None, write_mode=WriteMode.APPEND
) -> MagicMock:
    model = MagicMock()
    model.name = name
    model.target.full_name = name
    model.upstream = upstream or []
    model.target.write_mode = write_mode
    return model


def names(results):
    return [m.target.full_name for m in results]


# --- no dependencies ---


def test_empty_list():
    assert topological_sort([]) == []


def test_single_model_no_deps():
    m = make_model("a")
    result = topological_sort([m])
    assert names(result) == ["a"]


def test_multiple_models_no_deps():
    a, b, c = make_model("a"), make_model("b"), make_model("c")
    result = topological_sort([c, a, b])
    assert names(result) == ["a", "b", "c"]


# --- linear chains ---


def test_simple_dependency():
    a = make_model("a")
    b = make_model("b", upstream=["a"])
    result = topological_sort([b, a])
    assert names(result) == ["a", "b"]


def test_three_model_chain():
    a = make_model("a")
    b = make_model("b", upstream=["a"])
    c = make_model("c", upstream=["b"])
    result = topological_sort([c, b, a])
    assert names(result) == ["a", "b", "c"]


# --- diamond dependency ---


def test_diamond_dependency():
    a = make_model("a")
    b = make_model("b", upstream=["a"])
    c = make_model("c", upstream=["a"])
    d = make_model("d", upstream=["b", "c"])
    result = topological_sort([d, c, b, a])
    ordered = names(result)
    assert ordered.index("a") < ordered.index("b")
    assert ordered.index("a") < ordered.index("c")
    assert ordered.index("b") < ordered.index("d")
    assert ordered.index("c") < ordered.index("d")


# --- missing upstream ---


def test_unmatched_upstream_raises():
    b = make_model("b", upstream=["missing"])
    with pytest.raises(ValueError, match="unmatched upstream"):
        topological_sort([b])


# --- circular dependency ---


def test_circular_dependency_raises():
    a = make_model("a", upstream=["b"])
    b = make_model("b", upstream=["a"])
    with pytest.raises(ValueError, match="Circular dependency"):
        topological_sort([a, b])


def test_self_referencing_raises():
    a = make_model("a", upstream=["a"])
    with pytest.raises(ValueError, match="Circular dependency"):
        topological_sort([a])


# --- views skip upstream ---


def test_view_ignores_upstream():
    a = make_model("a")
    v = make_model("v", upstream=["a"], write_mode=WriteMode.VIEW)
    result = topological_sort([v], upstream_mode=UpstreamMode.IGNORE_VIEWS)
    assert names(result) == ["v"]


def test_view_does_not_require_upstream_in_matched_set():
    v = make_model("v", upstream=["missing"], write_mode=WriteMode.VIEW)
    result = topological_sort([v], upstream_mode=UpstreamMode.IGNORE_VIEWS)
    assert names(result) == ["v"]


def test_view_not_ordered_after_upstream():
    a = make_model("a")
    v = make_model("v", upstream=["a"], write_mode=WriteMode.VIEW)
    result = topological_sort([v, a], upstream_mode=UpstreamMode.IGNORE_VIEWS)
    assert names(result) == ["a", "v"]


# --- upstream_mode=enforce ---


def test_enforce_mode_requires_view_upstream():
    v = make_model("v", upstream=["missing"], write_mode=WriteMode.VIEW)
    with pytest.raises(ValueError, match="unmatched upstream"):
        topological_sort([v], upstream_mode=UpstreamMode.ENFORCE)


def test_enforce_mode_orders_views():
    a = make_model("a")
    v = make_model("v", upstream=["a"], write_mode=WriteMode.VIEW)
    result = topological_sort([v, a], upstream_mode=UpstreamMode.ENFORCE)
    assert names(result) == ["a", "v"]


# --- upstream_mode=ignore ---


def test_ignore_mode_skips_sorting():
    b = make_model("b", upstream=["a"])
    a = make_model("a")
    result = topological_sort([b, a], upstream_mode=UpstreamMode.IGNORE_COMPLETELY)
    assert names(result) == ["b", "a"]


def test_ignore_mode_allows_missing_upstream():
    b = make_model("b", upstream=["missing"])
    result = topological_sort([b], upstream_mode=UpstreamMode.IGNORE_COMPLETELY)
    assert names(result) == ["b"]
