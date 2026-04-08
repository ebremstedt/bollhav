from unittest.mock import MagicMock
import pytest

from bollhav.model.ordering import topological_sort


def make_model(name: str, upstream: list[str] | None = None) -> MagicMock:
    model = MagicMock()
    model.name = name
    model.upstream = upstream or []
    return model


def names(results):
    return [m.name for m, _ in results]


# --- no dependencies ---


def test_empty_list():
    assert topological_sort([]) == []


def test_single_model_no_deps():
    m = make_model("a")
    result = topological_sort([(m, False)])
    assert names(result) == ["a"]


def test_multiple_models_no_deps():
    a, b, c = make_model("a"), make_model("b"), make_model("c")
    result = topological_sort([(c, False), (a, False), (b, False)])
    assert names(result) == ["a", "b", "c"]


# --- linear chains ---


def test_simple_dependency():
    a = make_model("a")
    b = make_model("b", upstream=["a"])
    result = topological_sort([(b, False), (a, False)])
    assert names(result) == ["a", "b"]


def test_three_model_chain():
    a = make_model("a")
    b = make_model("b", upstream=["a"])
    c = make_model("c", upstream=["b"])
    result = topological_sort([(c, False), (b, True), (a, False)])
    assert names(result) == ["a", "b", "c"]


def test_reload_flags_preserved():
    a = make_model("a")
    b = make_model("b", upstream=["a"])
    result = topological_sort([(b, True), (a, False)])
    assert result == [(a, False), (b, True)]


# --- diamond dependency ---


def test_diamond_dependency():
    a = make_model("a")
    b = make_model("b", upstream=["a"])
    c = make_model("c", upstream=["a"])
    d = make_model("d", upstream=["b", "c"])
    result = topological_sort([(d, False), (c, False), (b, False), (a, False)])
    ordered = names(result)
    assert ordered.index("a") < ordered.index("b")
    assert ordered.index("a") < ordered.index("c")
    assert ordered.index("b") < ordered.index("d")
    assert ordered.index("c") < ordered.index("d")


# --- missing upstream ---


def test_unmatched_upstream_raises():
    b = make_model("b", upstream=["missing"])
    with pytest.raises(ValueError, match="unmatched upstream"):
        topological_sort([(b, False)])


# --- circular dependency ---


def test_circular_dependency_raises():
    a = make_model("a", upstream=["b"])
    b = make_model("b", upstream=["a"])
    with pytest.raises(ValueError, match="Circular dependency"):
        topological_sort([(a, False), (b, False)])


def test_self_referencing_raises():
    a = make_model("a", upstream=["a"])
    with pytest.raises(ValueError, match="Circular dependency"):
        topological_sort([(a, False)])
