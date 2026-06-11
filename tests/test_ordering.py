from unittest.mock import MagicMock
import pytest

from bollhav.model.ordering import topological_sort
from bollhav.model.kind import Kind


def make_model(
    name: str, upstream: list[str] | None = None, kind=Kind.INTERVAL
) -> MagicMock:
    model = MagicMock()
    model.name = name
    model.target.full_name = name
    # `topological_sort` orders against `model.upstream_names` (a property on
    # the real Model that resolves Contracts/bare strings to names), so the
    # mock must expose that — `upstream` itself is no longer read here.
    upstream = upstream or []
    model.upstream = upstream
    # `topological_sort` orders on `declared_inputs` (gated upstreams + ungated
    # model sources) so ordering follows every producer→consumer edge, while
    # runtime gating stays contract-only via `upstream_names`. These tests pass
    # bare names as the model's full set of inputs, so both resolve to it.
    model.upstream_names = upstream
    model.declared_inputs = upstream
    # Views are identified by kind now (`model.is_view` is kind-based). Set
    # both `kind` and the derived `is_view` so the mock matches the real
    # Model's contract.
    model.kind = kind
    model.is_view = kind is Kind.VIEW
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
#
# An unmatched upstream (one not present in THIS run) is no longer an error:
# it ships in another pipeline / under different TAGS and its satisfaction is
# resolved at runtime against the cross-pipeline state library, not at sort
# time. So it is simply skipped during ordering rather than raising.


def test_unmatched_upstream_is_skipped_not_raised():
    b = make_model("b", upstream=["missing"])
    result = topological_sort([b])
    assert names(result) == ["b"]


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


# --- views are ordered like any other model ---


def test_view_ordered_after_upstream():
    a = make_model("a")
    v = make_model("v", upstream=["a"], kind=Kind.VIEW)
    result = topological_sort([v, a])
    assert names(result) == ["a", "v"]
