import textwrap
from pathlib import Path
from unittest.mock import MagicMock
import pytest

from bollhav.model.tagexpr import parse_expression
from bollhav.model.matching import _model_matches, match_models


def make_model(*tags: str) -> MagicMock:
    model = MagicMock()
    model.tags = set(tags)
    return model


# --- _model_matches ---


def test_model_matches_when_tag_matches():
    model = make_model("wee", "all")
    assert _model_matches(model, parse_expression("[wee]")) == (model, False)


def test_model_does_not_match_when_tag_absent():
    model = make_model("all")
    assert _model_matches(model, parse_expression("[wee]")) is None


def test_model_matches_with_reload_tag_level():
    model = make_model("sales")
    assert _model_matches(model, parse_expression("[r:sales]")) == (model, True)


def test_model_matches_with_reload_group_level():
    model = make_model("sales")
    assert _model_matches(model, parse_expression("r:[sales]")) == (model, True)


def test_model_matches_with_reload_paren_level():
    model = make_model("finance")
    assert _model_matches(model, parse_expression("[r:(sales|finance)]")) == (
        model,
        True,
    )


def test_model_reload_true_if_any_matching_group_has_reload():
    model = make_model("sales")
    assert _model_matches(model, parse_expression("[r:sales][other]")) == (model, True)


def test_model_no_reload_when_matched_group_has_no_reload():
    model = make_model("finance")
    assert _model_matches(model, parse_expression("[r:sales][finance]")) == (
        model,
        False,
    )


# --- match_models ---


def _write_model_file(path: Path, tags: list[str]) -> None:
    tag_str = ", ".join(f'"{t}"' for t in tags)
    path.write_text(
        textwrap.dedent(f"""
        from unittest.mock import MagicMock
        model = MagicMock()
        model.tags = {{{tag_str}}}
    """)
    )


def test_match_models_no_tags_raises(tmp_path: Path):
    with pytest.raises(ValueError, match="tags must be a non-empty expression"):
        match_models(folder=str(tmp_path), tags=None)


def test_match_models_empty_tags_raises(tmp_path: Path):
    with pytest.raises(ValueError, match="tags must be a non-empty expression"):
        match_models(folder=str(tmp_path), tags="")


def test_match_models_returns_matching_model(tmp_path: Path):
    _write_model_file(tmp_path / "model_a.py", ["wee", "all"])
    results = match_models(folder=str(tmp_path), tags="[wee]")
    assert isinstance(results, list)


def test_match_models_empty_folder(tmp_path: Path):
    results = match_models(folder=str(tmp_path), tags="[wee]")
    assert results == []


def test_match_models_ignores_non_py_files(tmp_path: Path):
    (tmp_path / "readme.txt").write_text("not python")
    results = match_models(folder=str(tmp_path), tags="[all]")
    assert results == []


def test_match_models_recurses_subdirectory(tmp_path: Path):
    sub = tmp_path / "sub"
    sub.mkdir()
    _write_model_file(sub / "model_b.py", ["all"])
    results = match_models(folder=str(tmp_path), tags="[all]")
    assert isinstance(results, list)


def test_match_models_or_expression(tmp_path: Path):
    _write_model_file(tmp_path / "model_x.py", ["x"])
    _write_model_file(tmp_path / "model_y.py", ["y"])
    results = match_models(folder=str(tmp_path), tags="[x|y]")
    assert isinstance(results, list)


def test_match_models_and_expression_no_match(tmp_path: Path):
    _write_model_file(tmp_path / "model_a.py", ["xyz"])
    results = match_models(folder=str(tmp_path), tags="[xyz&abc]")
    assert results == []


def _write_model_list_file(path: Path, tags_per_model: list[list[str]]) -> None:
    lines = ["from unittest.mock import MagicMock\n"]
    for i, tags in enumerate(tags_per_model):
        tag_str = ", ".join(f'"{t}"' for t in tags)
        lines.append(f"m{i} = MagicMock()")
        lines.append(f"m{i}.tags = {{{tag_str}}}")
    list_str = ", ".join(f"m{i}" for i in range(len(tags_per_model)))
    lines.append(f"models = [{list_str}]")
    path.write_text("\n".join(lines))


def test_match_models_finds_list_of_models(tmp_path: Path):
    _write_model_list_file(
        tmp_path / "model_list.py", [["wee"], ["xyz"], ["wee", "xyz"]]
    )
    results = match_models(folder=str(tmp_path), tags="[wee]")
    assert isinstance(results, list)


def test_match_models_list_filters_by_tag(tmp_path: Path):
    _write_model_list_file(tmp_path / "model_list.py", [["wee"], ["xyz"]])
    results = match_models(folder=str(tmp_path), tags="[wee]")
    assert isinstance(results, list)


def test_match_models_empty_list_ignored(tmp_path: Path):
    path = tmp_path / "empty_list.py"
    path.write_text("models = []\n")
    results = match_models(folder=str(tmp_path), tags="[wee]")
    assert results == []


def test_match_models_mixed_list_ignored_if_not_models(tmp_path: Path):
    path = tmp_path / "mixed.py"
    path.write_text('models = ["wee", 1, None]\n')
    results = match_models(folder=str(tmp_path), tags="[wee]")
    assert results == []
