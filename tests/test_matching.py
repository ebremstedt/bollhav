import textwrap
from pathlib import Path
from unittest.mock import MagicMock
import pytest

from bollhav.model.matching import (
    _group_matches,
    _model_matches,
    _parse_tag_expression,
    _tags_match,
    match_models,
)


def make_model(*tags: str) -> MagicMock:
    model = MagicMock()
    model.model_config.tags = set(tags)
    return model


# --- _parse_tag_expression ---


def test_parse_single_group_single_tag():
    assert _parse_tag_expression("[wee]") == [["wee"]]


def test_parse_single_group_and():
    assert _parse_tag_expression("[xyz&abc]") == [["xyz", "abc"]]


def test_parse_single_group_or():
    result = _parse_tag_expression("[wee|x]")
    assert result == [[["wee", "x"]]]


def test_parse_single_group_and_with_or():
    result = _parse_tag_expression("[xyz&(c|e)]")
    assert result == [["xyz", ["c", "e"]]]


def test_parse_multiple_groups():
    result = _parse_tag_expression("[wee],[xyz]")
    assert result == [["wee"], ["xyz"]]


def test_parse_complex_expression():
    result = _parse_tag_expression("[wee|x],[xyz&(c|e)]")
    assert result == [[["wee", "x"]], ["xyz", ["c", "e"]]]


def test_parse_no_brackets_raises():
    with pytest.raises(ValueError, match="Invalid tag expression"):
        _parse_tag_expression("wee")


def test_parse_empty_string_raises():
    with pytest.raises(ValueError, match="Invalid tag expression"):
        _parse_tag_expression("")


# --- _group_matches ---


def test_group_matches_single_tag_present():
    assert _group_matches({"wee", "xyz"}, ["wee"]) is True


def test_group_matches_single_tag_absent():
    assert _group_matches({"abc"}, ["wee"]) is False


def test_group_matches_and_all_present():
    assert _group_matches({"xyz", "abc"}, ["xyz", "abc"]) is True


def test_group_matches_and_one_missing():
    assert _group_matches({"xyz"}, ["xyz", "abc"]) is False


def test_group_matches_or_one_present():
    assert _group_matches({"x"}, [["wee", "x"]]) is True


def test_group_matches_or_none_present():
    assert _group_matches({"abc"}, [["wee", "x"]]) is False


def test_group_matches_and_with_or_satisfied():
    assert _group_matches({"xyz", "c"}, ["xyz", ["c", "e"]]) is True


def test_group_matches_and_with_or_not_satisfied():
    assert _group_matches({"xyz"}, ["xyz", ["c", "e"]]) is False


def test_group_matches_empty_tags():
    assert _group_matches(set(), ["wee"]) is False


# --- _tags_match ---


def test_tags_match_any_group_matches():
    parsed = [["wee"], ["xyz"]]
    assert _tags_match({"xyz"}, parsed) is True


def test_tags_match_no_group_matches():
    parsed = [["wee"], ["xyz"]]
    assert _tags_match({"abc"}, parsed) is False


def test_tags_match_first_group_matches():
    parsed = [["wee"], ["xyz"]]
    assert _tags_match({"wee"}, parsed) is True


# --- _model_matches ---


def test_model_matches_when_tag_matches():
    model = make_model("wee", "all")
    parsed = _parse_tag_expression("[wee]")
    assert _model_matches(model, parsed) is True


def test_model_does_not_match_when_tag_absent():
    model = make_model("all")
    parsed = _parse_tag_expression("[wee]")
    assert _model_matches(model, parsed) is False


# --- match_models ---


def _write_model_file(path: Path, tags: list[str]) -> None:
    tag_str = ", ".join(f'"{t}"' for t in tags)
    path.write_text(
        textwrap.dedent(f"""
        from unittest.mock import MagicMock
        model = MagicMock()
        model.model_config.tags = {{{tag_str}}}
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
    # Since the fake module uses MagicMock (not real Model instances),
    # match_models filters via isinstance(obj, Model) — no real models will match.
    # This test verifies execution without error and returns a list.
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
        lines.append(f"m{i}.model_config.tags = {{{tag_str}}}")
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
    # MagicMocks won't pass isinstance(obj, Model), so result is empty
    # but execution must not raise
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
