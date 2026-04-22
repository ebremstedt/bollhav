import textwrap
from pathlib import Path
from unittest.mock import MagicMock
import pytest

from bollhav.model.batch import ChunkMode
from bollhav.model.tagexpr import parse_expression
from bollhav.model.matching import _model_matches, match_models


def make_model(*tags: str) -> MagicMock:
    model = MagicMock()
    model.tags = set(tags)
    model.directives = MagicMock(
        reload=False,
        reload_mode=None,
        reload_batch_size=None,
        reload_interval_expression=None,
    )
    return model


# --- _model_matches ---


def test_model_matches_when_tag_matches():
    model = make_model("wee", "all")
    result = _model_matches(model, parse_expression("[wee]"))
    assert result is model
    assert result.directives.reload is False


def test_model_does_not_match_when_tag_absent():
    model = make_model("all")
    assert _model_matches(model, parse_expression("[wee]")) is None


def test_model_matches_with_reload_tag_level():
    model = make_model("sales")
    result = _model_matches(model, parse_expression("[r:sales]"))
    assert result is model
    assert result.directives.reload is True


def test_model_matches_with_reload_group_level():
    model = make_model("sales")
    result = _model_matches(model, parse_expression("r:[sales]"))
    assert result is model
    assert result.directives.reload is True


def test_model_matches_with_reload_paren_level():
    model = make_model("finance")
    result = _model_matches(model, parse_expression("[r:(sales|finance)]"))
    assert result is model
    assert result.directives.reload is True


def test_model_reload_true_if_any_matching_group_has_reload():
    model = make_model("sales")
    result = _model_matches(model, parse_expression("[r:sales][other]"))
    assert result is model
    assert result.directives.reload is True


def test_model_no_reload_when_matched_group_has_no_reload():
    model = make_model("finance")
    result = _model_matches(model, parse_expression("[r:sales][finance]"))
    assert result is model
    assert result.directives.reload is False


def test_disabled_model_does_not_match():
    model = make_model("wee", "all")
    model.enabled = False
    assert _model_matches(model, parse_expression("[wee]")) is None


# --- _model_matches propagates r_row_<N>: to directives ---


def test_model_matches_populates_row_mode_and_batch_size():
    model = make_model("vPAS")
    result = _model_matches(model, parse_expression("[r_row_100:vPAS]"))
    assert result is model
    assert result.directives.reload is True
    assert result.directives.reload_mode is ChunkMode.ROW
    assert result.directives.reload_batch_size == 100


def test_model_matches_group_level_r_row_propagates():
    model = make_model("vPAS")
    result = _model_matches(model, parse_expression("r_row_500:[vPAS]"))
    assert result.directives.reload_mode is ChunkMode.ROW
    assert result.directives.reload_batch_size == 500


def test_model_matches_plain_reload_leaves_row_fields_none():
    model = make_model("vPAS")
    result = _model_matches(model, parse_expression("[r:vPAS]"))
    assert result.directives.reload is True
    assert result.directives.reload_mode is None
    assert result.directives.reload_batch_size is None


def test_model_matches_no_reload_leaves_row_fields_none():
    model = make_model("vPAS")
    result = _model_matches(model, parse_expression("[vPAS]"))
    assert result.directives.reload is False
    assert result.directives.reload_mode is None
    assert result.directives.reload_batch_size is None


def test_model_matches_populates_interval_expression():
    model = make_model("foo")
    result = _model_matches(model, parse_expression("[r_interval_@daily:foo]"))
    assert result.directives.reload is True
    assert result.directives.reload_mode is ChunkMode.INTERVAL
    assert result.directives.reload_interval_expression == "@daily"
    assert result.directives.reload_batch_size is None


def test_model_matches_reload_word_alias_equivalent_to_r():
    model = make_model("foo")
    result = _model_matches(model, parse_expression("[reload_row_100:foo]"))
    assert result.directives.reload is True
    assert result.directives.reload_mode is ChunkMode.ROW
    assert result.directives.reload_batch_size == 100


def test_model_matches_runtime_row_upsert_no_delete_is_compatible():
    """UPSERT_NO_DELETE is one of the two write modes compatible with
    ROW — each chunk is an idempotent keyed upsert, so partial batches
    are fine (unlike truncate/recreate which assume full datasets)."""
    from unittest.mock import MagicMock

    from bollhav.model.database import Database
    from bollhav.model.model import Model
    from bollhav.model.schema import Schema
    from bollhav.model.tags import Tags
    from bollhav.model.target import Target
    from bollhav.model.write_modes import WriteMode
    from bollhav.model.batch import ChunkMode

    id_col = MagicMock(name="id", unique=True, sensitive=False)
    id_col.name = "id"

    m = Model(
        target=Target(
            name="dim_user",
            schema=Schema(name="s"),
            write_mode=WriteMode.UPSERT_NO_DELETE,
            database=Database.POSTGRES,
            columns=[id_col],
        ),
        tagging=Tags({"dim_user"}),
    )
    result = _model_matches(m, parse_expression("[r_row_100:dim_user]"))
    assert result is m
    assert result.directives.reload_mode is ChunkMode.ROW
    assert result.directives.reload_batch_size == 100


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
