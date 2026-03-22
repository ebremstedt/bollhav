from datetime import datetime, timezone
from unittest.mock import MagicMock
import pytest
from bollhav.model.model_type import ModelType
from bollhav.model.model import Model
from bollhav.model.schema import Schema
from bollhav.model.target import Target
from bollhav.model.bounds import Bounds
from bollhav.model.tags import Tags
from bollhav.model.write_modes import WriteMode


UTC = timezone.utc


def make_column(name: str, unique: bool = False, sensitive: bool = False) -> MagicMock:
    col = MagicMock()
    col.name = name
    col.unique = unique
    col.sensitive = sensitive
    return col


def make_db() -> MagicMock:
    return MagicMock()


def make_model(**overrides) -> Model:
    return Model(
        name=overrides.pop("name", "test_model"),
        target=overrides.pop("target", Target(name="test_table")),
        source=overrides.pop("source", None),
        **overrides,
    )


# --- Target validation ---


def test_view_model_type_requires_view_write_mode():
    with pytest.raises(ValueError, match="ModelType.VIEW must use WriteMode.VIEW"):
        Target(
            name="test_table", model_type=ModelType.VIEW, write_mode=WriteMode.APPEND
        )


def test_table_model_type_cannot_use_view_write_mode():
    with pytest.raises(ValueError, match="ModelType.TABLE cannot use WriteMode.VIEW"):
        Target(name="test_table", model_type=ModelType.TABLE, write_mode=WriteMode.VIEW)


def test_database_without_columns_raises():
    with pytest.raises(ValueError, match="columns must be set"):
        Target(name="test_table", database=make_db())


def test_columns_without_database_raises():
    with pytest.raises(ValueError, match="database must be set"):
        Target(name="test_table", columns=[make_column("id")])


def test_partitioned_by_unknown_column_raises():
    with pytest.raises(ValueError, match="unknown column"):
        Target(
            name="test_table",
            database=make_db(),
            columns=[make_column("id"), make_column("ts")],
            partitioned_by="missing",
        )


def test_upsert_no_delete_without_unique_columns_raises():
    with pytest.raises(
        ValueError, match="requires at least one column with unique=True"
    ):
        Target(
            name="test_table",
            database=make_db(),
            columns=[make_column("id"), make_column("ts")],
            write_mode=WriteMode.UPSERT_NO_DELETE,
        )


# --- Bounds ---


def test_bounds_stores_begin_end():
    begin = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 12, 31, tzinfo=UTC)
    b = Bounds(begin=begin, end=end)
    assert b.begin == begin
    assert b.end == end


# --- Model defaults ---


def test_defaults():
    m = make_model()
    assert m.source is None
    assert m.target.name == "test_table"
    assert m.target.schema.resolved == ""
    assert m.target.database is None
    assert m.target.columns == []
    assert m.target.model_type == ModelType.TABLE
    assert m.target.write_mode == WriteMode.APPEND
    assert m.enabled is True
    assert m.debug is False
    assert m.batching.default == "0 0 * * *"
    assert m.target.sensitive is False
    assert m.target.unique_columns == []
    assert m.batching.retries is None
    assert m.batching.lookback is None


# --- Tags ---


def test_name_added_to_tags_by_default():
    m = make_model()
    assert "test_model" in m.tags


def test_schema_added_to_tags_by_default():
    m = make_model(target=Target(name="test_table", schema=Schema(name="my_schema")))
    assert "my_schema" in m.tags


def test_all_tag_added_by_default():
    m = make_model()
    assert "all" in m.tags


def test_name_not_added_to_tags_when_disabled():
    m = make_model(tagging=Tags(name_add_to_tags=False))
    assert "test_model" not in m.tags


def test_schema_not_added_to_tags_when_disabled():
    m = make_model(
        target=Target(name="test_table", schema=Schema(name="my_schema")),
        tagging=Tags(schema_add_to_tags=False),
    )
    assert "my_schema" not in m.tags


def test_all_tag_not_added_when_disabled():
    m = make_model(tagging=Tags(model_gets_all_tag=False))
    assert "all" not in m.tags


# --- Target columns ---


def test_sensitive_flag_set_from_columns():
    t = Target(
        name="test_table",
        database=make_db(),
        columns=[make_column("id"), make_column("secret", sensitive=True)],
    )
    assert t.sensitive is True


def test_sensitive_false_when_no_sensitive_columns():
    t = Target(
        name="test_table",
        database=make_db(),
        columns=[make_column("id"), make_column("name")],
    )
    assert t.sensitive is False


def test_unique_columns_extracted():
    t = Target(
        name="test_table",
        database=make_db(),
        columns=[make_column("id", unique=True), make_column("name")],
    )
    assert len(t.unique_columns) == 1
    assert t.unique_columns[0].name == "id"


def test_upsert_no_delete_with_unique_column_ok():
    t = Target(
        name="test_table",
        database=make_db(),
        columns=[make_column("id", unique=True), make_column("name")],
        write_mode=WriteMode.UPSERT_NO_DELETE,
    )
    assert t.write_mode == WriteMode.UPSERT_NO_DELETE


# --- Column sorting ---


def test_column_sorting_applied():
    def custom_sort(names: list[str]) -> list[str]:
        return sorted(names, reverse=True)

    t = Target(
        name="test_table",
        database=make_db(),
        columns=[make_column("b"), make_column("a")],
        column_sorting=custom_sort,
    )
    assert [c.name for c in t.columns] == ["b", "a"]


def test_column_sorting_none_skips_sort():
    t = Target(
        name="test_table",
        database=make_db(),
        columns=[make_column("b"), make_column("a")],
        column_sorting=None,
    )
    assert [c.name for c in t.columns] == ["b", "a"]


# --- Partitioned by ---


def test_partitioned_by_sets_index_true():
    t = Target(
        name="test_table",
        database=make_db(),
        columns=[make_column("id"), make_column("ts")],
        partitioned_by="ts",
        column_sorting=None,
    )
    assert t.partitioned_by == "ts"
    assert t.partitioned_by_index is True


def test_partitioned_by_none_sets_index_false():
    t = Target(name="test_table")
    assert t.partitioned_by_index is False


# --- Extra kwargs ---


def test_extra_kwargs_stored():
    m = make_model(custom_key="custom_value")
    assert m.extra["custom_key"] == "custom_value"


def test_callable_extra_kwarg_resolved():
    m = make_model(
        static_val="hello",
        dynamic_val=lambda static_val: static_val + "_world",
    )
    assert m.extra["dynamic_val"] == "hello_world"


# --- __repr__ ---


def test_repr_contains_name():
    m = make_model()
    assert "test_model" in repr(m)


# --- __eq__ ---


def test_eq_same_config():
    m1 = make_model()
    m2 = make_model()
    assert m1 == m2


def test_eq_different_config():
    m1 = make_model(name="a")
    m2 = make_model(name="b")
    assert m1 != m2


def test_eq_non_model():
    m = make_model()
    assert m.__eq__("not a model") == NotImplemented
