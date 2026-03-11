from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
import pytest
from bollhav.model_type import ModelType
from bollhav.model.model_config import ModelConfig
from bollhav.write_modes import WriteMode


UTC = timezone.utc


def make_column(name: str, unique: bool = False, sensitive: bool = False) -> MagicMock:
    col = MagicMock()
    col.name = name
    col.unique = unique
    col.sensitive = sensitive
    return col


def make_db() -> MagicMock:
    return MagicMock()


def base_kwargs(**overrides) -> dict:
    return {"name": "test_model", "source_entity": "test_source", **overrides}


# --- Validation errors ---


def test_view_model_type_requires_view_write_mode():
    with pytest.raises(ValueError, match="ModelType.VIEW must use WriteMode.VIEW"):
        ModelConfig(
            **base_kwargs(model_type=ModelType.VIEW, write_mode=WriteMode.APPEND)
        )


def test_table_model_type_cannot_use_view_write_mode():
    with pytest.raises(ValueError, match="ModelType.TABLE cannot use WriteMode.VIEW"):
        ModelConfig(
            **base_kwargs(model_type=ModelType.TABLE, write_mode=WriteMode.VIEW)
        )


def test_database_without_columns_raises():
    with pytest.raises(ValueError, match="columns must be set"):
        ModelConfig(**base_kwargs(database=make_db()))


def test_columns_without_database_raises():
    with pytest.raises(ValueError, match="database must be set"):
        ModelConfig(**base_kwargs(columns=[make_column("id")]))


def test_partitioned_by_unknown_column_raises():
    db = make_db()
    cols = [make_column("id"), make_column("ts")]
    with pytest.raises(ValueError, match="unknown column"):
        ModelConfig(**base_kwargs(database=db, columns=cols, partitioned_by="missing"))


def test_update_insert_without_unique_columns_raises():
    db = make_db()
    cols = [make_column("id"), make_column("ts")]
    with pytest.raises(
        ValueError, match="requires at least one column with unique=True"
    ):
        ModelConfig(
            **base_kwargs(database=db, columns=cols, write_mode=WriteMode.UPDATE_INSERT)
        )


def test_tz_aware_begin_naive_raises():
    with pytest.raises(ValueError, match="begin must be UTC-aware"):
        ModelConfig(**base_kwargs(begin=datetime(2024, 1, 1)))


def test_tz_aware_begin_non_utc_raises():
    non_utc = datetime(2024, 1, 1, tzinfo=timezone(timedelta(hours=2)))
    with pytest.raises(ValueError, match="begin must be UTC-aware"):
        ModelConfig(**base_kwargs(begin=non_utc))


def test_tz_aware_end_naive_raises():
    with pytest.raises(ValueError, match="end must be UTC-aware"):
        ModelConfig(**base_kwargs(end=datetime(2024, 1, 1)))


def test_tz_aware_false_allows_naive_begin():
    m = ModelConfig(**base_kwargs(tz_aware=False, begin=datetime(2024, 1, 1)))
    assert m.begin == datetime(2024, 1, 1)


# --- Defaults ---


def test_defaults():
    m = ModelConfig(**base_kwargs())
    assert m.table == ""
    assert m.schema == ""
    assert m.database is None
    assert m.columns == []
    assert m.model_type == ModelType.TABLE
    assert m.write_mode == WriteMode.APPEND
    assert m.enabled is True
    assert m.debug is False
    assert m.cron is None
    assert m.batch_size is None
    assert m.sensitive is False
    assert m.unique_columns == []
    assert m.tz_aware is True
    assert m.retries is None
    assert m.lookback is None


# --- Tags ---


def test_name_added_to_tags_by_default():
    m = ModelConfig(**base_kwargs())
    assert "test_model" in m.tags


def test_schema_added_to_tags_by_default():
    m = ModelConfig(**base_kwargs(schema="my_schema"))
    assert "my_schema" in m.tags


def test_all_tag_added_by_default():
    m = ModelConfig(**base_kwargs())
    assert "all" in m.tags


def test_name_not_added_to_tags_when_disabled():
    m = ModelConfig(**base_kwargs(name_add_to_tags=False))
    assert "test_model" not in m.tags


def test_schema_not_added_to_tags_when_disabled():
    m = ModelConfig(**base_kwargs(schema="my_schema", schema_add_to_tags=False))
    assert "my_schema" not in m.tags


def test_all_tag_not_added_when_disabled():
    m = ModelConfig(**base_kwargs(model_gets_all_tag=False))
    assert "all" not in m.tags


# --- Columns ---


def test_sensitive_flag_set_from_columns():
    db = make_db()
    cols = [make_column("id"), make_column("secret", sensitive=True)]
    m = ModelConfig(**base_kwargs(database=db, columns=cols))
    assert m.sensitive is True


def test_sensitive_false_when_no_sensitive_columns():
    db = make_db()
    cols = [make_column("id"), make_column("name")]
    m = ModelConfig(**base_kwargs(database=db, columns=cols))
    assert m.sensitive is False


def test_unique_columns_extracted():
    db = make_db()
    cols = [make_column("id", unique=True), make_column("name")]
    m = ModelConfig(**base_kwargs(database=db, columns=cols))
    assert len(m.unique_columns) == 1
    assert m.unique_columns[0].name == "id"


def test_update_insert_with_unique_column_ok():
    db = make_db()
    cols = [make_column("id", unique=True), make_column("name")]
    m = ModelConfig(
        **base_kwargs(database=db, columns=cols, write_mode=WriteMode.UPDATE_INSERT)
    )
    assert m.write_mode == WriteMode.UPDATE_INSERT


# --- Column sorting ---


def test_column_sorting_applied():
    db = make_db()
    cols = [make_column("b"), make_column("a")]
    reverse_sort: list[str] = []

    def custom_sort(names: list[str]) -> list[str]:
        return sorted(names, reverse=True)

    m = ModelConfig(
        **base_kwargs(database=db, columns=cols, column_sorting=custom_sort)
    )
    assert [c.name for c in m.columns] == ["b", "a"]


def test_column_sorting_none_skips_sort():
    db = make_db()
    cols = [make_column("b"), make_column("a")]
    m = ModelConfig(**base_kwargs(database=db, columns=cols, column_sorting=None))
    assert [c.name for c in m.columns] == ["b", "a"]


# --- Partitioned by ---


def test_partitioned_by_sets_index_true():
    db = make_db()
    cols = [make_column("id"), make_column("ts")]
    m = ModelConfig(
        **base_kwargs(
            database=db, columns=cols, partitioned_by="ts", column_sorting=None
        )
    )
    assert m.partitioned_by == "ts"
    assert m.partitioned_by_index is True


def test_partitioned_by_none_sets_index_false():
    m = ModelConfig(**base_kwargs())
    assert m.partitioned_by_index is False


# --- Batch size ---


def test_batch_size_inferred_from_cron():
    with patch(
        "bollhav.model.model_config.infer_batch_size", return_value=100
    ) as mock_infer:
        m = ModelConfig(**base_kwargs(cron="0 * * * *"))
        mock_infer.assert_called_once_with("0 * * * *")
        assert m.batch_size == 100


def test_batch_size_none_when_no_cron():
    m = ModelConfig(**base_kwargs())
    assert m.batch_size is None


# --- Extra kwargs ---


def test_extra_kwargs_stored():
    m = ModelConfig(**base_kwargs(custom_key="custom_value"))
    assert m.extra["custom_key"] == "custom_value"


def test_callable_extra_kwarg_resolved():
    resolved = ModelConfig(
        **base_kwargs(
            static_val="hello",
            dynamic_val=lambda static_val: static_val + "_world",
        )
    )
    assert resolved.extra["dynamic_val"] == "hello_world"


# --- __repr__ ---


def test_repr_contains_name():
    m = ModelConfig(**base_kwargs())
    assert "test_model" in repr(m)


# --- __eq__ ---


def test_eq_same_config():
    m1 = ModelConfig(**base_kwargs())
    m2 = ModelConfig(**base_kwargs())
    assert m1 == m2


def test_eq_different_config():
    m1 = ModelConfig(**base_kwargs(name="a"))
    m2 = ModelConfig(**base_kwargs(name="b"))
    assert m1 != m2


def test_eq_non_model_config():
    m = ModelConfig(**base_kwargs())
    assert m.__eq__("not a model") == NotImplemented


# --- UTC begin/end ---


def test_utc_begin_end_accepted():
    begin = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 12, 31, tzinfo=UTC)
    m = ModelConfig(**base_kwargs(begin=begin, end=end))
    assert m.begin == begin
    assert m.end == end
