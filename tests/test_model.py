from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from typing import Any

import pytest

from bollhav.modes import WriteMode, ModelType
from bollhav.model import Model  # adjust import path as needed


UTC = timezone.utc


def utc_dt(year: int, month: int, day: int, hour: int = 0) -> datetime:
    return datetime(year, month, day, hour, tzinfo=UTC)


def make_column(name: str, unique: bool = False, sensitive: bool = False) -> MagicMock:
    col = MagicMock()
    col.name = name
    col.unique = unique
    col.sensitive = sensitive
    return col


def make_db() -> MagicMock:
    return MagicMock()


def minimal_model(**overrides: Any) -> Model:
    defaults: dict[str, Any] = {
        "name": "test_model",
        "source_entity": "source",
    }
    defaults.update(overrides)
    return Model(**defaults)


# --- __init__ validation ---


class TestInitValidation:
    def test_view_model_type_requires_view_write_mode(self) -> None:
        with pytest.raises(ValueError, match="ModelType.VIEW must use WriteMode.VIEW"):
            minimal_model(model_type=ModelType.VIEW, write_mode=WriteMode.APPEND)

    def test_table_model_type_rejects_view_write_mode(self) -> None:
        with pytest.raises(
            ValueError, match="ModelType.TABLE cannot use WriteMode.VIEW"
        ):
            minimal_model(model_type=ModelType.TABLE, write_mode=WriteMode.VIEW)

    def test_database_without_columns_raises(self) -> None:
        with pytest.raises(ValueError, match="columns must be set"):
            minimal_model(database=make_db(), columns=None)

    def test_columns_without_database_raises(self) -> None:
        cols = [make_column("id")]
        with pytest.raises(ValueError, match="database must be set"):
            minimal_model(database=None, columns=cols)

    def test_partitioned_by_unknown_column_raises(self) -> None:
        db = make_db()
        cols = [make_column("id")]
        with pytest.raises(
            ValueError, match="Partitioned_by references unknown column"
        ):
            minimal_model(database=db, columns=cols, partitioned_by="nonexistent")

    def test_partitioned_by_known_column_ok(self) -> None:
        db = make_db()
        cols = [make_column("ts")]
        m = minimal_model(database=db, columns=cols, partitioned_by="ts")
        assert m.partitioned_by == "ts"
        assert m.partitioned_by_index is True

    def test_partitioned_by_index_false_when_not_set(self) -> None:
        m = minimal_model()
        assert m.partitioned_by_index is False

    def test_begin_naive_tz_aware_raises(self) -> None:
        with pytest.raises(ValueError, match="begin must be UTC-aware"):
            minimal_model(begin=datetime(2024, 1, 1))

    def test_begin_non_utc_raises(self) -> None:
        non_utc = timezone(timedelta(hours=2))
        with pytest.raises(ValueError, match="begin must be UTC-aware"):
            minimal_model(begin=datetime(2024, 1, 1, tzinfo=non_utc))

    def test_end_naive_tz_aware_raises(self) -> None:
        with pytest.raises(ValueError, match="end must be UTC-aware"):
            minimal_model(end=datetime(2024, 1, 1))

    def test_begin_end_utc_ok(self) -> None:
        m = minimal_model(begin=utc_dt(2024, 1, 1), end=utc_dt(2024, 2, 1))
        assert m.begin == utc_dt(2024, 1, 1)
        assert m.end == utc_dt(2024, 2, 1)

    def test_tz_aware_false_skips_begin_end_check(self) -> None:
        m = minimal_model(tz_aware=False, begin=datetime(2024, 1, 1))
        assert m.begin == datetime(2024, 1, 1)

    def test_update_insert_without_unique_columns_raises(self) -> None:
        db = make_db()
        cols = [make_column("id", unique=False)]
        with pytest.raises(
            ValueError, match="requires at least one column with unique=True"
        ):
            minimal_model(database=db, columns=cols, write_mode=WriteMode.UPDATE_INSERT)

    def test_update_insert_with_unique_column_ok(self) -> None:
        db = make_db()
        cols = [make_column("id", unique=True)]
        m = minimal_model(database=db, columns=cols, write_mode=WriteMode.UPDATE_INSERT)
        assert len(m.unique_columns) == 1


# --- sensitive / unique_columns derivation ---


class TestDerivedAttributes:
    def test_sensitive_true_when_any_column_sensitive(self) -> None:
        db = make_db()
        cols = [make_column("id"), make_column("secret", sensitive=True)]
        m = minimal_model(database=db, columns=cols)
        assert m.sensitive is True

    def test_sensitive_false_when_no_sensitive_columns(self) -> None:
        db = make_db()
        cols = [make_column("id"), make_column("name")]
        m = minimal_model(database=db, columns=cols)
        assert m.sensitive is False

    def test_sensitive_false_without_columns(self) -> None:
        m = minimal_model()
        assert m.sensitive is False

    def test_unique_columns_collected(self) -> None:
        db = make_db()
        cols = [make_column("id", unique=True), make_column("name")]
        m = minimal_model(database=db, columns=cols)
        assert len(m.unique_columns) == 1
        assert m.unique_columns[0].name == "id"

    def test_unique_columns_empty_without_columns(self) -> None:
        m = minimal_model()
        assert m.unique_columns == []

    @patch("bollhav.model.infer_batch_size", return_value=100)
    def test_batch_size_inferred_from_cron(self, mock_infer: MagicMock) -> None:
        m = minimal_model(cron="0 * * * *")
        assert m.batch_size == 100
        mock_infer.assert_called_once_with("0 * * * *")

    def test_batch_size_none_without_cron(self) -> None:
        m = minimal_model()
        assert m.batch_size is None


# --- column sorting ---


class TestColumnSorting:
    def test_columns_sorted_via_column_sorting(self) -> None:
        db = make_db()
        cols = [make_column("b"), make_column("a")]
        # sorting fn reverses the list
        m = minimal_model(
            database=db,
            columns=cols,
            column_sorting=lambda names: list(reversed(names)),
        )
        assert [c.name for c in m.columns] == ["a", "b"]

    def test_no_sorting_when_column_sorting_is_none(self) -> None:
        db = make_db()
        cols = [make_column("b"), make_column("a")]
        m = minimal_model(database=db, columns=cols, column_sorting=None)
        assert [c.name for c in m.columns] == ["b", "a"]


# --- kwargs / extra ---


class TestKwargsExtra:
    def test_non_callable_kwargs_stored_in_extra(self) -> None:
        m = minimal_model(foo="bar", baz=42)
        assert m.extra == {"foo": "bar", "baz": 42}

    def test_callable_kwargs_resolved(self) -> None:
        fn = MagicMock(return_value="resolved")
        m = minimal_model(my_val=fn, static="s")
        assert m.extra["my_val"] == "resolved"


# --- __repr__ ---


class TestRepr:
    def test_repr_contains_name(self) -> None:
        m = minimal_model(name="my_model")
        assert "my_model" in repr(m)

    def test_repr_contains_source_entity(self) -> None:
        m = minimal_model(source_entity="my_source")
        assert "my_source" in repr(m)


# --- __eq__ ---


class TestEq:
    def test_equal_models(self) -> None:
        m1 = minimal_model()
        m2 = minimal_model()
        assert m1 == m2

    def test_unequal_models(self) -> None:
        m1 = minimal_model(name="a")
        m2 = minimal_model(name="b")
        assert m1 != m2

    def test_eq_with_non_model_returns_not_implemented(self) -> None:
        m = minimal_model()
        assert m.__eq__("not a model") == NotImplemented


# --- get_batch_intervals ---


class TestGetBatchIntervals:
    def _make_interval(self, since: datetime, until: datetime) -> MagicMock:
        interval = MagicMock()
        interval.since = since
        interval.until = until
        return interval

    @patch("bollhav.model.croniter")
    def test_returns_intervals_split_by_cron_ticks(
        self, mock_croniter: MagicMock
    ) -> None:
        since = utc_dt(2024, 1, 1, 0)
        tick1 = utc_dt(2024, 1, 1, 1)
        until = utc_dt(2024, 1, 1, 2)
        mock_it = MagicMock()
        mock_it.get_next.side_effect = [tick1, until]
        mock_croniter.return_value = mock_it
        m = minimal_model(cron="0 * * * *")
        interval = self._make_interval(since, until)
        result = m.get_batch_intervals(interval)
        assert len(result) == 2
        assert result[0].since == since
        assert result[0].until == tick1
        assert result[1].since == tick1
        assert result[1].until == until

    @patch("bollhav.model.croniter")
    def test_cron_override_used_when_provided(self, mock_croniter: MagicMock) -> None:
        since = utc_dt(2024, 1, 1)
        until = utc_dt(2024, 1, 2)
        mock_it = MagicMock()
        mock_it.get_next.side_effect = [until]
        mock_croniter.return_value = mock_it
        m = minimal_model(cron="0 * * * *")
        interval = self._make_interval(since, until)
        m.get_batch_intervals(interval, cron_override="0 0 * * *")
        mock_croniter.assert_called_with("0 0 * * *", since)

    @patch("bollhav.model.croniter")
    def test_no_ticks_before_until_returns_single_interval(
        self, mock_croniter: MagicMock
    ) -> None:
        since = utc_dt(2024, 1, 1)
        until = utc_dt(2024, 1, 2)
        mock_it = MagicMock()
        mock_it.get_next.side_effect = [until]
        mock_croniter.return_value = mock_it
        m = minimal_model(cron="0 * * * *")
        interval = self._make_interval(since, until)
        result = m.get_batch_intervals(interval)
        assert len(result) == 1
        assert result[0].since == since
        assert result[0].until == until

    @patch("bollhav.model.croniter")
    def test_tick_exactly_at_until_not_included(self, mock_croniter: MagicMock) -> None:
        since = utc_dt(2024, 1, 1)
        until = utc_dt(2024, 1, 1, 6)
        tick = utc_dt(2024, 1, 1, 6)
        mock_it = MagicMock()
        mock_it.get_next.side_effect = [tick]
        mock_croniter.return_value = mock_it
        m = minimal_model(cron="0 * * * *")
        interval = self._make_interval(since, until)
        result = m.get_batch_intervals(interval)
        assert len(result) == 1
        assert result[0].since == since
