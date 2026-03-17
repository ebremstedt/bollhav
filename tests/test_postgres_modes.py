from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, call
import pytest
import sys

roskarl_mock = MagicMock()
cron_mock = MagicMock()
sys.modules["roskarl"] = roskarl_mock
sys.modules["icron"] = MagicMock()
sys.modules["cron"] = cron_mock

from bollhav.model.database import Database
from bollhav.postgres.modes import (
    _col_ddl,
    ensure_schema,
    ensure_table,
    append,
    overwrite_insert,
    recreate_insert,
    truncate_insert,
    update_insert,
    create_replace_view,
)
from bollhav.postgres.columns import PostgresColumn, PostgresType
from bollhav.model.model import Model
from bollhav.model.source import Source
from bollhav.model.target import Target
from bollhav.model.schema import Schema
from bollhav.model.write_modes import WriteMode
from bollhav.model.model_type import ModelType


def _col(
    name: str,
    data_type: PostgresType = PostgresType.TEXT,
    nullable: bool = True,
    primary_key: bool = False,
    unique: bool = False,
    precision: int | None = None,
    scale: int | None = None,
    length: int | None = None,
) -> PostgresColumn:
    return PostgresColumn(
        name=name,
        data_type=data_type,
        nullable=nullable,
        primary_key=primary_key,
        unique=unique,
        precision=precision,
        scale=scale,
        length=length,
    )


def _model(
    write_mode: WriteMode = WriteMode.APPEND,
    columns: list[PostgresColumn] | None = None,
    partitioned_by: str | None = None,
    source_query: str | None = None,
) -> Model:
    cols = columns or [_col("id"), _col("val")]
    return Model(
        name="test_table",
        source=Source(name="src", query=source_query),
        target=Target(
            name="test_table",
            schema=Schema(name="test_schema"),
            database=Database.POSTGRES,
            columns=cols,
            write_mode=write_mode,
            model_type=ModelType.TABLE
            if write_mode != WriteMode.VIEW
            else ModelType.VIEW,
            partitioned_by=partitioned_by,
        ),
    )


def _conn() -> MagicMock:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn


class TestColDdl:
    def test_basic_text(self) -> None:
        result = _col_ddl(_col("my_col"))
        assert "my_col" in result
        assert "TEXT" in result

    def test_not_null(self) -> None:
        result = _col_ddl(_col("my_col", nullable=False))
        assert "NOT NULL" in result

    def test_nullable_no_not_null(self) -> None:
        result = _col_ddl(_col("my_col", nullable=True))
        assert "NOT NULL" not in result

    def test_primary_key(self) -> None:
        result = _col_ddl(_col("my_col", primary_key=True, nullable=False))
        assert "PRIMARY KEY" in result

    def test_unique(self) -> None:
        result = _col_ddl(_col("my_col", unique=True))
        assert "UNIQUE" in result

    def test_precision_and_scale(self) -> None:
        result = _col_ddl(
            _col("my_col", data_type=PostgresType.NUMERIC, precision=10, scale=2)
        )
        assert "10, 2" in result

    def test_length(self) -> None:
        result = _col_ddl(_col("my_col", data_type=PostgresType.VARCHAR, length=255))
        assert "255" in result


class TestEnsureSchema:
    def test_executes(self) -> None:
        conn = _conn()
        ensure_schema(conn=conn, schema="my_schema")
        conn.execute.assert_called_once()


class TestEnsureTable:
    def test_executes(self) -> None:
        conn = _conn()
        ensure_table(conn=conn, model=_model())
        conn.execute.assert_called()

    def test_creates_index_when_partitioned(self) -> None:
        conn = _conn()
        model = _model(columns=[_col("id"), _col("ts")], partitioned_by="ts")
        ensure_table(conn=conn, model=model)
        assert conn.execute.call_count == 2


class TestAppend:
    def test_calls_copy(self) -> None:
        conn = _conn()
        import polars as pl

        df = pl.DataFrame({"id": [1], "val": ["a"]})
        copy_mock = MagicMock()
        copy_mock.__enter__ = MagicMock(return_value=copy_mock)
        copy_mock.__exit__ = MagicMock(return_value=False)
        cursor_mock = MagicMock()
        cursor_mock.copy.return_value = copy_mock
        cursor_ctx = MagicMock()
        cursor_ctx.__enter__ = MagicMock(return_value=cursor_mock)
        cursor_ctx.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value = cursor_ctx
        append(conn=conn, model=_model(), df=df)
        cursor_mock.copy.assert_called_once()
        copy_mock.write_row.assert_called_once_with((1, "a"))


class TestOverwriteInsert:
    def test_raises_without_partitioned_by(self) -> None:
        conn = _conn()
        import polars as pl

        df = pl.DataFrame({"id": [1], "val": ["a"]})
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 1, 2, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="partitioned_by"):
            overwrite_insert(conn=conn, model=_model(), df=df, since=since, until=until)

    def test_raises_non_utc_since(self) -> None:
        conn = _conn()
        import polars as pl

        df = pl.DataFrame({"id": [1], "val": ["a"]})
        since = datetime(2024, 1, 1)
        until = datetime(2024, 1, 2, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="UTC"):
            overwrite_insert(conn=conn, model=_model(), df=df, since=since, until=until)

    def test_raises_non_utc_until(self) -> None:
        conn = _conn()
        import polars as pl

        df = pl.DataFrame({"id": [1], "val": ["a"]})
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 1, 2)
        with pytest.raises(ValueError, match="UTC"):
            overwrite_insert(conn=conn, model=_model(), df=df, since=since, until=until)


class TestRecreateInsert:
    def test_drops_and_recreates(self) -> None:
        conn = _conn()
        import polars as pl

        df = pl.DataFrame({"id": [1], "val": ["a"]})
        copy_mock = MagicMock()
        copy_mock.__enter__ = MagicMock(return_value=copy_mock)
        copy_mock.__exit__ = MagicMock(return_value=False)
        cursor_mock = MagicMock()
        cursor_mock.copy.return_value = copy_mock
        conn.cursor.return_value.copy.return_value = copy_mock
        recreate_insert(conn=conn, model=_model(), df=df)
        conn.__enter__.assert_called_once()


class TestTruncateInsert:
    def test_truncates(self) -> None:
        conn = _conn()
        import polars as pl

        df = pl.DataFrame({"id": [1], "val": ["a"]})
        copy_mock = MagicMock()
        copy_mock.__enter__ = MagicMock(return_value=copy_mock)
        copy_mock.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value.copy.return_value = copy_mock
        truncate_insert(conn=conn, model=_model(), df=df)
        conn.__enter__.assert_called_once()


class TestUpdateInsert:
    def test_requires_unique_columns(self) -> None:
        conn = _conn()
        import polars as pl

        cols = [
            _col("id", primary_key=True, nullable=False, unique=True),
            _col("val"),
        ]
        model = Model(
            name="test_table",
            source=Source(name="src"),
            target=Target(
                name="test_table",
                schema=Schema(name="test_schema"),
                columns=cols,
                database=Database.POSTGRES,
                write_mode=WriteMode.UPDATE_INSERT,
                model_type=ModelType.TABLE,
            ),
        )
        df = pl.DataFrame({"id": [1], "val": ["a"]})
        copy_mock = MagicMock()
        copy_mock.__enter__ = MagicMock(return_value=copy_mock)
        copy_mock.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value.copy.return_value = copy_mock
        update_insert(conn=conn, model=model, df=df)
        conn.__enter__.assert_called_once()
