from datetime import datetime, timezone
from unittest.mock import MagicMock
import pytest
import sys

roskarl_mock = MagicMock()
cron_mock = MagicMock()
sys.modules["roskarl"] = roskarl_mock
sys.modules["icron"] = MagicMock()
sys.modules["cron"] = cron_mock

from bollhav.model.database import Database  # noqa: E402
from bollhav.postgres.data import PostgresData  # noqa: E402
from bollhav.postgres.schema import _col_ddl, ensure_schema  # noqa: E402
from bollhav.postgres.modes import (  # noqa: E402
    append,
    recreate_partition,
    upsert_no_delete,
)
from bollhav.postgres.columns import PostgresColumn, PostgresType  # noqa: E402
from bollhav.model.model import Model  # noqa: E402
from bollhav.model.source_table import SourceTable  # noqa: E402
from bollhav.model.target import Target  # noqa: E402
from bollhav.model.target_schema import TargetSchema  # noqa: E402
from bollhav.model.write_modes import WriteMode  # noqa: E402
from bollhav.model.model_type import ModelType  # noqa: E402


def _col(
    name: str,
    data_type: PostgresType = PostgresType.TEXT,
    nullable: bool = True,
    primary_key: bool = False,
    unique: bool = False,
    precision: int | None = None,
    scale: int | None = None,
    length: int | None = None,
    partition_on: bool = False,
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
        partition_on=partition_on,
    )


def _model(
    write_mode: WriteMode = WriteMode.APPEND,
    columns: list[PostgresColumn] | None = None,
    source_query: str | None = None,
    recreate_table: bool = False,
    truncate_table: bool = False,
) -> Model:
    cols = columns or [_col("id"), _col("val")]
    return Model(
        source=SourceTable(name="src", query=source_query),
        target=Target(
            name="test_table",
            schema=TargetSchema(name="test_schema"),
            database=Database.POSTGRES,
            columns=cols,
            write_mode=write_mode,
            model_type=ModelType.TABLE,
            recreate_table=recreate_table,
            truncate_table=truncate_table,
        ),
    )


def _conn() -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
    conn.transaction.return_value.__exit__ = MagicMock(return_value=False)
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
        assert "UNIQUE" not in result

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


class TestEnsureAssets:
    """`run_pre_model_actions` is gone — target asset DDL now runs as the
    discrete steps `PostgresData.ensure_assets()` drives (the lifecycle
    hook calls the same methods). These assert the same DDL the old
    pre-model action runner produced: schema + table, plus index / unique
    constraint when the columns call for them."""

    def test_executes(self) -> None:
        conn = _conn()
        PostgresData(model=_model(), conn=conn).ensure_assets()
        conn.execute.assert_called()

    def test_creates_index_when_partitioned(self) -> None:
        conn = _conn()
        model = _model(columns=[_col("id"), _col("ts", partition_on=True)])
        PostgresData(model=model, conn=conn).ensure_assets()
        # CREATE SCHEMA + CREATE TABLE + CREATE INDEX.
        assert conn.execute.call_count == 3

    def test_creates_composite_unique_constraint(self) -> None:
        conn = _conn()
        model = _model(
            columns=[_col("a", unique=True), _col("b", unique=True), _col("val")]
        )
        PostgresData(model=model, conn=conn).ensure_assets()
        # CREATE SCHEMA + CREATE TABLE + ALTER TABLE ADD UNIQUE.
        assert conn.execute.call_count == 3


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
        from bollhav.model.database import Database
        from bollhav.model.target_schema import TargetSchema
        from bollhav.model.target import Target

        with pytest.raises(ValueError, match="partition_on"):
            Target(
                name="t",
                schema=TargetSchema(name="s"),
                database=Database.POSTGRES,
                columns=[_col("id"), _col("val")],
                write_mode=WriteMode.RECREATE_PARTITION,
            )

    def test_raises_non_utc_since(self) -> None:
        conn = _conn()
        import polars as pl

        df = pl.DataFrame({"id": [1], "val": ["a"]})
        since = datetime(2024, 1, 1)
        until = datetime(2024, 1, 2, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="UTC"):
            recreate_partition(
                conn=conn, model=_model(), df=df, since=since, until=until
            )

    def test_raises_non_utc_until(self) -> None:
        conn = _conn()
        import polars as pl

        df = pl.DataFrame({"id": [1], "val": ["a"]})
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 1, 2)
        with pytest.raises(ValueError, match="UTC"):
            recreate_partition(
                conn=conn, model=_model(), df=df, since=since, until=until
            )


class TestRecreateTable:
    def test_drops_before_create_when_recreate_table(self) -> None:
        # The lifecycle runs `recreate_table()` (the destructive DROP)
        # before `create_table()` for a `recreate_table=True` model.
        conn = _conn()
        pg = PostgresData(model=_model(recreate_table=True), conn=conn)
        pg.recreate_table()
        pg.create_table()
        statements = [str(call.args[0]) for call in conn.execute.call_args_list]
        assert any("DROP TABLE" in s for s in statements), statements
        assert any("CREATE TABLE" in s for s in statements), statements
        # DROP precedes CREATE.
        drop_idx = next(i for i, s in enumerate(statements) if "DROP TABLE" in s)
        create_idx = next(i for i, s in enumerate(statements) if "CREATE TABLE" in s)
        assert drop_idx < create_idx, statements


class TestTruncateTable:
    def test_truncates_after_create_when_truncate_table(self) -> None:
        # The lifecycle runs `create_table()` then `truncate_table()` for a
        # `truncate_table=True` model.
        conn = _conn()
        pg = PostgresData(model=_model(truncate_table=True), conn=conn)
        pg.create_table()
        pg.truncate_table()
        statements = [str(call.args[0]) for call in conn.execute.call_args_list]
        assert any("CREATE TABLE" in s for s in statements), statements
        assert any("TRUNCATE TABLE" in s for s in statements), statements
        create_idx = next(i for i, s in enumerate(statements) if "CREATE TABLE" in s)
        truncate_idx = next(
            i for i, s in enumerate(statements) if "TRUNCATE TABLE" in s
        )
        assert create_idx < truncate_idx, statements


class TestUpdateInsert:
    def test_requires_unique_columns(self) -> None:
        conn = _conn()
        import polars as pl

        cols = [
            _col("id", primary_key=True, nullable=False, unique=True),
            _col("val"),
        ]
        model = Model(
            source=SourceTable(name="src"),
            target=Target(
                name="test_table",
                schema=TargetSchema(name="test_schema"),
                columns=cols,
                database=Database.POSTGRES,
                write_mode=WriteMode.UPSERT_NO_DELETE,
                model_type=ModelType.TABLE,
            ),
        )
        df = pl.DataFrame({"id": [1], "val": ["a"]})
        copy_mock = MagicMock()
        copy_mock.__enter__ = MagicMock(return_value=copy_mock)
        copy_mock.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value.copy.return_value = copy_mock
        upsert_no_delete(conn=conn, model=model, df=df)
        assert conn.transaction.call_count == 1
