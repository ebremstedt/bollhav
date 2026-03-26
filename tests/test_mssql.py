from unittest.mock import MagicMock, patch
import pytest
import sys

sys.modules.setdefault("roskarl", MagicMock())
sys.modules.setdefault("icron", MagicMock())
sys.modules.setdefault("cron", MagicMock())

import polars as pl  # noqa: E402

from bollhav.model.database import Database  # noqa: E402
from bollhav.model.model import Model  # noqa: E402
from bollhav.model.model_type import ModelType  # noqa: E402
from bollhav.model.schema import Schema  # noqa: E402
from bollhav.model.source import Source  # noqa: E402
from bollhav.model.target import Target  # noqa: E402
from bollhav.model.write_modes import WriteMode  # noqa: E402
from bollhav.mssql.columns import MssqlColumn, MssqlType  # noqa: E402
from bollhav.mssql.modes import create_replace_view, merge, truncate_write  # noqa: E402
from bollhav.mssql.schema import _b, _col_ddl, _col_type, ensure_schema, ensure_table  # noqa: E402
from bollhav.mssql.write_modes import write, write_dataframes  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _col(
    name: str,
    data_type: MssqlType = MssqlType.NVARCHAR,
    nullable: bool = True,
    primary_key: bool = False,
    unique: bool = False,
    length: int | None = None,
    precision: int | None = None,
    scale: int | None = None,
) -> MssqlColumn:
    return MssqlColumn(
        name=name,
        data_type=data_type,
        nullable=nullable,
        primary_key=primary_key,
        unique=unique,
        length=length,
        precision=precision,
        scale=scale,
    )


def _model(
    write_mode: WriteMode = WriteMode.TRUNCATE_TABLE_INSERT,
    columns: list[MssqlColumn] | None = None,
    source_query: str | None = None,
) -> Model:
    cols = columns or [_col("id", data_type=MssqlType.INT), _col("val")]
    return Model(
        name="test_table",
        source=Source(name="src", query=source_query),
        target=Target(
            name="test_table",
            schema=Schema(name="dbo"),
            database=Database.MSSQL,
            columns=cols,
            write_mode=write_mode,
            model_type=ModelType.TABLE
            if write_mode != WriteMode.VIEW
            else ModelType.VIEW,
        ),
    )


def _conn() -> MagicMock:
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


# ---------------------------------------------------------------------------
# _b
# ---------------------------------------------------------------------------


class TestBracket:
    def test_simple(self) -> None:
        assert _b("foo") == "[foo]"

    def test_escapes_closing_bracket(self) -> None:
        assert _b("fo]o") == "[fo]]o]"


# ---------------------------------------------------------------------------
# _col_type
# ---------------------------------------------------------------------------


class TestColType:
    def test_plain_type(self) -> None:
        assert _col_type(_col("x", data_type=MssqlType.INT)) == "INT"

    def test_nvarchar_default_max(self) -> None:
        assert _col_type(_col("x", data_type=MssqlType.NVARCHAR)) == "NVARCHAR(MAX)"

    def test_nvarchar_with_length(self) -> None:
        assert (
            _col_type(_col("x", data_type=MssqlType.NVARCHAR, length=100))
            == "NVARCHAR(100)"
        )

    def test_varchar_with_length(self) -> None:
        assert (
            _col_type(_col("x", data_type=MssqlType.VARCHAR, length=50))
            == "VARCHAR(50)"
        )

    def test_decimal_precision_scale(self) -> None:
        assert (
            _col_type(_col("x", data_type=MssqlType.DECIMAL, precision=10, scale=2))
            == "DECIMAL(10, 2)"
        )

    def test_decimal_precision_only(self) -> None:
        assert (
            _col_type(_col("x", data_type=MssqlType.DECIMAL, precision=18))
            == "DECIMAL(18)"
        )

    def test_datetime2_with_scale(self) -> None:
        assert (
            _col_type(_col("x", data_type=MssqlType.DATETIME2, scale=7))
            == "DATETIME2(7)"
        )

    def test_datetime2_without_scale(self) -> None:
        assert _col_type(_col("x", data_type=MssqlType.DATETIME2)) == "DATETIME2"


# ---------------------------------------------------------------------------
# _col_ddl
# ---------------------------------------------------------------------------


class TestColDdl:
    def test_basic(self) -> None:
        result = _col_ddl(_col("my_col", data_type=MssqlType.INT))
        assert "[my_col]" in result
        assert "INT" in result

    def test_not_null(self) -> None:
        result = _col_ddl(_col("x", nullable=False))
        assert "NOT NULL" in result

    def test_nullable_no_not_null(self) -> None:
        result = _col_ddl(_col("x", nullable=True))
        assert "NOT NULL" not in result

    def test_primary_key(self) -> None:
        result = _col_ddl(_col("x", primary_key=True, nullable=False))
        assert "PRIMARY KEY" in result

    def test_nvarchar_max(self) -> None:
        result = _col_ddl(_col("x", data_type=MssqlType.NVARCHAR))
        assert "NVARCHAR(MAX)" in result


# ---------------------------------------------------------------------------
# MssqlColumn validation
# ---------------------------------------------------------------------------


class TestMssqlColumn:
    def test_primary_key_nullable_raises(self) -> None:
        with pytest.raises(ValueError, match="primary_key"):
            MssqlColumn(name="x", primary_key=True, nullable=True)

    def test_primary_key_not_nullable_ok(self) -> None:
        col = MssqlColumn(name="x", primary_key=True, nullable=False)
        assert col.primary_key is True


# ---------------------------------------------------------------------------
# ensure_schema
# ---------------------------------------------------------------------------


class TestEnsureSchema:
    def test_executes_and_commits(self) -> None:
        conn = _conn()
        ensure_schema(conn=conn, schema="my_schema")
        cursor = conn.cursor.return_value
        cursor.execute.assert_called_once()
        cursor.commit.assert_called_once()

    def test_passes_schema_name_as_param(self) -> None:
        conn = _conn()
        ensure_schema(conn=conn, schema="my_schema")
        cursor = conn.cursor.return_value
        args = cursor.execute.call_args
        assert "my_schema" in args[0][1:]  # positional params tuple


# ---------------------------------------------------------------------------
# ensure_table
# ---------------------------------------------------------------------------


class TestEnsureTable:
    def test_executes_and_commits(self) -> None:
        conn = _conn()
        ensure_table(conn=conn, model=_model())
        cursor = conn.cursor.return_value
        cursor.execute.assert_called()
        cursor.commit.assert_called_once()

    def test_adds_unique_constraint_when_unique_cols(self) -> None:
        conn = _conn()
        model = _model(
            columns=[_col("id", data_type=MssqlType.INT, unique=True), _col("val")]
        )
        ensure_table(conn=conn, model=model)
        cursor = conn.cursor.return_value
        assert cursor.execute.call_count == 2  # CREATE TABLE + ALTER TABLE unique

    def test_no_unique_constraint_without_unique_cols(self) -> None:
        conn = _conn()
        ensure_table(conn=conn, model=_model())
        cursor = conn.cursor.return_value
        assert cursor.execute.call_count == 1


# ---------------------------------------------------------------------------
# truncate_write
# ---------------------------------------------------------------------------


class TestTruncateWrite:
    def test_truncates_then_inserts_then_commits(self) -> None:
        conn = _conn()
        df = pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        truncate_write(conn=conn, model=_model(), df=df)
        cursor = conn.cursor.return_value
        # First execute is TRUNCATE, then executemany for bulk insert
        first_call = cursor.execute.call_args_list[0][0][0]
        assert "TRUNCATE TABLE" in first_call
        cursor.executemany.assert_called_once()
        cursor.commit.assert_called_once()

    def test_bulk_insert_passes_all_rows(self) -> None:
        conn = _conn()
        df = pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        truncate_write(conn=conn, model=_model(), df=df)
        cursor = conn.cursor.return_value
        rows_passed = cursor.executemany.call_args[0][1]
        assert len(rows_passed) == 3


# ---------------------------------------------------------------------------
# merge
# ---------------------------------------------------------------------------


class TestMerge:
    def _merge_model(self) -> Model:
        return _model(
            write_mode=WriteMode.UPSERT_NO_DELETE,
            columns=[
                _col("id", data_type=MssqlType.INT, nullable=False, unique=True),
                _col("val"),
            ],
        )

    def test_creates_and_drops_temp_table(self) -> None:
        conn = _conn()
        df = pl.DataFrame({"id": [1], "val": ["x"]})
        merge(conn=conn, model=self._merge_model(), df=df)
        cursor = conn.cursor.return_value
        calls = [c[0][0] for c in cursor.execute.call_args_list]
        assert any("DROP TABLE" in c for c in calls)
        assert any("CREATE TABLE" in c for c in calls)

    def test_executes_merge_statement(self) -> None:
        conn = _conn()
        df = pl.DataFrame({"id": [1], "val": ["x"]})
        merge(conn=conn, model=self._merge_model(), df=df)
        cursor = conn.cursor.return_value
        calls = [c[0][0] for c in cursor.execute.call_args_list]
        assert any("MERGE INTO" in c for c in calls)

    def test_bulk_inserts_into_temp(self) -> None:
        conn = _conn()
        df = pl.DataFrame({"id": [1, 2], "val": ["x", "y"]})
        merge(conn=conn, model=self._merge_model(), df=df)
        cursor = conn.cursor.return_value
        cursor.executemany.assert_called_once()
        rows = cursor.executemany.call_args[0][1]
        assert len(rows) == 2

    def test_commits(self) -> None:
        conn = _conn()
        df = pl.DataFrame({"id": [1], "val": ["x"]})
        merge(conn=conn, model=self._merge_model(), df=df)
        conn.cursor.return_value.commit.assert_called_once()

    def test_merge_on_clause_uses_unique_col(self) -> None:
        conn = _conn()
        df = pl.DataFrame({"id": [1], "val": ["x"]})
        merge(conn=conn, model=self._merge_model(), df=df)
        cursor = conn.cursor.return_value
        merge_call = next(
            c[0][0] for c in cursor.execute.call_args_list if "MERGE INTO" in c[0][0]
        )
        assert "[id]" in merge_call
        assert "ON" in merge_call


# ---------------------------------------------------------------------------
# create_replace_view
# ---------------------------------------------------------------------------


class TestCreateReplaceView:
    def test_raises_without_source_query(self) -> None:
        conn = _conn()
        model = _model(write_mode=WriteMode.VIEW, source_query=None)
        with pytest.raises(ValueError, match="query"):
            create_replace_view(conn=conn, model=model)

    def test_executes_create_or_alter_view(self) -> None:
        conn = _conn()
        model = _model(write_mode=WriteMode.VIEW, source_query="SELECT 1 AS x")
        create_replace_view(conn=conn, model=model)
        cursor = conn.cursor.return_value
        sql_called = cursor.execute.call_args[0][0]
        assert "CREATE OR ALTER VIEW" in sql_called
        assert "SELECT 1 AS x" in sql_called

    def test_commits(self) -> None:
        conn = _conn()
        model = _model(write_mode=WriteMode.VIEW, source_query="SELECT 1")
        create_replace_view(conn=conn, model=model)
        conn.cursor.return_value.commit.assert_called_once()


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------


class TestWrite:
    def test_view_mode_calls_create_replace_view(self) -> None:
        conn = _conn()
        model = _model(write_mode=WriteMode.VIEW, source_query="SELECT 1")
        with patch("bollhav.mssql.write_modes.create_replace_view") as mock_crv:
            write(conn=conn, model=model)
            mock_crv.assert_called_once_with(conn=conn, model=model)

    def test_view_mode_with_df_gen_raises(self) -> None:
        conn = _conn()
        model = _model(write_mode=WriteMode.VIEW, source_query="SELECT 1")
        with pytest.raises(ValueError, match="does not take a dataframe"):
            write(conn=conn, model=model, df_gen=(x for x in [pl.DataFrame()]))

    def test_table_mode_without_df_gen_raises(self) -> None:
        conn = _conn()
        with pytest.raises(ValueError, match="requires a dataframe generator"):
            write(conn=conn, model=_model())

    def test_truncate_write_routed(self) -> None:
        conn = _conn()
        df = pl.DataFrame({"id": [1], "val": ["a"]})
        with (
            patch("bollhav.mssql.write_modes.truncate_write") as mock_tw,
            patch("bollhav.mssql.write_modes.ensure_schema_and_table"),
        ):
            write(conn=conn, model=_model(), df_gen=iter([df]))
            mock_tw.assert_called_once()

    def test_merge_routed(self) -> None:
        conn = _conn()
        df = pl.DataFrame({"id": [1], "val": ["a"]})
        model = _model(
            write_mode=WriteMode.UPSERT_NO_DELETE,
            columns=[
                _col("id", data_type=MssqlType.INT, nullable=False, unique=True),
                _col("val"),
            ],
        )
        with (
            patch("bollhav.mssql.write_modes.merge") as mock_merge,
            patch("bollhav.mssql.write_modes.ensure_schema_and_table"),
        ):
            write(conn=conn, model=model, df_gen=iter([df]))
            mock_merge.assert_called_once()


# ---------------------------------------------------------------------------
# write_dataframes
# ---------------------------------------------------------------------------


class TestWriteDataframes:
    def test_skips_empty_frames(self) -> None:
        conn = _conn()
        with (
            patch("bollhav.mssql.write_modes.truncate_write") as mock_tw,
            patch("bollhav.mssql.write_modes.ensure_schema_and_table"),
        ):
            write_dataframes(conn=conn, model=_model(), df_gen=iter([pl.DataFrame()]))
            mock_tw.assert_not_called()

    def test_raises_on_unsupported_mode(self) -> None:
        conn = _conn()
        model = _model()
        # Patch write_mode to something MSSQL doesn't handle
        model.target.write_mode = WriteMode.APPEND
        with patch("bollhav.mssql.write_modes.ensure_schema_and_table"):
            with pytest.raises(ValueError, match="Unhandled write mode"):
                write_dataframes(
                    conn=conn,
                    model=model,
                    df_gen=iter([pl.DataFrame({"id": [1], "val": ["a"]})]),
                )

    def test_create_if_missing_false_skips_ensure(self) -> None:
        conn = _conn()
        df = pl.DataFrame({"id": [1], "val": ["a"]})
        with (
            patch("bollhav.mssql.write_modes.truncate_write"),
            patch("bollhav.mssql.write_modes.ensure_schema_and_table") as mock_ensure,
        ):
            write_dataframes(
                conn=conn, model=_model(), df_gen=iter([df]), create_if_missing=False
            )
            mock_ensure.assert_not_called()
