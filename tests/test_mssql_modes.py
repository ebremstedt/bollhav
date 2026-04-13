from unittest.mock import MagicMock
import sys
import polars as pl
import pyodbc

roskarl_mock = MagicMock()
sys.modules["roskarl"] = roskarl_mock
sys.modules["icron"] = MagicMock()

from bollhav.mssql.modes import _bulk_insert  # noqa: E402


def _cursor() -> MagicMock:
    return MagicMock(spec=pyodbc.Cursor)


class TestBulkInsertSetInputSizes:
    def test_string_column_uses_sql_wvarchar(self) -> None:
        cursor = _cursor()
        df = pl.DataFrame({"name": ["alice", "bob"]})
        _bulk_insert(cursor, "#tmp", ["name"], df)
        sizes = cursor.setinputsizes.call_args[0][0]
        assert sizes[0][0] == pyodbc.SQL_WVARCHAR

    def test_binary_column_uses_sql_varbinary(self) -> None:
        cursor = _cursor()
        df = pl.DataFrame({"data": [b"\x00\x01\x02", b"\xff\xfe"]})
        _bulk_insert(cursor, "#tmp", ["data"], df)
        sizes = cursor.setinputsizes.call_args[0][0]
        assert sizes[0][0] == pyodbc.SQL_VARBINARY
        assert sizes[0][1] == 3  # max length of the two values

    def test_binary_column_all_nulls(self) -> None:
        cursor = _cursor()
        df = pl.DataFrame({"data": [None, None]}, schema={"data": pl.Binary})
        _bulk_insert(cursor, "#tmp", ["data"], df)
        sizes = cursor.setinputsizes.call_args[0][0]
        assert sizes[0][0] == pyodbc.SQL_VARBINARY
        assert sizes[0][1] == 1  # fallback minimum

    def test_int_column_passes_none(self) -> None:
        cursor = _cursor()
        df = pl.DataFrame({"id": [1, 2]})
        _bulk_insert(cursor, "#tmp", ["id"], df)
        sizes = cursor.setinputsizes.call_args[0][0]
        assert sizes[0] is None

    def test_mixed_columns(self) -> None:
        cursor = _cursor()
        df = pl.DataFrame(
            {"id": [1], "name": ["alice"], "blob": [b"\x00"]},
        )
        _bulk_insert(cursor, "#tmp", ["id", "name", "blob"], df)
        sizes = cursor.setinputsizes.call_args[0][0]
        assert sizes[0] is None  # int
        assert sizes[1][0] == pyodbc.SQL_WVARCHAR  # string
        assert sizes[2][0] == pyodbc.SQL_VARBINARY  # binary
