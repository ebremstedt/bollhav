"""Tests for `_input_size_for` — the per-column setinputsizes tuple builder.

These tests need the *real* pyodbc package, not the MagicMock substitute used
by the other mssql test files (because we're asserting that the SQL_TYPE
constants in the returned tuples match pyodbc's actual integer values, and a
MagicMock-mock'd `pyodbc.SQL_WVARCHAR` is just a MagicMock instance).

Skipped automatically when real pyodbc isn't importable, so CI runners without
the libodbc system library don't break.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

# Reject a mock substitute that another test file may have already injected
# into sys.modules — we need the real package here.
_existing = sys.modules.get("pyodbc")
if isinstance(_existing, MagicMock):
    pytest.skip("real pyodbc not installed (got MagicMock)", allow_module_level=True)

try:
    import pyodbc
except ImportError:
    pytest.skip("pyodbc not installed", allow_module_level=True)

from bollhav.mssql.columns import MssqlColumn, MssqlType  # noqa: E402
from bollhav.mssql.modes import _input_size_for  # noqa: E402


def _col(data_type: MssqlType, **kwargs) -> MssqlColumn:
    return MssqlColumn(name="x", data_type=data_type, **kwargs)


class TestVarcharFamily:
    def test_nvarchar_bounded(self) -> None:
        assert _input_size_for(_col(MssqlType.NVARCHAR, length=4000)) == (
            pyodbc.SQL_WVARCHAR,
            4000,
            0,
        )

    def test_nvarchar_max(self) -> None:
        # length=None means NVARCHAR(MAX); we have to fall back to streaming
        # but it should still be an explicit tuple, not bare 0.
        assert _input_size_for(_col(MssqlType.NVARCHAR, length=None)) == (
            pyodbc.SQL_WVARCHAR,
            0,
            0,
        )

    def test_varchar_bounded(self) -> None:
        assert _input_size_for(_col(MssqlType.VARCHAR, length=200)) == (
            pyodbc.SQL_VARCHAR,
            200,
            0,
        )

    def test_varchar_max(self) -> None:
        assert _input_size_for(_col(MssqlType.VARCHAR, length=None)) == (
            pyodbc.SQL_VARCHAR,
            0,
            0,
        )

    def test_char_bounded(self) -> None:
        assert _input_size_for(_col(MssqlType.CHAR, length=10)) == (
            pyodbc.SQL_CHAR,
            10,
            0,
        )

    def test_char_unset_defaults_to_one(self) -> None:
        # CHAR with no explicit length is unusual, but we shouldn't pass 0 —
        # default to length 1.
        assert _input_size_for(_col(MssqlType.CHAR, length=None)) == (
            pyodbc.SQL_CHAR,
            1,
            0,
        )


class TestDateAndTimeFamily:
    def test_date(self) -> None:
        assert _input_size_for(_col(MssqlType.DATE)) == (pyodbc.SQL_TYPE_DATE, 10, 0)

    def test_time_default_scale(self) -> None:
        # Default scale=7 (MSSQL max). column_size = 8 + (7+1) = 16.
        assert _input_size_for(_col(MssqlType.TIME)) == (pyodbc.SQL_TYPE_TIME, 16, 7)

    def test_time_explicit_scale(self) -> None:
        # TIME(3): column_size = 8 + (3+1) = 12, scale = 3.
        assert _input_size_for(_col(MssqlType.TIME, scale=3)) == (
            pyodbc.SQL_TYPE_TIME,
            12,
            3,
        )

    def test_time_scale_zero(self) -> None:
        # TIME(0): column_size = 8 (no decimal point), scale = 0.
        assert _input_size_for(_col(MssqlType.TIME, scale=0)) == (
            pyodbc.SQL_TYPE_TIME,
            8,
            0,
        )

    def test_datetime(self) -> None:
        # Plain DATETIME has fixed scale=3, not user-settable.
        assert _input_size_for(_col(MssqlType.DATETIME)) == (
            pyodbc.SQL_TYPE_TIMESTAMP,
            23,
            3,
        )

    def test_datetime2_default_scale(self) -> None:
        # Default scale=7. column_size = 19 + (7+1) = 27.
        assert _input_size_for(_col(MssqlType.DATETIME2)) == (
            pyodbc.SQL_TYPE_TIMESTAMP,
            27,
            7,
        )

    def test_datetime2_explicit_scale(self) -> None:
        # DATETIME2(3): column_size = 19 + (3+1) = 23, scale = 3.
        assert _input_size_for(_col(MssqlType.DATETIME2, scale=3)) == (
            pyodbc.SQL_TYPE_TIMESTAMP,
            23,
            3,
        )

    def test_datetimeoffset_default_scale(self) -> None:
        assert _input_size_for(_col(MssqlType.DATETIMEOFFSET)) == (
            pyodbc.SQL_TYPE_TIMESTAMP,
            27,
            7,
        )

    def test_datetimeoffset_explicit_scale(self) -> None:
        assert _input_size_for(_col(MssqlType.DATETIMEOFFSET, scale=3)) == (
            pyodbc.SQL_TYPE_TIMESTAMP,
            23,
            3,
        )


class TestIntegerFamily:
    def test_bigint(self) -> None:
        assert _input_size_for(_col(MssqlType.BIGINT, nullable=False)) == (
            pyodbc.SQL_BIGINT,
            19,
            0,
        )

    def test_int(self) -> None:
        assert _input_size_for(_col(MssqlType.INT, nullable=False)) == (
            pyodbc.SQL_INTEGER,
            10,
            0,
        )

    def test_smallint(self) -> None:
        assert _input_size_for(_col(MssqlType.SMALLINT, nullable=False)) == (
            pyodbc.SQL_SMALLINT,
            5,
            0,
        )

    def test_tinyint(self) -> None:
        assert _input_size_for(_col(MssqlType.TINYINT, nullable=False)) == (
            pyodbc.SQL_TINYINT,
            3,
            0,
        )

    def test_bit(self) -> None:
        assert _input_size_for(_col(MssqlType.BIT)) == (pyodbc.SQL_BIT, 1, 0)


class TestFloatingPoint:
    def test_float(self) -> None:
        assert _input_size_for(_col(MssqlType.FLOAT)) == (pyodbc.SQL_DOUBLE, 53, 0)

    def test_real(self) -> None:
        assert _input_size_for(_col(MssqlType.REAL)) == (pyodbc.SQL_REAL, 24, 0)


class TestDecimalAndNumeric:
    def test_decimal_with_precision_and_scale(self) -> None:
        assert _input_size_for(_col(MssqlType.DECIMAL, precision=12, scale=4)) == (
            pyodbc.SQL_DECIMAL,
            12,
            4,
        )

    def test_decimal_defaults_when_unset(self) -> None:
        # No precision/scale on the column → use ANSI defaults (18, 0).
        assert _input_size_for(_col(MssqlType.DECIMAL)) == (
            pyodbc.SQL_DECIMAL,
            18,
            0,
        )

    def test_numeric_with_precision_and_scale(self) -> None:
        assert _input_size_for(_col(MssqlType.NUMERIC, precision=20, scale=6)) == (
            pyodbc.SQL_NUMERIC,
            20,
            6,
        )


class TestMisc:
    def test_uniqueidentifier(self) -> None:
        assert _input_size_for(_col(MssqlType.UNIQUEIDENTIFIER)) == (
            pyodbc.SQL_GUID,
            36,
            0,
        )

    def test_varbinary_max(self) -> None:
        assert _input_size_for(_col(MssqlType.VARBINARY_MAX)) == (
            pyodbc.SQL_VARBINARY,
            0,
            0,
        )


class TestExhaustiveness:
    def test_every_mssqltype_is_handled(self) -> None:
        # If a new MssqlType variant is added but the function isn't updated,
        # this test fails — flagging that the new type would silently fall
        # through to "unhandled" instead of being caught.
        for member in MssqlType:
            col = _col(member)
            if member in (MssqlType.DECIMAL, MssqlType.NUMERIC):
                col.precision = 18
                col.scale = 0
            result = _input_size_for(col)
            assert isinstance(result, tuple) and len(result) == 3, (
                f"{member!r} did not return a 3-tuple"
            )
