from unittest.mock import patch
from datetime import datetime

from bollhav.model.schema import Schema


def test_resolved_no_suffix():
    assert Schema(name="kodserver_raw").resolved == "kodserver_raw"


def test_resolved_with_suffix_no_appendix():
    s = Schema(name="kodserver_raw", suffix="pr123", suffix_appendix=None)
    assert s.resolved == "kodserver_raw_pr123"


def test_resolved_with_suffix_and_appendix_has_trailing_underscore():
    fixed_now = datetime(2026, 4, 1)
    with patch("bollhav.model.schema.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_now
        s = Schema(name="kodserver_raw", suffix="pr123")
        result = s.resolved
    assert result == "kodserver_raw_pr123_2613_"


def test_resolved_appendix_yyww_format():
    fixed_now = datetime(2026, 4, 1)
    with patch("bollhav.model.schema.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_now
        s = Schema(name="s", suffix="x")
        result = s.resolved
    assert result.endswith("_")
    parts = result.rstrip("_").split("_")
    yyww = parts[-1]
    assert len(yyww) == 4
    assert yyww.isdigit()
