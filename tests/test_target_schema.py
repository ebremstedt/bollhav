from unittest.mock import patch
from datetime import datetime, timezone
from bollhav.model.target_schema import TargetSchema

def test_resolved_no_suffix():
    assert TargetSchema(name="kodserver_raw").resolved == "kodserver_raw"

def test_resolved_with_suffix_no_appendix():
    s = TargetSchema(name="kodserver_raw", suffix="pr123", suffix_appendix=None)
    assert s.resolved == "kodserver_raw_pr123"

def test_resolved_with_suffix_and_appendix_has_trailing_underscore():
    fixed_now = datetime(2026, 4, 1, tzinfo=timezone.utc)
    with patch("bollhav.model.target_schema.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_now
        s = TargetSchema(name="kodserver_raw", suffix="pr123")
        result = s.resolved
    assert result == "kodserver_raw_pr123_2614_"

def test_resolved_appendix_yyww_format():
    fixed_now = datetime(2026, 4, 1, tzinfo=timezone.utc)
    with patch("bollhav.model.target_schema.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_now
        s = TargetSchema(name="s", suffix="x")
        result = s.resolved
    assert result.endswith("_")
    parts = result.rstrip("_").split("_")
    yyww = parts[-1]
    assert len(yyww) == 4
    assert yyww.isdigit()

def test_resolved_uses_base_name_after_name_overwritten():
    s = TargetSchema(name="warehouse", suffix="pr123", suffix_appendix=None)
    assert s.resolved == "warehouse_pr123"
    s.name = s.resolved
    assert s.resolved == "warehouse_pr123"

def test_apply_pipe_style_mutation_is_idempotent():
    s = TargetSchema(name="warehouse", suffix="pr123", suffix_appendix=None)
    s.name = s.resolved
    s.name = s.resolved
    s.name = s.resolved
    assert s.name == "warehouse_pr123"
