"""Schema-name resolution (formerly the TargetSchema class).

`TargetSchema` is gone — a schema is now a plain `str` on `Target`, and the
dev/prod/PR isolation transform lives in `Target.schema_suffix` /
`schema_suffix_appendix`, applied by `Target.schema_resolved` (and, for
upstream refs, the pure `resolve_schema_name()` helper).
"""

from unittest.mock import patch
from datetime import datetime, timezone

from bollhav.model.target import Target, resolve_schema_name


def test_resolved_no_suffix():
    assert resolve_schema_name("kodserver_raw", "", None) == "kodserver_raw"


def test_resolved_with_suffix_no_appendix():
    assert resolve_schema_name("kodserver_raw", "pr123", None) == "kodserver_raw_pr123"


def test_resolved_with_suffix_and_appendix_has_trailing_underscore():
    fixed_now = datetime(2026, 4, 1, tzinfo=timezone.utc)
    with patch("bollhav.model.target.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_now
        result = resolve_schema_name("kodserver_raw", "pr123", "%y%V")
    assert result == "kodserver_raw_pr123_2614_"


def test_resolved_appendix_yyww_format():
    fixed_now = datetime(2026, 4, 1, tzinfo=timezone.utc)
    with patch("bollhav.model.target.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_now
        result = resolve_schema_name("s", "x", "%y%V")
    assert result.endswith("_")
    yyww = result.rstrip("_").split("_")[-1]
    assert len(yyww) == 4
    assert yyww.isdigit()


def test_target_schema_resolved_property():
    t = Target(
        name="orders",
        schema="warehouse",
        schema_suffix="pr123",
        schema_suffix_appendix=None,
    )
    assert t.schema_resolved == "warehouse_pr123"


def test_schema_resolved_is_idempotent():
    # schema_resolved is a pure view over the base `schema`, so calling it
    # repeatedly never double-applies the suffix — and the base is untouched.
    t = Target(
        name="orders",
        schema="warehouse",
        schema_suffix="pr123",
        schema_suffix_appendix=None,
    )
    assert t.schema_resolved == "warehouse_pr123"
    assert t.schema_resolved == "warehouse_pr123"
    assert t.schema == "warehouse"
