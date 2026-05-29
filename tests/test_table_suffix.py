"""Tests for TABLE_SUFFIX — the per-table-name analog of SCHEMA_SUFFIX.

Covers:
- `Target.name_resolved` with/without suffix and optional date appendix.
- `Target.full_name` composition with the table suffix in play.
- `apply_runtime_overrides(table_suffix=...)` baking the suffix into a
  copied target without mutating the source.
- `@load_models` reading `TABLE_SUFFIX` / `USE_TABLE_SUFFIX` env vars
  and rejecting illegal combinations.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import sys

sys.modules.setdefault("roskarl", MagicMock())
sys.modules.setdefault("icron", MagicMock())

from bollhav.model.target import Target  # noqa: E402
from bollhav.model.target_schema import TargetSchema  # noqa: E402


# ── Target.name_resolved ─────────────────────────────────────────────


def test_name_resolved_no_suffix():
    assert Target(name="customers").name_resolved == "customers"


def test_name_resolved_with_suffix_no_appendix():
    t = Target(name="customers", suffix="v2", suffix_appendix=None)
    assert t.name_resolved == "customers_v2"


def test_name_resolved_with_suffix_and_appendix():
    fixed_now = datetime(2026, 4, 1, tzinfo=timezone.utc)
    with patch("bollhav.model.target.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_now
        t = Target(name="customers", suffix="v2", suffix_appendix="%y%V")
        assert t.name_resolved == "customers_v2_2614"


def test_name_resolved_default_appendix_is_none():
    """Unlike TargetSchema (which defaults the date suffix on), Target
    defaults it off — table hotswaps usually want a clean predictable
    name, not a time-varying one."""
    assert Target(name="customers", suffix="v2").suffix_appendix is None


# ── Target.full_name ─────────────────────────────────────────────────


def test_full_name_with_table_suffix_only():
    t = Target(
        name="customers",
        suffix="v2",
        schema=TargetSchema(name="warehouse"),
    )
    assert t.full_name == "warehouse.customers_v2"


def test_full_name_combines_schema_and_table_suffix():
    t = Target(
        name="customers",
        suffix="v2",
        schema=TargetSchema(name="warehouse", suffix="pr123", suffix_appendix=None),
    )
    assert t.full_name == "warehouse_pr123.customers_v2"


def test_full_name_with_catalog_schema_and_table_suffix():
    t = Target(
        name="customers",
        suffix="v2",
        schema=TargetSchema(name="warehouse"),
        catalog="prod_cat",
    )
    assert t.full_name == "prod_cat.warehouse.customers_v2"


def test_full_name_unchanged_when_no_table_suffix():
    t = Target(name="customers", schema=TargetSchema(name="warehouse"))
    assert t.full_name == "warehouse.customers"


# ── apply_runtime_overrides ──────────────────────────────────────────


def _make_target(name="customers", suffix=""):
    return Target(name=name, suffix=suffix, schema=TargetSchema(name="warehouse"))


def test_apply_runtime_overrides_bakes_table_suffix(tmp_path, monkeypatch):
    """Pipe-level table_suffix overwrites whatever the source model declared."""
    from bollhav.model.runtime import _target_with_suffix

    src = _make_target(name="customers", suffix="")
    new = _target_with_suffix(src, schema_suffix="", table_suffix="v2")

    assert new.suffix == "v2"
    assert new.name_resolved == "customers_v2"
    # source target must not be mutated
    assert src.suffix == ""
    assert src.name_resolved == "customers"


def test_apply_runtime_overrides_empty_table_suffix_preserves_source():
    """Empty pipe table_suffix shouldn't clobber a model-declared suffix —
    mirrors how schema_suffix='' would simply not be applied."""
    from bollhav.model.runtime import _target_with_suffix

    src = _make_target(name="customers", suffix="legacy")
    new = _target_with_suffix(src, schema_suffix="", table_suffix="")

    assert new.suffix == "legacy"
    assert new.name_resolved == "customers_legacy"


def test_apply_runtime_overrides_combines_schema_and_table_suffix():
    """Both suffixes flow through together — used for blue/green inside a
    suffixed dev schema."""
    from bollhav.model.runtime import _target_with_suffix

    src = _make_target(name="customers", suffix="")
    new = _target_with_suffix(src, schema_suffix="pr123", table_suffix="v2")

    assert new.suffix == "v2"
    assert new.schema.suffix == "pr123"
    # resolved schema includes the schema date appendix by default, so use
    # the relevant pieces independently rather than asserting the full name.
    assert new.name_resolved == "customers_v2"


# ── load_models env wiring ───────────────────────────────────────────


_UNSET = object()


def _patches(
    *,
    table_suffix: str = "",
    use_table_suffix: bool = False,
    use_schema_suffix: bool = False,
    schema_suffix: str = "",
):
    bools = {
        "LATEST_ENABLED": False,
        "BACKFILL_ENABLED": True,
        "DRY_RUN": False,
        "DRY_RUN_EXTRA": False,
        "DEBUG": False,
        "USE_SCHEMA_SUFFIX": use_schema_suffix,
        "USE_TABLE_SUFFIX": use_table_suffix,
    }
    strs = {
        "TAGS": "[mytag]",
        "SCHEMA_SUFFIX": schema_suffix,
        "TABLE_SUFFIX": table_suffix,
    }
    return [
        patch(
            "bollhav.model.load_models.env_var_bool",
            lambda name, default=False: bools.get(name, default),
        ),
        patch(
            "bollhav.model.load_models.env_var",
            lambda name, required=False, default=None, should_print_unset=True: (
                strs.get(name, default)
                if not required or strs.get(name) is not None
                else (_ for _ in ()).throw(ValueError(f"required {name}"))
            ),
        ),
        patch(
            "bollhav.model.load_models.env_var_interval_expression",
            lambda name, should_print_unset=True: None,
        ),
        patch(
            "bollhav.model.load_models.env_var_int",
            lambda name, should_print_unset=True: None,
        ),
        patch(
            "bollhav.model.load_models.env_var_iso8601_datetime",
            lambda name: None,
        ),
    ]


def _read_env(**kw):
    from bollhav.model.load_models import _read_env as _re

    patches = _patches(**kw)
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        return _re()


def test_load_models_reads_table_suffix():
    cfg = _read_env(table_suffix="v2", use_table_suffix=True)
    assert cfg.table_suffix == "v2"


def test_load_models_use_table_suffix_false_clears():
    cfg = _read_env(table_suffix="v2", use_table_suffix=False)
    assert cfg.table_suffix == ""


def test_load_models_default_table_suffix_is_empty():
    cfg = _read_env()
    assert cfg.table_suffix == ""


def test_load_models_rejects_use_table_suffix_without_value():
    with pytest.raises(
        ValueError, match="USE_TABLE_SUFFIX=True requires non-empty TABLE_SUFFIX"
    ):
        _read_env(use_table_suffix=True, table_suffix="")


def test_load_models_passes_table_suffix_to_apply_runtime_overrides():
    """The decorator must thread cfg.table_suffix into apply_runtime_overrides
    so downstream code (target rebuild, schema DDL) sees the suffix."""
    from bollhav.model.load_models import load_models

    apm_kwargs: dict = {}

    def _fake_apm(**kwargs):
        apm_kwargs.update(kwargs)
        return []

    patches = _patches(use_table_suffix=True, table_suffix="v2")
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patch(
            "bollhav.model.load_models.apply_runtime_overrides", side_effect=_fake_apm
        ),
        patch("bollhav.model.load_models._print_summary", lambda cfg, models: None),
    ):

        @load_models
        def main(models, debug):
            pass

        main()

    assert apm_kwargs["table_suffix"] == "v2"
