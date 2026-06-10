"""Tests for the @load_models decorator — env-var reading + cross-validation.

The decorator's job: read env, validate, hand a list of models + debug to
the wrapped function. We patch the env helpers and mock apply_runtime_overrides
so we can assert what the decorator passes through.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import sys

# Mock external libs that load_models pulls in.
sys.modules.setdefault("roskarl", MagicMock())
sys.modules.setdefault("icron", MagicMock())

from bollhav.model.load_models import load_models  # noqa: E402
from bollhav.model.ordering import UpstreamMode  # noqa: E402


_UNSET = object()


def _patches(
    *,
    tags: str = "[mytag]",
    schema_suffix: str = "dev",
    use_schema_suffix: bool = True,
    latest_enabled: bool = False,
    backfill_enabled: object = _UNSET,
    backfill_since: datetime | None = None,
    backfill_until: datetime | None = None,
    interval_expression_override: str | None = None,
    window_expression_override: str | None = None,
    lookback_override: int | None = None,
    tz_override: str | None = None,
    upstream: str | None = None,
    dry_run: bool = False,
    debug: bool = False,
):
    bools: dict = {
        "LATEST_ENABLED": latest_enabled,
        "DRY_RUN": dry_run,
        "DRY_RUN_EXTRA": False,
        "DEBUG": debug,
        "USE_SCHEMA_SUFFIX": use_schema_suffix,
    }
    if backfill_enabled is not _UNSET:
        bools["BACKFILL_ENABLED"] = backfill_enabled

    strs = {
        "TAGS": tags,
        "SCHEMA_SUFFIX": schema_suffix,
        "UPSTREAM": upstream,
        "TIMEZONE_OVERRIDE": tz_override,
    }

    intervals = {
        "INTERVAL_EXPRESSION_OVERRIDE": interval_expression_override,
        "WINDOW_EXPRESSION_OVERRIDE": window_expression_override,
    }

    ints = {"LOOKBACK_OVERRIDE": lookback_override}

    iso = {"BACKFILL_SINCE": backfill_since, "BACKFILL_UNTIL": backfill_until}

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
                else (_ for _ in ()).throw(ValueError(f"required env var {name} unset"))
            ),
        ),
        patch(
            "bollhav.model.load_models.env_var_interval_expression",
            lambda name, should_print_unset=True: intervals.get(name),
        ),
        patch(
            "bollhav.model.load_models.env_var_int",
            lambda name, should_print_unset=True: ints.get(name),
        ),
        patch(
            "bollhav.model.load_models.env_var_iso8601_datetime",
            lambda name: iso.get(name),
        ),
    ]


class _FakeRun:
    """Minimal stand-in for a ModelRun: `@load_models` calls
    `compute_intervals(run)` and stashes the result on `run.intervals`, so the
    fake needs a `model` (with `batching=None` → the (None,) contract), a
    `window`, and an assignable `intervals`."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.model = MagicMock()
        self.model.batching = None
        self.window = None
        self.intervals = (None,)


def _run_decorator(**env):
    """Apply patches, define a @load_models main, call it, return the kwargs
    apply_runtime_overrides was called with plus what main received."""
    patches = _patches(**env)
    received: dict = {}
    apm_kwargs: dict = {}

    def _fake_apm(**kwargs):
        apm_kwargs.update(kwargs)
        return [_FakeRun("fake-model-1"), _FakeRun("fake-model-2")]

    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patches[4],
        patch(
            "bollhav.model.load_models.apply_runtime_overrides", side_effect=_fake_apm
        ),
        patch("bollhav.model.load_models._print_summary", lambda cfg, runs: None),
    ):

        @load_models
        def main(runs, debug):
            received["runs"] = runs
            received["debug"] = debug

        main()

    return apm_kwargs, received


class TestEnvReading:
    def test_basic(self) -> None:
        apm, received = _run_decorator()
        assert apm["tags"] == "[mytag]"
        assert apm["schema_suffix"] == "dev"
        assert apm["latest"] is False
        assert apm["upstream_mode"] is UpstreamMode.ENFORCE
        assert [r.name for r in received["runs"]] == [
            "fake-model-1",
            "fake-model-2",
        ]
        assert received["debug"] is False

    def test_latest_enabled_passes_through(self) -> None:
        apm, _ = _run_decorator(latest_enabled=True)
        assert apm["latest"] is True

    def test_backfill_window_passes_through(self) -> None:
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 1, 2, tzinfo=timezone.utc)
        apm, _ = _run_decorator(backfill_since=since, backfill_until=until)
        assert apm["backfill_since"] == since
        assert apm["backfill_until"] == until

    def test_use_schema_suffix_false_clears(self) -> None:
        apm, _ = _run_decorator(use_schema_suffix=False)
        assert apm["schema_suffix"] == ""

    def test_debug_propagates(self) -> None:
        _, received = _run_decorator(debug=True)
        assert received["debug"] is True

    def test_upstream_mode_parsing(self) -> None:
        apm, _ = _run_decorator(upstream="ignore_views")
        assert apm["upstream_mode"] is UpstreamMode.IGNORE_VIEWS

    def test_upstream_mode_is_case_insensitive(self) -> None:
        apm, _ = _run_decorator(upstream="IGNORE_VIEWS")
        assert apm["upstream_mode"] is UpstreamMode.IGNORE_VIEWS

    def test_lookback_override_passes_through(self) -> None:
        apm, _ = _run_decorator(lookback_override=5)
        assert apm["lookback_override"] == 5

    def test_lookback_override_unset(self) -> None:
        apm, _ = _run_decorator()
        assert apm["lookback_override"] is None

    def test_dry_run_default_false(self) -> None:
        from bollhav.model.load_models import _read_env

        patches = _patches()
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            cfg = _read_env()
        assert cfg.dry_run is False

    def test_dry_run_true(self) -> None:
        from bollhav.model.load_models import _read_env

        patches = _patches(dry_run=True)
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            cfg = _read_env()
        assert cfg.dry_run is True


class TestValidation:
    def test_latest_and_backfill_both_true_raises(self) -> None:
        with pytest.raises(
            ValueError, match="LATEST_ENABLED and BACKFILL_ENABLED cannot both be true"
        ):
            _run_decorator(latest_enabled=True, backfill_enabled=True)

    def test_use_schema_suffix_with_empty_suffix_raises(self) -> None:
        with pytest.raises(
            ValueError, match="USE_SCHEMA_SUFFIX=True requires non-empty SCHEMA_SUFFIX"
        ):
            _run_decorator(schema_suffix="")

    def test_window_override_without_latest_raises(self) -> None:
        with pytest.raises(
            ValueError,
            match="WINDOW_EXPRESSION_OVERRIDE only applies when LATEST_ENABLED",
        ):
            _run_decorator(window_expression_override="@daily")

    def test_negative_lookback_override_raises(self) -> None:
        with pytest.raises(ValueError, match="LOOKBACK_OVERRIDE must be non-negative"):
            _run_decorator(lookback_override=-1)

    def test_invalid_upstream_raises(self) -> None:
        with pytest.raises(ValueError, match="UPSTREAM must be one of"):
            _run_decorator(upstream="bogus")


class TestDecoratorForms:
    def test_bare_decorator(self) -> None:
        # @load_models with no parens should still work.
        apm, _ = _run_decorator()
        assert apm["tags"] == "[mytag]"

    def test_decorator_with_folder_kwarg(self) -> None:
        # Build a decorator that captures the folder arg.
        captured: dict = {}

        def _fake_apm(**kwargs):
            captured.update(kwargs)
            return []

        patches = _patches()
        with (
            patches[0],
            patches[1],
            patches[2],
            patches[3],
            patch(
                "bollhav.model.load_models.apply_runtime_overrides",
                side_effect=_fake_apm,
            ),
            patch("bollhav.model.load_models._print_summary", lambda cfg, models: None),
        ):

            @load_models(folder="custom/path")
            def main(runs, debug):
                pass

            main()

        assert captured["folder"] == "custom/path"
