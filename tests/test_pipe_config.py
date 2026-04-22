from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import pytest
import sys

roskarl_mock = MagicMock()
cron_mock = MagicMock()
sys.modules["roskarl"] = roskarl_mock
sys.modules["icron"] = MagicMock()
sys.modules["cron"] = cron_mock

from bollhav.pipe.pipe_config import (  # noqa: E402
    PipeConfig,
    LatestConfig,
    BackfillConfig,
    load_pipe_config,
    with_pipe_config,
)
from bollhav.model.ordering import UpstreamMode  # noqa: E402

DT_SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
DT_UNTIL = datetime(2024, 1, 2, tzinfo=timezone.utc)


def _base_latest(enabled: bool = False) -> LatestConfig:
    return LatestConfig(enabled=enabled)


def _base_backfill(enabled: bool = False) -> BackfillConfig:
    return BackfillConfig(enabled=enabled, since=None, until=None)


_UNSET = object()


def _make_patches(
    *,
    latest_enabled: bool = False,
    backfill_enabled: object = _UNSET,
    tags: str = "mytag",
    schema_suffix: str = "dev",
    use_schema_suffix: bool = True,
    debug: bool = False,
    interval_expression_override: str | None = None,
    backfill_since: datetime | None = None,
    backfill_until: datetime | None = None,
    upstream: str | None = None,
) -> dict:
    bool_map: dict = {
        "LATEST_ENABLED": latest_enabled,
        "DEBUG": debug,
        "USE_SCHEMA_SUFFIX": use_schema_suffix,
    }
    if backfill_enabled is not _UNSET:
        bool_map["BACKFILL_ENABLED"] = backfill_enabled
    return {
        "bollhav.pipe.pipe_config.env_var_bool": lambda name, default=False: (
            bool_map.get(name, default)
        ),
        "bollhav.pipe.pipe_config.env_var": lambda name, required=False, default=None, should_print_unset=True: (
            {
                "TAGS": tags,
                "SCHEMA_SUFFIX": schema_suffix,
                "UPSTREAM": upstream,
            }.get(name, default)
        ),
        "bollhav.pipe.pipe_config.env_var_interval_expression": lambda name, should_print_unset=True: (
            {
                "INTERVAL_EXPRESSION_OVERRIDE": interval_expression_override,
            }.get(name)
        ),
        "bollhav.pipe.pipe_config.env_var_iso8601_datetime": lambda name: {
            "BACKFILL_SINCE": backfill_since,
            "BACKFILL_UNTIL": backfill_until,
        }.get(name),
    }


# ... rest of TestPipeConfigPostInit and TestPipeConfigDebugprint unchanged ...


class TestLoadPipeConfig:
    def _run(self, **kwargs) -> PipeConfig:
        patches = _make_patches(**kwargs)
        with (
            patch(
                "bollhav.pipe.pipe_config.env_var_bool",
                patches["bollhav.pipe.pipe_config.env_var_bool"],
            ),
            patch(
                "bollhav.pipe.pipe_config.env_var",
                patches["bollhav.pipe.pipe_config.env_var"],
            ),
            patch(
                "bollhav.pipe.pipe_config.env_var_interval_expression",
                patches["bollhav.pipe.pipe_config.env_var_interval_expression"],
            ),
            patch(
                "bollhav.pipe.pipe_config.env_var_iso8601_datetime",
                patches["bollhav.pipe.pipe_config.env_var_iso8601_datetime"],
            ),
            patch(
                "bollhav.pipe.tz.env_var",
                patches["bollhav.pipe.pipe_config.env_var"],
            ),
        ):
            return load_pipe_config()

    def test_basic_load(self) -> None:
        cfg = self._run()
        assert cfg.tags == "mytag"
        assert cfg.schema_suffix == "dev"
        assert cfg.latest.enabled is False
        assert cfg.backfill.enabled is True

    def test_raises_when_both_enabled(self) -> None:
        with pytest.raises(
            ValueError, match="LATEST_ENABLED and BACKFILL_ENABLED cannot both be true"
        ):
            self._run(latest_enabled=True, backfill_enabled=True)

    def test_interval_expression_override_stored(self) -> None:
        cfg = self._run(interval_expression_override="0 0 * * *")
        assert cfg.interval_expression_override == "0 0 * * *"

    def test_interval_expression_override_defaults_to_none(self) -> None:
        cfg = self._run()
        assert cfg.interval_expression_override is None

    def test_backfill_populates_fields(self) -> None:
        cfg = self._run(
            backfill_enabled=True,
            backfill_since=DT_SINCE,
            backfill_until=DT_UNTIL,
        )
        assert cfg.backfill.enabled is True
        assert cfg.backfill.since == DT_SINCE
        assert cfg.backfill.until == DT_UNTIL

    def test_backfill_disabled_clears_fields(self) -> None:
        cfg = self._run(backfill_enabled=False)
        assert cfg.backfill.since is None
        assert cfg.backfill.until is None

    def test_use_schema_suffix_false_clears_suffix(self) -> None:
        cfg = self._run(use_schema_suffix=False)
        assert cfg.schema_suffix == ""

    def test_upstream_defaults_to_enforce(self) -> None:
        cfg = self._run()
        assert cfg.upstream_mode == UpstreamMode.ENFORCE

    def test_upstream_ignore_views(self) -> None:
        cfg = self._run(upstream="ignore_views")
        assert cfg.upstream_mode == UpstreamMode.IGNORE_VIEWS

    def test_upstream_ignore_completely(self) -> None:
        cfg = self._run(upstream="ignore_completely")
        assert cfg.upstream_mode == UpstreamMode.IGNORE_COMPLETELY

    def test_upstream_enforce(self) -> None:
        cfg = self._run(upstream="enforce")
        assert cfg.upstream_mode == UpstreamMode.ENFORCE

    def test_upstream_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="UPSTREAM must be one of"):
            self._run(upstream="bogus")


class TestWithPipeConfig:
    def test_calls_func_with_config(self) -> None:
        patches = _make_patches(tags="t")
        received: list[PipeConfig] = []

        @with_pipe_config
        def my_func(cfg: PipeConfig) -> None:
            received.append(cfg)

        with (
            patch(
                "bollhav.pipe.pipe_config.env_var_bool",
                patches["bollhav.pipe.pipe_config.env_var_bool"],
            ),
            patch(
                "bollhav.pipe.pipe_config.env_var",
                patches["bollhav.pipe.pipe_config.env_var"],
            ),
            patch(
                "bollhav.pipe.pipe_config.env_var_interval_expression",
                patches["bollhav.pipe.pipe_config.env_var_interval_expression"],
            ),
            patch(
                "bollhav.pipe.pipe_config.env_var_iso8601_datetime",
                patches["bollhav.pipe.pipe_config.env_var_iso8601_datetime"],
            ),
            patch(
                "bollhav.pipe.tz.env_var",
                patches["bollhav.pipe.pipe_config.env_var"],
            ),
        ):
            my_func()

        assert len(received) == 1
        assert received[0].tags == "t"
