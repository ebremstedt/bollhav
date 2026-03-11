from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import pytest
import sys

roskarl_mock = MagicMock()
cron_mock = MagicMock()
sys.modules["roskarl"] = roskarl_mock
sys.modules["icron"] = MagicMock()
sys.modules["cron"] = cron_mock

from bollhav.pipe.pipe_config import (
    PipeConfig,
    LatestConfig,
    BackfillConfig,
    load_pipe_config,
    with_pipe_config,
)  # noqa: E402

DT_SINCE = datetime(2024, 1, 1, tzinfo=timezone.utc)
DT_UNTIL = datetime(2024, 1, 2, tzinfo=timezone.utc)


def _base_latest(enabled: bool = False) -> LatestConfig:
    return LatestConfig(enabled=enabled, cron=None, since=None, until=None)


def _base_backfill(enabled: bool = False) -> BackfillConfig:
    return BackfillConfig(enabled=enabled, since=None, until=None, cron=None)


def _make_patches(
    *,
    latest_enabled: bool = False,
    backfill_enabled: bool = False,
    tags: str = "mytag",
    schema_suffix: str = "dev",
    use_schema_suffix: bool = True,
    debug: bool = False,
    cron_expr: str | None = None,
    backfill_since: datetime | None = None,
    backfill_until: datetime | None = None,
    backfill_cron: str | None = None,
    cron_interval: tuple[datetime, datetime] = (DT_SINCE, DT_UNTIL),
) -> dict:
    return {
        "bollhav.pipe.pipe_config.env_var_bool": lambda name, default=False: {
            "LATEST_ENABLED": latest_enabled,
            "BACKFILL_ENABLED": backfill_enabled,
            "DEBUG": debug,
            "USE_SCHEMA_SUFFIX": use_schema_suffix,
        }.get(name, default),
        "bollhav.pipe.pipe_config.env_var": lambda name, required=False, default=None: {
            "TAGS": tags,
            "SCHEMA_SUFFIX": schema_suffix,
        }.get(name, default),
        "bollhav.pipe.pipe_config.env_var_cron": lambda name: {
            "LATEST_CRON": cron_expr,
            "BACKFILL_CRON": backfill_cron,
        }.get(name),
        "bollhav.pipe.pipe_config.env_var_iso8601_datetime": lambda name: {
            "BACKFILL_SINCE": backfill_since,
            "BACKFILL_UNTIL": backfill_until,
        }.get(name),
        "bollhav.pipe.pipe_config._resolve_cron_interval": MagicMock(
            return_value=cron_interval
        ),
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
                "bollhav.pipe.pipe_config.env_var_cron",
                patches["bollhav.pipe.pipe_config.env_var_cron"],
            ),
            patch(
                "bollhav.pipe.pipe_config.env_var_iso8601_datetime",
                patches["bollhav.pipe.pipe_config.env_var_iso8601_datetime"],
            ),
            patch(
                "bollhav.pipe.pipe_config._resolve_cron_interval",
                patches["bollhav.pipe.pipe_config._resolve_cron_interval"],
            ),
        ):
            return load_pipe_config()

    def test_basic_load(self) -> None:
        cfg = self._run()
        assert cfg.tags == "mytag"
        assert cfg.schema_suffix == "dev"
        assert cfg.latest.enabled is False
        assert cfg.backfill.enabled is False

    def test_raises_when_both_enabled(self) -> None:
        with pytest.raises(
            ValueError, match="LATEST_ENABLED and BACKFILL_ENABLED cannot both be true"
        ):
            self._run(latest_enabled=True, backfill_enabled=True)

    def test_latest_resolves_cron_interval(self) -> None:
        cfg = self._run(latest_enabled=True, cron_expr="0 0 * * *")
        assert cfg.latest.enabled is True
        assert cfg.latest.since == DT_SINCE
        assert cfg.latest.until == DT_UNTIL
        assert cfg.latest.cron == "0 0 * * *"

    def test_latest_disabled_clears_cron(self) -> None:
        cfg = self._run(latest_enabled=False, cron_expr="0 0 * * *")
        assert cfg.latest.cron is None
        assert cfg.latest.since is None
        assert cfg.latest.until is None

    def test_backfill_populates_fields(self) -> None:
        cfg = self._run(
            backfill_enabled=True,
            backfill_since=DT_SINCE,
            backfill_until=DT_UNTIL,
            backfill_cron="0 6 * * *",
        )
        assert cfg.backfill.enabled is True
        assert cfg.backfill.since == DT_SINCE
        assert cfg.backfill.until == DT_UNTIL
        assert cfg.backfill.cron == "0 6 * * *"

    def test_backfill_disabled_clears_fields(self) -> None:
        cfg = self._run(backfill_enabled=False)
        assert cfg.backfill.since is None
        assert cfg.backfill.until is None
        assert cfg.backfill.cron is None

    def test_use_schema_suffix_false_clears_suffix(self) -> None:
        cfg = self._run(use_schema_suffix=False)
        assert cfg.schema_suffix == ""


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
                "bollhav.pipe.pipe_config.env_var_cron",
                patches["bollhav.pipe.pipe_config.env_var_cron"],
            ),
            patch(
                "bollhav.pipe.pipe_config.env_var_iso8601_datetime",
                patches["bollhav.pipe.pipe_config.env_var_iso8601_datetime"],
            ),
            patch(
                "bollhav.pipe.pipe_config._resolve_cron_interval",
                patches["bollhav.pipe.pipe_config._resolve_cron_interval"],
            ),
        ):
            my_func()

        assert len(received) == 1
        assert received[0].tags == "t"
