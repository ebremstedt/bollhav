from dataclasses import dataclass
from datetime import datetime, tzinfo
from roskarl import (
    env_var_bool,
    env_var_interval_expression,
    env_var,
    env_var_iso8601_datetime,
)
from functools import wraps
from typing import Callable
from bollhav.model.ordering import UpstreamMode
from bollhav.model.progress_bar import get_progress_level
from bollhav.pipe.tz import _resolve_tz_override, _apply_tz


@dataclass
class LatestConfig:
    enabled: bool


@dataclass
class BackfillConfig:
    enabled: bool
    since: datetime | None
    until: datetime | None


@dataclass
class PipeConfig:
    tags: str | None
    latest: LatestConfig
    backfill: BackfillConfig
    schema_suffix: str
    interval_expression_override: str | None = None
    window_expression_override: str | None = None
    tz_override: tzinfo | None = None
    use_schema_suffix: bool = True
    debug: bool = False
    upstream_mode: UpstreamMode = UpstreamMode.ENFORCE

    def __str__(self) -> str:
        suffix_part = (
            f", schema_suffix={self.schema_suffix}" if self.use_schema_suffix else ""
        )
        return f"EnvConfig(tags={self.tags}, cron={self.latest}, backfill={self.backfill}, debug={self.debug}, use_schema_suffix={self.use_schema_suffix}{suffix_part})"

    def __post_init__(self) -> None:
        if self.use_schema_suffix and self.schema_suffix == "":
            raise ValueError("use_schema_suffix=True requires non-empty schema_suffix")

        if not self.use_schema_suffix:
            self.schema_suffix = ""

        if self.window_expression_override and not self.latest.enabled:
            raise ValueError(
                "WINDOW_EXPRESSION_OVERRIDE only applies when LATEST_ENABLED=True — "
                "in backfill mode since/until are set explicitly and no window is inferred"
            )

        def _row(key: str, val: str) -> None:
            print(f"  {key:<18}{val}")

        def _date(dt: datetime | None) -> str:
            return dt.isoformat() if dt else "—"

        print("── pipe ────────────────────")
        _row("progress", get_progress_level().value)
        _row("tags", self.tags or "—")
        if self.latest.enabled:
            _row("mode", "latest")
        elif self.backfill.enabled:
            _row(
                "mode",
                f"backfill  {_date(self.backfill.since)} → {_date(self.backfill.until)}",
            )
        else:
            _row("mode", "off")
        _row("debug", "on" if self.debug else "off")
        if self.use_schema_suffix:
            _row("suffix", self.schema_suffix)
        if self.upstream_mode != UpstreamMode.ENFORCE:
            _row("upstream", self.upstream_mode.value)
        if self.latest.enabled or self.backfill.enabled:
            _row("tz override", str(self.tz_override) if self.tz_override else "unset")
            _row("interval override", self.interval_expression_override or "unset")
        if self.latest.enabled:
            _row("window override", self.window_expression_override or "unset")
        print("────────────────────────────")


def _resolve_upstream_mode() -> UpstreamMode:
    raw = env_var(name="UPSTREAM", should_print_unset=False)
    if raw is None:
        return UpstreamMode.ENFORCE
    valid = {m.value: m for m in UpstreamMode}
    if raw not in valid:
        raise ValueError(f"UPSTREAM must be one of {list(valid.keys())}, got {raw!r}")
    return valid[raw]


def load_pipe_config() -> PipeConfig:
    latest_enabled = env_var_bool(name="LATEST_ENABLED", default=False)
    backfill_enabled = env_var_bool(name="BACKFILL_ENABLED", default=not latest_enabled)
    if latest_enabled and backfill_enabled:
        raise ValueError("LATEST_ENABLED and BACKFILL_ENABLED cannot both be true")

    tz_override = _resolve_tz_override()

    def _backfill_dt(name: str) -> datetime | None:
        if not backfill_enabled:
            return None
        dt = env_var_iso8601_datetime(name=name)
        return _apply_tz(dt, tz_override) if tz_override else dt

    return PipeConfig(
        tags=env_var(name="TAGS", required=True),
        latest=LatestConfig(enabled=latest_enabled),
        backfill=BackfillConfig(
            enabled=backfill_enabled,
            since=_backfill_dt("BACKFILL_SINCE"),
            until=_backfill_dt("BACKFILL_UNTIL"),
        ),
        interval_expression_override=env_var_interval_expression(
            name="INTERVAL_EXPRESSION_OVERRIDE", should_print_unset=False
        ),
        window_expression_override=env_var_interval_expression(
            name="WINDOW_EXPRESSION_OVERRIDE", should_print_unset=False
        ),
        tz_override=tz_override,
        debug=env_var_bool(name="DEBUG", default=False),
        use_schema_suffix=env_var_bool(name="USE_SCHEMA_SUFFIX", default=True),
        schema_suffix=env_var(name="SCHEMA_SUFFIX", default=""),
        upstream_mode=_resolve_upstream_mode(),
    )


def with_pipe_config(func: Callable[[PipeConfig], None]) -> Callable[[], None]:
    @wraps(func)
    def wrapper() -> None:
        pipe = load_pipe_config()
        func(pipe)

    return wrapper
