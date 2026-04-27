from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, tzinfo
from functools import wraps
from typing import Callable
from zoneinfo import ZoneInfo

from roskarl import (
    env_var,
    env_var_bool,
    env_var_interval_expression,
    env_var_iso8601_datetime,
)

from bollhav.model.runtime import apply_runtime_overrides
from bollhav.model.model import Model
from bollhav.model.ordering import UpstreamMode
from bollhav.model.progress_bar import get_progress_level


@dataclass
class _RuntimeConfig:
    """Internal: env-var read result. Not exposed."""

    tags: str
    schema_suffix: str
    upstream_mode: UpstreamMode
    latest: bool
    backfill_enabled: bool
    backfill_since: datetime | None
    backfill_until: datetime | None
    interval_expression_override: str | None
    window_expression_override: str | None
    tz_override: tzinfo | None
    debug: bool


def load_models(
    _func: Callable[..., None] | None = None,
    *,
    folder: str = "src/models",
) -> Callable[..., None]:
    """Decorator that reads runtime overrides from env vars, matches models
    in `folder` by `TAGS`, bakes the overrides in, and calls the wrapped
    function with `(models, debug)`.

    Usage:
        @load_models
        def main(models: list[Model], debug: bool) -> None:
            for model in models:
                ...

    or with a custom folder:

        @load_models(folder="custom/models")
        def main(models: list[Model], debug: bool) -> None:
            ...

    Env vars consumed:
        TAGS                          (required)
        SCHEMA_SUFFIX                 (required when USE_SCHEMA_SUFFIX=true)
        USE_SCHEMA_SUFFIX             default true
        DEBUG                         default false
        TIMEZONE_OVERRIDE             IANA timezone name
        LATEST_ENABLED                default false
        BACKFILL_ENABLED              default !LATEST_ENABLED
        BACKFILL_SINCE                ISO 8601 datetime
        BACKFILL_UNTIL                ISO 8601 datetime
        INTERVAL_EXPRESSION_OVERRIDE  cron / @alias
        WINDOW_EXPRESSION_OVERRIDE    cron / @alias (latest mode only)
        UPSTREAM                      enforce | ignore_views | ignore_completely
    """

    def decorator(func: Callable[..., None]) -> Callable[[], None]:
        @wraps(func)
        def wrapper() -> None:
            cfg = _read_env()
            _print_summary(cfg)
            models = apply_runtime_overrides(
                folder=folder,
                tags=cfg.tags,
                schema_suffix=cfg.schema_suffix,
                upstream_mode=cfg.upstream_mode,
                latest=cfg.latest,
                backfill_since=cfg.backfill_since,
                backfill_until=cfg.backfill_until,
                interval_expression_override=cfg.interval_expression_override,
                window_expression_override=cfg.window_expression_override,
                tz_override=cfg.tz_override,
            )
            func(models=models, debug=cfg.debug)

        return wrapper

    # Allow both @load_models and @load_models(folder=...).
    if _func is None:
        return decorator
    return decorator(_func)


def _read_env() -> _RuntimeConfig:
    latest = env_var_bool(name="LATEST_ENABLED", default=False)
    backfill_enabled = env_var_bool(name="BACKFILL_ENABLED", default=not latest)
    if latest and backfill_enabled:
        raise ValueError("LATEST_ENABLED and BACKFILL_ENABLED cannot both be true")

    tz_override = _resolve_tz_override()

    def _backfill_dt(name: str) -> datetime | None:
        if not backfill_enabled:
            return None
        dt = env_var_iso8601_datetime(name=name)
        return _apply_tz(dt, tz_override) if tz_override else dt

    use_schema_suffix = env_var_bool(name="USE_SCHEMA_SUFFIX", default=True)
    raw_suffix = env_var(name="SCHEMA_SUFFIX", default="")
    if use_schema_suffix and raw_suffix == "":
        raise ValueError("USE_SCHEMA_SUFFIX=True requires non-empty SCHEMA_SUFFIX")
    schema_suffix = raw_suffix if use_schema_suffix else ""

    window_expression_override = env_var_interval_expression(
        name="WINDOW_EXPRESSION_OVERRIDE", should_print_unset=False
    )
    if window_expression_override and not latest:
        raise ValueError(
            "WINDOW_EXPRESSION_OVERRIDE only applies when LATEST_ENABLED=True — "
            "in backfill mode since/until are set explicitly and no window is inferred"
        )

    return _RuntimeConfig(
        tags=env_var(name="TAGS", required=True),
        schema_suffix=schema_suffix,
        upstream_mode=_resolve_upstream_mode(),
        latest=latest,
        backfill_enabled=backfill_enabled,
        backfill_since=_backfill_dt("BACKFILL_SINCE"),
        backfill_until=_backfill_dt("BACKFILL_UNTIL"),
        interval_expression_override=env_var_interval_expression(
            name="INTERVAL_EXPRESSION_OVERRIDE", should_print_unset=False
        ),
        window_expression_override=window_expression_override,
        tz_override=tz_override,
        debug=env_var_bool(name="DEBUG", default=False),
    )


def _resolve_tz_override() -> tzinfo | None:
    raw = env_var(name="TIMEZONE_OVERRIDE", should_print_unset=False)
    if raw is None:
        return None
    try:
        return ZoneInfo(raw)
    except KeyError:
        raise ValueError(f"TIMEZONE_OVERRIDE is not a valid IANA timezone: {raw!r}")


def _apply_tz(dt: datetime | None, tz: tzinfo) -> datetime | None:
    if dt is None:
        return None
    return dt.replace(tzinfo=tz)


def _resolve_upstream_mode() -> UpstreamMode:
    raw = env_var(name="UPSTREAM", should_print_unset=False)
    if raw is None:
        return UpstreamMode.ENFORCE
    valid = {m.value: m for m in UpstreamMode}
    if raw not in valid:
        raise ValueError(f"UPSTREAM must be one of {list(valid.keys())}, got {raw!r}")
    return valid[raw]


def _print_summary(cfg: _RuntimeConfig) -> None:
    def _row(key: str, val: str) -> None:
        print(f"  {key:<18}{val}")

    def _date(dt: datetime | None) -> str:
        return dt.isoformat() if dt else "—"

    print("── runtime ─────────────────")
    _row("progress", get_progress_level().value)
    _row("tags", cfg.tags or "—")
    if cfg.latest:
        _row("mode", "latest")
    elif cfg.backfill_enabled:
        _row(
            "mode",
            f"backfill  {_date(cfg.backfill_since)} → {_date(cfg.backfill_until)}",
        )
    else:
        _row("mode", "off")
    _row("debug", "on" if cfg.debug else "off")
    if cfg.schema_suffix:
        _row("suffix", cfg.schema_suffix)
    if cfg.upstream_mode != UpstreamMode.ENFORCE:
        _row("upstream", cfg.upstream_mode.value)
    if cfg.latest or cfg.backfill_enabled:
        _row("tz override", str(cfg.tz_override) if cfg.tz_override else "unset")
        _row("interval override", cfg.interval_expression_override or "unset")
    if cfg.latest:
        _row("window override", cfg.window_expression_override or "unset")
    print("────────────────────────────")


__all__ = ["load_models", "Model"]
