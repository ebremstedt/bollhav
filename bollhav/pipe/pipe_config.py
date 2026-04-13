from dataclasses import dataclass
from datetime import datetime, timezone
from roskarl import (
    env_var_bool,
    env_var_batch_expression,
    env_var,
    env_var_iso8601_datetime,
)
from functools import wraps
from typing import Callable
from bollhav.model.ordering import UpstreamMode


@dataclass
class LatestConfig:
    enabled: bool
    batch_expression: str | None


@dataclass
class BackfillConfig:
    enabled: bool
    since: datetime | None
    until: datetime | None
    batch_expression: str | None


@dataclass
class PipeConfig:
    tags: str | None
    latest: LatestConfig
    backfill: BackfillConfig
    schema_suffix: str
    use_schema_suffix: bool = True
    debug: bool = False
    upstream_mode: UpstreamMode = UpstreamMode.ENFORCE

    def __str__(self) -> str:
        suffix_part = (
            f", schema_suffix={self.schema_suffix}" if self.use_schema_suffix else ""
        )
        return f"EnvConfig(tags={self.tags}, cron={self.latest}, backfill={self.backfill}, debug={self.debug}, use_schema_suffix={self.use_schema_suffix}{suffix_part})"

    def debugprint(self, msg: str) -> None:
        if self.debug:
            ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            print(f"{ts} {msg}")

    def __post_init__(self) -> None:
        if self.use_schema_suffix and self.schema_suffix == "":
            raise ValueError("use_schema_suffix=True requires non-empty schema_suffix")

        if not self.use_schema_suffix:
            self.schema_suffix = ""

        def _row(key: str, val: str) -> None:
            print(f"  {key:<10}{val}")

        def _date(dt: datetime | None) -> str:
            return dt.isoformat() if dt else "—"

        print("── pipe ────────────────────")
        _row("tags", self.tags or "—")
        _row("debug", "on" if self.debug else "off")
        if self.use_schema_suffix:
            _row("suffix", self.schema_suffix)
        if self.latest.enabled:
            _row(
                "mode",
                "latest (interval inferred from model config or batch expression override)",
            )
            if self.latest.batch_expression:
                _row("batch", self.latest.batch_expression)
        elif self.backfill.enabled:
            _row(
                "mode",
                f"backfill  {_date(self.backfill.since)} → {_date(self.backfill.until)}",
            )
            if self.backfill.batch_expression:
                _row("batch", self.backfill.batch_expression)
        else:
            _row("mode", "off")
        if self.upstream_mode != UpstreamMode.ENFORCE:
            _row("upstream", self.upstream_mode.value)
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
    backfill_enabled = env_var_bool(name="BACKFILL_ENABLED", default=False)
    if latest_enabled and backfill_enabled:
        raise ValueError("LATEST_ENABLED and BACKFILL_ENABLED cannot both be true")

    latest_batch_expression = env_var_batch_expression(
        name="LATEST_BATCH_EXPRESSION", should_print_unset=latest_enabled
    )
    if not latest_enabled:
        latest_batch_expression = None

    return PipeConfig(
        tags=env_var(name="TAGS", required=True),
        latest=LatestConfig(
            enabled=latest_enabled,
            batch_expression=latest_batch_expression,
        ),
        backfill=BackfillConfig(
            enabled=backfill_enabled,
            since=env_var_iso8601_datetime(name="BACKFILL_SINCE")
            if backfill_enabled
            else None,
            until=env_var_iso8601_datetime(name="BACKFILL_UNTIL")
            if backfill_enabled
            else None,
            batch_expression=env_var_batch_expression(
                name="BACKFILL_BATCH_EXPRESSION", should_print_unset=backfill_enabled
            )
            if backfill_enabled
            else None,
        ),
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
