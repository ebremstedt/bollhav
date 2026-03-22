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
from bollhav.pipe.cron import _resolve_cron_interval


@dataclass
class LatestConfig:
    enabled: bool
    since: datetime | None
    until: datetime | None
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

        print(f"tags:             {self.tags}")
        print(f"debug:            {self.debug}")
        print(f"use_schema_suffix:{self.use_schema_suffix}")
        if self.use_schema_suffix:
            print(f"schema_suffix:    {self.schema_suffix}")
        if self.latest.enabled:
            print(f"latest.batch_expression:{self.latest.batch_expression}")
            print(f"latest.since:     {self.latest.since}")
            print(f"latest.until:     {self.latest.until}")
        if self.backfill.enabled:
            print(f"backfill.since:   {self.backfill.since}")
            print(f"backfill.until:   {self.backfill.until}")
            print(f"backfill.batch_expression:{self.backfill.batch_expression}")


def load_pipe_config() -> PipeConfig:
    latest_enabled = env_var_bool(name="LATEST_ENABLED", default=False)
    backfill_enabled = env_var_bool(name="BACKFILL_ENABLED", default=False)
    if latest_enabled and backfill_enabled:
        raise ValueError("LATEST_ENABLED and BACKFILL_ENABLED cannot both be true")

    latest_batch_expression = env_var_batch_expression(name="LATEST_CRON_BATCH")
    if not latest_enabled:
        latest_batch_expression = None

    cron_since, cron_until = (
        _resolve_cron_interval(latest_batch_expression)
        if latest_batch_expression
        else (None, None)
    )

    return PipeConfig(
        tags=env_var(name="TAGS", required=True),
        latest=LatestConfig(
            enabled=latest_enabled,
            batch_expression=latest_batch_expression,
            since=cron_since,
            until=cron_until,
        ),
        backfill=BackfillConfig(
            enabled=backfill_enabled,
            since=env_var_iso8601_datetime(name="BACKFILL_SINCE")
            if backfill_enabled
            else None,
            until=env_var_iso8601_datetime(name="BACKFILL_UNTIL")
            if backfill_enabled
            else None,
            batch_expression=env_var_batch_expression(name="BACKFILL_CRON_BATCH")
            if backfill_enabled
            else None,
        ),
        debug=env_var_bool(name="DEBUG", default=False),
        use_schema_suffix=env_var_bool(name="USE_SCHEMA_SUFFIX", default=True),
        schema_suffix=env_var(name="SCHEMA_SUFFIX", default=""),
    )


def with_pipe_config(func: Callable[[PipeConfig], None]) -> Callable[[], None]:
    @wraps(func)
    def wrapper() -> None:
        pipe = load_pipe_config()
        func(pipe)

    return wrapper
