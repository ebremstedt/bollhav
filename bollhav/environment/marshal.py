from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from roskarl import (
    env_var_bool,
    env_var_cron,
    env_var,
    env_var_iso8601_datetime,
)
from icron import croniter
from functools import wraps
from typing import Callable


@dataclass
class CronConfig:
    enabled: bool
    expression: str | None
    since: datetime | None
    until: datetime | None


@dataclass
class BackfillConfig:
    enabled: bool
    since: datetime | None
    until: datetime | None


@dataclass
class EnvConfig:
    tags: str | None
    cron: CronConfig
    backfill: BackfillConfig
    schema_suffix: str
    production: bool = False
    debug: bool = False

    def __str__(self) -> str:
        return f"EnvConfig(tags={self.tags}, cron={self.cron}, backfill={self.backfill}, debug={self.debug}, production={self.production}, schema_suffix={self.schema_suffix})"

    def debugprint(self, msg: str) -> None:
        if self.debug:
            ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            print(f"{ts} {msg}")

    def __post_init__(self) -> None:
        if not self.production and self.schema_suffix == "":
            raise ValueError("Non-production requires non-empty schema_suffix")

        if self.production:
            self.schema_suffix = ""

        print(f"tags:             {self.tags}")
        print(f"debug:            {self.debug}")
        print(f"production:       {self.production}")
        print(f"schema_suffix:    {self.schema_suffix}")
        if self.cron.enabled:
            print(f"cron.expression:  {self.cron.expression}")
            print(f"cron.since:       {self.cron.since}")
            print(f"cron.until:       {self.cron.until}")
        if self.backfill.enabled:
            print(f"backfill.since:   {self.backfill.since}")
            print(f"backfill.until:   {self.backfill.until}")


def _resolve_cron_interval(expression: str) -> tuple[datetime, datetime]:
    now = datetime.now(tz=timezone.utc)
    cron = croniter(expression, now - timedelta(days=2))
    ticks = []
    while True:
        tick = cron.get_next(datetime)
        if tick >= now:
            break
        ticks.append(tick)
    since = ticks[-2]
    until = ticks[-1]
    return since, until


def load_env_config() -> EnvConfig:
    cron_enabled = env_var_bool(name="CRON_ENABLED", default=False)
    backfill_enabled = env_var_bool(name="BACKFILL_ENABLED", default=False)
    if cron_enabled and backfill_enabled:
        raise ValueError("CRON_ENABLED and BACKFILL_ENABLED cannot both be true")

    cron_expression = env_var_cron(name="CRON_EXPRESSION")
    if not cron_enabled:
        cron_expression = None

    cron_since, cron_until = (
        _resolve_cron_interval(cron_expression) if cron_expression else (None, None)
    )

    return EnvConfig(
        tags=env_var(name="TAGS", required=True),
        cron=CronConfig(
            enabled=cron_enabled,
            expression=cron_expression,
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
        ),
        debug=env_var_bool(name="DEBUG", default=False),
        production=env_var_bool(name="PRODUCTION", default=False),
        schema_suffix=env_var(name="SCHEMA_SUFFIX", default=""),
    )


def with_env_config(func: Callable[[EnvConfig], None]) -> Callable[[], None]:
    @wraps(func)
    def wrapper() -> None:
        env = load_env_config()
        func(env)

    return wrapper
