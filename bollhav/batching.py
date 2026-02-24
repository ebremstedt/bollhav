from enum import Enum
from croniter import croniter


class BatchSize(Enum):
    YEARLY = "YEARLY"
    MONTHLY = "MONTHLY"
    WEEKLY = "WEEKLY"
    DAILY = "DAILY"
    HOURLY = "HOURLY"
    SUBHOURLY = "SUBHOURLY"


def infer_batch_size(cron: str) -> BatchSize | None:
    if not croniter.is_valid(cron):
        raise ValueError(f"Invalid cron expression: {cron!r}")

    minute, hour, day, month, weekday = cron.split()

    if month != "*":
        return BatchSize.YEARLY
    if day != "*" and weekday == "*":
        return BatchSize.MONTHLY
    if weekday != "*":
        return BatchSize.WEEKLY
    if hour != "*":
        return BatchSize.DAILY
    if minute == "0":
        return BatchSize.HOURLY
    return None
