from icron import croniter
from datetime import datetime, timezone, timedelta


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
