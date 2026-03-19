from dataclasses import dataclass
from datetime import datetime

from icron import croniter
from bollhav.model.intervals import TZInterval
from roskarl import CronBatch, CronBatchExtended, CRON_BATCH_SHORTCUTS, CRON_BATCH_EXTENDED_SHORTCUTS


@dataclass
class Batch:
    """
    Controls how a time interval is split into smaller chunks for processing.

    `default` is a cron batch expression that defines the chunk size. For example,
    "0 * * * *" means each chunk is one hour, "0 0 * * *" means one day.
    A pipeline can pass an override at runtime, but this is the fallback.

    `lookback` extends the start of the interval backwards by N cron-ticks.
    If the cron is hourly and lookback is 3, processing starts 3 hours before
    the interval's natural start. Useful for reprocessing recent history to
    account for late-arriving data.

    `retries` is the number of times a failed chunk should be retried.
    """

    default: CronBatch | CronBatchExtended = "0 0 * * *"
    lookback: int | None = None
    retries: int | None = None

    def infer_intervals_from_cron_batch(
        self,
        interval: TZInterval,
        override: CronBatch | CronBatchExtended | None = None,
    ) -> list[TZInterval]:
        cron = override or self.default
        raw = override or self.default
        cron = CRON_BATCH_SHORTCUTS.get(raw) or CRON_BATCH_EXTENDED_SHORTCUTS.get(raw) or raw
        since = interval.since
        if self.lookback:
            it = croniter(cron, since)
            tick1 = it.get_next(datetime)
            tick2 = it.get_next(datetime)
            tick_size = tick2 - tick1
            since = since - (tick_size * self.lookback)
        it = croniter(cron, since)
        intervals: list[TZInterval] = []
        current = since
        while True:
            tick = it.get_next(datetime)
            if tick >= interval.until:
                break
            intervals.append(TZInterval(current, tick))
            current = tick
        if current < interval.until:
            intervals.append(TZInterval(current, interval.until))
        return intervals
