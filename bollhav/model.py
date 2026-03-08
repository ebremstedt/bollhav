from datetime import datetime
from typing import Callable
from icron import croniter
from bollhav.intervals import TZInterval
from bollhav.model_config import ModelConfig
from dataclasses import dataclass


@dataclass
class Model:
    model_config: ModelConfig
    execute: Callable

    def get_batch_intervals(
        self, interval: TZInterval, cron_override: str | None = None
    ) -> list[TZInterval]:
        it = croniter(cron_override or self.model_config.cron, interval.since)
        intervals: list[TZInterval] = []
        current = interval.since
        while True:
            tick = it.get_next(datetime)
            if tick >= interval.until:
                break
            intervals.append(TZInterval(current, tick))
            current = tick
        if current < interval.until:
            intervals.append(TZInterval(current, interval.until))
        return intervals
