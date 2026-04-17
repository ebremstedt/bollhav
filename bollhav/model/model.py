from __future__ import annotations

import logging
from datetime import datetime, tzinfo
from typing import TYPE_CHECKING

from icron import croniter
from bollhav.model.source import Source
from bollhav.model.target import Target
from bollhav.model.bounds import Bounds
from bollhav.model.batch import Batch, _resolve_cron, _chunk_interval
from bollhav.model.intervals import TZInterval
from bollhav.model.runtime_override import RuntimeOverride
from bollhav.model.tags import Tags
from roskarl import BatchExpression, BatchExpressionExtended

if TYPE_CHECKING:
    from bollhav.pipe.pipe_config import PipeConfig

logger = logging.getLogger(__name__)


class Model:
    def __init__(
        self,
        target: Target,
        source: Source | None = None,
        bounds: Bounds | None = None,
        batching: Batch | None = None,
        tagging: Tags | None = None,
        enabled: bool = True,
        debug: bool = False,
        description: str | None = None,
        upstream: list[str] | None = None,
        **kwargs,
    ):
        self.source = source
        self.target = target
        self.bounds = bounds or Bounds()
        self.batching = batching or Batch()
        self.enabled = enabled
        self.debug = debug
        self.description = description
        self.upstream: list[str] = upstream or []
        self.runtime_override = RuntimeOverride()
        self.tags: set[str] = (tagging or Tags()).assemble(
            self.target.name, self.target.schema.name
        )

        self.extra = kwargs

        logger.debug(
            "Initialized model %r (enabled=%s)", self.target.full_name, self.enabled
        )
        if self.debug:
            self.pretty()

    def apply_pipe(self, pipe: PipeConfig) -> None:
        """Apply pipe-level config to this model — sets runtime_override state
        and propagates the schema suffix to the target. Call before
        infer_intervals()."""
        self.runtime_override.apply_pipe(pipe)
        self.target.schema.suffix = self.runtime_override.schema_suffix

    def pretty(self) -> None:
        cols = self.target.columns
        unique_cols = [c.name for c in self.target.unique_columns]
        col_summary = ", ".join(
            f"{c.name}*" if c.name in unique_cols else c.name for c in cols
        )
        lines = [
            f"Model: {self.target.full_name}",
            f"  enabled:       {self.enabled}",
            f"  description:   {self.description}",
            f"  tags:          {', '.join(sorted(self.tags))}",
            f"  upstream:      {', '.join(self.upstream) if self.upstream else '(none)'}",
            "",
            "  target:",
            f"    name:        {self.target.name}",
            f"    schema:      {self.target.schema.resolved}",
            f"    write_mode:  {self.target.write_mode.value}",
            f"    model_type:  {self.target.model_type.value}",
            f"    partitioned: {self.target.partitioned_by}",
            f"    columns ({len(cols)}): {col_summary}",
        ]
        if self.source:
            lines += [
                "",
                "  source:",
                f"    name:        {self.source.name}",
                f"    schema:      {self.source.schema}",
                f"    dsn_env_var: {self.source.dsn_env_var}",
            ]
        lines += [
            "",
            "  batching:",
            f"    expression:  {self.batching.batch_expression}",
            f"    lookback:    {self.batching.lookback}",
            f"    retries:     {self.batching.retries}",
            "",
            "  bounds:",
            f"    begin:       {self.bounds.begin}",
            f"    end:         {self.bounds.end}",
        ]
        logger.debug("\n".join(lines))

    def __repr__(self) -> str:
        return (
            f"Model("
            f"name={self.target.full_name!r}, "
            f"source={self.source!r}, "
            f"target={self.target!r}, "
            f"bounds={self.bounds!r}, "
            f"batching={self.batching!r}, "
            f"tags={self.tags!r}, "
            f"enabled={self.enabled}, "
            f"debug={self.debug}, "
            f"description={self.description!r}, "
            f"upstream={self.upstream!r}, "
            f"extra={self.extra!r})"
        )

    def latest_complete_interval(
        self,
        batch_expression_override: BatchExpression
        | BatchExpressionExtended
        | None = None,
        tz_override: tzinfo | None = None,
    ) -> TZInterval:
        """Return the most recent fully elapsed interval as a TZInterval.

        "Complete" means the interval's entire time window has passed.
        An in-progress interval is never returned — e.g. at 14:35 with
        an hourly expression, the 14:00-15:00 interval is still running,
        so this returns 13:00-14:00.

        Uses the provided batch expression and timezone if set,
        otherwise falls back to the model's own."""
        cron_expression = _resolve_cron(
            batch_expression_override or self.batching.batch_expression
        )
        tz = tz_override or self.batching.tz
        now = datetime.now(tz=tz)
        # Get two ticks from now to measure the interval size, then seed
        # far enough back to guarantee at least two ticks before now.
        probe = croniter(cron_expression, now)
        tick1 = probe.get_next(datetime)
        tick2 = probe.get_next(datetime)
        interval_size = tick2 - tick1
        it = croniter(cron_expression, now - (interval_size * 3))
        prev, curr = None, None
        while True:
            tick = it.get_next(datetime)
            if tick >= now:
                break
            prev, curr = curr, tick
        return TZInterval(prev, curr)

    def _apply_lookback(self, cron_expression: str, since: datetime) -> datetime:
        it = croniter(cron_expression, since)
        tick1 = it.get_next(datetime)
        tick2 = it.get_next(datetime)
        tick_size = tick2 - tick1
        return since - (tick_size * self.batching.lookback)

    def infer_intervals(self) -> list[TZInterval] | list[None]:
        """Resolve and chunk a time interval into TZIntervals.

        Returns `[None]` when the source is marked `is_unfiltered` — signalling
        to callers that no interval filtering should be applied to the read.

        All inputs come from the model's own settings and runtime_override.
        Call runtime_override.apply_pipe(pipe) before calling this method.

        Two cron expressions are resolved:
            batch_expression   defines the chunk size — how the resolved interval
                               is split into TZIntervals for processing. Used in
                               all three modes, and as the fallback tick when
                               resolving an open-ended `until` in reload/backfill.
            window_expression  defines the scope for `latest` mode only — the
                               outer interval to catch up on. Defaults to
                               `batch_expression` when unset. Irrelevant for
                               reload/backfill, which get bounds explicitly.

        Resolution order:
            batch_expression:   runtime_override > model's own batch expression
            window_expression:  runtime_override > model's own window expression
                                > batch_expression (fallback; latest mode only)
            timezone:           runtime_override > model's own timezone

        Three modes, evaluated in this order:

        1. latest (runtime_override.latest)
           Both since and until are inferred by finding the last two
           cron ticks before now:

            @hourly, now is 2024-06-15 14:35 UTC:

                ticks: ... 12:00  13:00  14:00  [14:35]  15:00
                                  ^^^^^  ^^^^^
                                  since  until

                14:00-15:00 is still in progress -> 13:00-14:00.

            @daily, now is 2024-06-15 14:35 UTC:

                ticks: ... Jun 13  Jun 14  Jun 15  [14:35]  Jun 16
                                   ^^^^^^  ^^^^^^
                                   since   until

                Jun 15-16 is still in progress -> Jun 14-15.

        2. reload (runtime_override.reload)
           since = model.bounds.begin
           until = model.bounds.end, or latest complete interval end:

            expression  | until resolves to
            ------------|---------------------
            @hourly     | 2024-06-15 14:00 UTC
            @daily      | 2024-06-15 00:00 UTC
            @weekly     | 2024-06-09 00:00 UTC

        3. backfill (default)
           since = runtime_override.since
           until = runtime_override.until, or latest complete interval end
                   (same fallback table as reload above)
        """
        if self.source and self.source.is_unfiltered:
            return [None]

        rt = self.runtime_override
        tz = rt.tz or self.batching.tz
        batchexpr = rt.batch_expression or self.batching.batch_expression

        if rt.latest:
            windowexpr = (
                rt.window_expression or self.batching.window_expression or batchexpr
            )
            interval = self.latest_complete_interval(windowexpr, tz)
            since, until = interval.since, interval.until
        elif rt.reload:
            if self.bounds.begin is None:
                raise ValueError(
                    f"reload requires bounds.begin to be set on model "
                    f"{self.target.full_name!r}"
                )
            since = self.bounds.begin
            until = self.bounds.end or self.latest_complete_interval(batchexpr).until
        else:
            since = rt.since or self.bounds.begin
            if since is None:
                raise ValueError(
                    f"backfill requires a since value — set bounds.begin on model "
                    f"{self.target.full_name!r} or pass --since at runtime"
                )
            until = rt.until or self.latest_complete_interval(batchexpr).until

        cron_expression = _resolve_cron(batchexpr)
        if self.batching.lookback:
            since = self._apply_lookback(cron_expression, since)

        return _chunk_interval(cron_expression, TZInterval(since, until))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Model):
            return NotImplemented
        return self.__dict__ == other.__dict__
