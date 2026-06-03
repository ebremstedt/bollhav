from __future__ import annotations

import logging
from datetime import datetime, timezone, tzinfo
from uuid import UUID, uuid4

from icron import croniter
from bollhav.model.source_file import SourceFile
from bollhav.model.source_table import SourceTable
from bollhav.model.target import Target
from bollhav.model.bounds import Bounds
from bollhav.model.batch import Batch, _resolve_cron, _chunk_interval
from bollhav.model.intervals import TZInterval
from bollhav.model.directives import Directives
from bollhav.model.kind import Kind
from bollhav.model.state import State
from bollhav.model.tags import Tags
from bollhav.model.upstream import Contract
from roskarl import IntervalExpression, IntervalExpressionExtended

logger = logging.getLogger(__name__)


class Model:
    def __init__(
        self,
        target: Target,
        source: SourceFile | SourceTable | None = None,
        bounds: Bounds | None = None,
        batching: Batch | None = None,
        *,
        kind: Kind,
        tagging: Tags | None = None,
        state: State | None = None,
        enabled: bool = True,
        debug: bool = False,
        description: str | None = None,
        upstream: list[Contract | str] | None = None,
        **kwargs,
    ):
        self.source = source
        self.target = target
        self.bounds = bounds or Bounds()
        self.batching = batching
        self.kind = kind
        self.state = state
        self.enabled = enabled
        self.debug = debug
        self.description = description
        self.upstream: list[Contract | str] = upstream or []
        self.directives = Directives()
        self.tags: set[str] = (tagging or Tags()).assemble(
            self.target.name, self.target.schema.name, self.target.catalog
        )
        self.intervals: tuple[TZInterval, ...] | tuple[None] = (None,)

        # Runtime state stashed by the lifecycle hooks + action runners.
        # Declared up-front so type-checkers don't complain at the
        # mutation sites in state.py / lifecycle.py / actions.py.
        self._state_run_id: UUID | None = None

        # `kind` is the single source of truth for the unit of work; the
        # rest of the model must be consistent with it. Validate up front so
        # a contradiction (e.g. a monolith with batching) fails at definition,
        # not silently at run time.
        if kind is Kind.INTERVAL and batching is None:
            raise ValueError(
                f"model {target.name!r} is kind=INTERVAL but has no batching — "
                f"an interval model's unit of work is a time window. Add "
                f"`batching=Batch(...)` (or pick kind=MONOLITHIC for a "
                f"whole-table load)."
            )
        if kind is Kind.MONOLITHIC and batching is not None:
            raise ValueError(
                f"model {target.name!r} is kind=MONOLITHIC but has batching — "
                f"a monolithic model is one whole-table unit, not windowed. "
                f"Drop `batching` (or pick kind=INTERVAL)."
            )
        if kind is Kind.VIEW:
            if batching is not None:
                raise ValueError(
                    f"model {target.name!r} is kind=VIEW but has batching — a "
                    f"view isn't windowed. Drop `batching`."
                )
            if target.staging is not None:
                raise ValueError(
                    f"model {target.name!r} is kind=VIEW but has staging — a "
                    f"view has nothing to stage. Drop `staging`."
                )
            if target.recreate_table or target.truncate_table:
                raise ValueError(
                    f"model {target.name!r} is kind=VIEW — recreate_table / "
                    f"truncate_table don't apply to views."
                )

        # Upstream is only meaningful with state: a contract's satisfaction is
        # checked by the state machine (`@execute_lifecycle`), which only runs
        # for state-tracked models. Declaring upstream without state would
        # silently never enforce it, so make it an error.
        if self.upstream and state is None:
            raise ValueError(
                f"model {target.name!r} declares upstream but has no state — "
                f"upstream contracts are only checked for state-tracked models. "
                f"Add state=State(...), or drop upstream."
            )

        self.extra = kwargs

        logger.debug(
            "Initialized model %r (enabled=%s)", self.target.full_name, self.enabled
        )
        if self.debug:
            self.pretty()

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
            f"  upstream:      {', '.join(self.upstream_names) if self.upstream else '(none)'}",
            "",
            "  target:",
            f"    name:        {self.target.name_resolved}",
            f"    schema:      {self.target.schema.resolved}",
            *(
                [f"    catalog:     {self.target.catalog}"]
                if self.target.catalog
                else []
            ),
            f"    write_mode:  {self.target.write_mode.value}",
            f"    kind:        {self.kind.value}",
            f"    partitioned: {self.target.partitioned_by}",
            f"    columns ({len(cols)}): {col_summary}",
        ]
        if isinstance(self.source, SourceTable):
            lines += [
                "",
                "  source (table):",
                f"    name:        {self.source.name}",
                f"    schema:      {self.source.schema}",
                f"    dsn_env_var: {self.source.dsn_env_var}",
            ]
        elif isinstance(self.source, SourceFile):
            lines += [
                "",
                "  source (file):",
                f"    name:        {self.source.name}",
                f"    path:        {self.source.path}",
                f"    encoding:    {self.source.encoding}",
                f"    separator:   {self.source.separator}",
            ]
        if self.batching is None:
            lines += [
                "",
                "  batching:    (none — single run, read all)",
            ]
        else:
            lines += [
                "",
                "  batching:",
                f"    interval:    expression={self.batching.interval.expression}, "
                f"lookback={self.batching.interval.lookback}",
                f"    size:        {self.batching.size}",
                f"    retries:     {self.batching.retries}",
            ]
        lines += [
            "",
            "  bounds:",
            f"    begin:       {self.bounds.begin}",
            f"    end:         {self.bounds.end}",
        ]
        logger.debug("\n".join(lines))

    @property
    def upstream_names(self) -> list[str]:
        """The full names of this model's upstreams, regardless of whether
        each was declared as a bare string or a `Contract`. Used wherever
        only the identity matters (library registration, banner, repr)."""
        return [u.name if isinstance(u, Contract) else u for u in self.upstream]

    @property
    def stateful(self) -> bool:
        """True when this model tracks state (`state=State(...)`).

        A live derivation, not a cached flag — `STATE_DISABLED` nulls
        `self.state` at runtime, and the lifecycle hooks must see that."""
        return self.state is not None

    @property
    def run_id(self) -> UUID:
        """The run_id for this pipeline invocation. Minted lazily on first
        access and stashed on the model so the bootstrap and every
        interval's state transitions share one id for the whole run."""
        if self._state_run_id is None:
            self._state_run_id = uuid4()
        return self._state_run_id

    # ── output shape: does this model produce a table or a view? ──────

    @property
    def is_table(self) -> bool:
        """True when this model produces a TABLE — any non-view kind
        (`INTERVAL` or `MONOLITHIC`). For asset-side decisions that only
        care about table-vs-view."""
        return self.kind is not Kind.VIEW

    @property
    def is_view(self) -> bool:
        """True when this model produces a VIEW. Its state is a single
        existence row; an upstream is satisfied when that row says the view
        exists."""
        return self.kind is Kind.VIEW

    # ── exact kind ────────────────────────────────────────────────────

    @property
    def is_kind_interval(self) -> bool:
        """True when `kind=Kind.INTERVAL` — batched, one state row per window."""
        return self.kind is Kind.INTERVAL

    @property
    def is_kind_monolithic(self) -> bool:
        """True when `kind=Kind.MONOLITHIC` — whole-table load, one state row."""
        return self.kind is Kind.MONOLITHIC

    @property
    def is_kind_view(self) -> bool:
        """True when `kind=Kind.VIEW` — a view, one existence state row."""
        return self.kind is Kind.VIEW

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
        interval_expression_override: IntervalExpression
        | IntervalExpressionExtended
        | None = None,
        tz_override: tzinfo | None = None,
    ) -> TZInterval:
        """Return the most recent fully elapsed interval as a TZInterval.

        "Complete" means the interval's entire time window has passed.
        An in-progress interval is never returned — e.g. at 14:35 with
        an hourly expression, the 14:00-15:00 interval is still running,
        so this returns 13:00-14:00.

        Uses the provided interval expression and timezone if set,
        otherwise falls back to the model's own. Raises if the model has
        no batching and no override is supplied."""
        if interval_expression_override is None and self.batching is None:
            raise ValueError(
                f"latest_complete_interval on model {self.target.full_name!r} "
                f"needs either an override or a configured batching."
            )
        cron_expression = _resolve_cron(
            interval_expression_override
            or (self.batching.interval.expression if self.batching else "@daily")
        )
        tz = tz_override or (
            self.batching.interval.tz if self.batching else timezone.utc
        )
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
        # Loop invariant: at least 2 ticks consumed before the break
        # (the cron is seeded `interval_size * 3` before now), so both
        # `prev` and `curr` are populated. If this ever fires the cron
        # iterator returned a tick >= now on the first or second call.
        if prev is None or curr is None:
            raise RuntimeError(
                f"cron seeding invariant violated for {cron_expression!r} on "
                f"{self.target.full_name!r}: iterator returned a tick >= now "
                f"within the first two steps"
            )
        return TZInterval(prev, curr)

    def _apply_lookback(self, cron_expression: str, since: datetime) -> datetime:
        if self.batching is None:
            raise ValueError(
                f"_apply_lookback called on model {self.target.full_name!r} "
                f"with no batching configured — lookback is an interval feature"
            )
        if self.batching.interval.lookback is None:
            raise ValueError(
                f"_apply_lookback called on model {self.target.full_name!r} "
                f"with batching.interval.lookback unset — set a non-negative "
                f"int to enable lookback"
            )
        it = croniter(cron_expression, since)
        tick1 = it.get_next(datetime)
        tick2 = it.get_next(datetime)
        tick_size = tick2 - tick1
        return since - (tick_size * self.batching.interval.lookback)

    def compute_intervals(self) -> tuple[TZInterval, ...] | tuple[None]:
        """Resolve and chunk a time interval into TZIntervals.

        Pure computation from the model's own settings + `directives`.
        Call this once — at a point where directives are final (e.g.
        `@load_models` discovery, after pipe overrides bake in) — and
        assign the result to `model.intervals`. The result depends on
        `datetime.now()` (via `latest_complete_interval`), which is the
        reason it isn't recomputed on every read: snapshot it once.

        Returns `(None,)` when the model has no `batching` configured —
        signalling to callers that no interval filtering should be
        applied to the read (the model runs once, unfiltered).

        All inputs come from the model's own settings (with pipe overrides
        already baked in by `apply_pipe`) and its `directives`.

        Two cron expressions are consulted:
            batching.interval.expression          defines the chunk size — how
                                                  the resolved interval is split
                                                  into TZIntervals.
            batching.interval.window_expression   defines the scope for `latest`
                                                  mode only; defaults to
                                                  `expression` when unset.

        Three modes, evaluated in this order:

        1. latest (directives.latest)
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

        2. reload (directives.reload)
           since = model.bounds.begin
           until = model.bounds.end, or latest complete interval end:

            expression  | until resolves to
            ------------|---------------------
            @hourly     | 2024-06-15 14:00 UTC
            @daily      | 2024-06-15 00:00 UTC
            @weekly     | 2024-06-09 00:00 UTC

        3. backfill (default)
           since = directives.since, or bounds.begin if unset
           until = directives.until — required. Raises if unset.

           Backfill is the "specific window" mode: both ends must be
           pinned. For "to the latest complete tick" use latest mode;
           for "to bounds.end (with a latest-tick fallback)" use
           reload mode. Backfill never silently extends to "now."
        """
        if self.batching is None:
            return (None,)

        d = self.directives
        tz = self.batching.interval.tz
        expr = self.batching.interval.expression

        if d.latest:
            windowexpr = self.batching.interval.window_expression or expr
            interval = self.latest_complete_interval(windowexpr, tz)
            since, until = interval.since, interval.until
        elif d.reload:
            if self.bounds.begin is None:
                raise ValueError(
                    f"reload requires bounds.begin to be set on model "
                    f"{self.target.full_name!r}"
                )
            since = self.bounds.begin
            until = self.bounds.end or self.latest_complete_interval(expr).until
        else:
            since = d.since or self.bounds.begin
            if since is None:
                raise ValueError(
                    f"backfill requires a since value — set bounds.begin on model "
                    f"{self.target.full_name!r} or pass --since at runtime"
                )
            if d.until is None:
                raise ValueError(
                    f"backfill requires an explicit until on model "
                    f"{self.target.full_name!r} — set BACKFILL_UNTIL (or "
                    f"directives.until programmatically). Backfill means a "
                    f'specific window; for "to the latest complete tick" use '
                    f'latest mode, for "to bounds.end" use reload mode.'
                )
            until = d.until

        cron_expression = _resolve_cron(expr)
        if self.batching.interval.lookback:
            since = self._apply_lookback(cron_expression, since)

        return tuple(_chunk_interval(cron_expression, TZInterval(since, until)))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Model):
            return NotImplemented
        return self.__dict__ == other.__dict__
