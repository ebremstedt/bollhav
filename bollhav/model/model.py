from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, tzinfo
from uuid import UUID, uuid4

from icron import croniter
from bollhav.model.target import Target
from bollhav.model.bounds import Bounds
from bollhav.model.batch import Batch, _resolve_cron, _chunk_interval
from bollhav.model.intervals import TZInterval
from bollhav.model.directives import Directives
from bollhav.model.kind import Kind
from bollhav.model.state import State
from bollhav.model.tags import Tags
from bollhav.model.source import Source
from roskarl import IntervalExpression, IntervalExpressionExtended

logger = logging.getLogger(__name__)


class Model:
    def __init__(
        self,
        target: Target,
        bounds: Bounds | None = None,
        batching: Batch | None = None,
        *,
        kind: Kind,
        tagging: Tags | None = None,
        state: State | None = None,
        enabled: bool = True,
        debug: bool = False,
        description: str | None = None,
        upstream: list[Source] | None = None,
        **kwargs,
    ):
        self.target = target
        self.bounds = bounds or Bounds()
        self.batching = batching
        self.kind = kind
        self.state = state
        self.enabled = enabled
        self.debug = debug
        self.description = description
        # Every input — gated upstream or ungated source — is one Source in
        # this list. Provenance is total: a model that declares nothing gets a
        # single typeless UNKNOWN source (uuid-named so each unknown is a
        # distinct lineage node). Carried through runtime copies untouched — a
        # non-empty list never re-triggers this.
        self.upstream: list[Source] = upstream or []
        if not self.upstream:
            self.upstream = [Source(name=f"unknown-{uuid4()}", type=None)]
        self.directives = Directives()
        self.tags: set[str] = (tagging or Tags()).assemble(
            self.target.name, self.target.schema, self.target.catalog
        )
        self.intervals: tuple[TZInterval, ...] | tuple[None] = (None,)

        # Runtime state stashed by the lifecycle hooks + action runners.
        # Declared up-front so type-checkers don't complain at the
        # mutation sites in state.py / lifecycle.py / actions.py.
        self._state_run_id: UUID | None = None

        self.extra = kwargs

        # Validate the model is internally consistent at definition time, so a
        # contradiction fails here rather than silently at run time.
        self._validate_kind_consistency()
        self._validate_upstream_requires_state()

        logger.debug(
            "Initialized model %r (enabled=%s)", self.target.full_name, self.enabled
        )
        if self.debug:
            self.pretty()

    # ── construction-time validation ──────────────────────────────────

    def _validate_kind_consistency(self) -> None:
        """`kind` is the single source of truth for the unit of work, so
        batching and the view-only knobs must agree with it:

          * INTERVAL needs batching (its unit is a time window).
          * MONOLITHIC must not have batching (one whole-table unit).
          * VIEW has no batching, staging, or recreate/truncate.
        """
        name = self.target.name
        if self.kind is Kind.INTERVAL and self.batching is None:
            raise ValueError(
                f"model {name!r} is kind=INTERVAL but has no batching — an "
                f"interval model's unit of work is a time window. Add "
                f"`batching=Batch(...)` (or pick kind=MONOLITHIC for a "
                f"whole-table load)."
            )
        if self.kind is Kind.MONOLITHIC and self.batching is not None:
            raise ValueError(
                f"model {name!r} is kind=MONOLITHIC but has batching — a "
                f"monolithic model is one whole-table unit, not windowed. "
                f"Drop `batching` (or pick kind=INTERVAL)."
            )
        if self.kind is Kind.VIEW:
            if self.batching is not None:
                raise ValueError(
                    f"model {name!r} is kind=VIEW but has batching — a view "
                    f"isn't windowed. Drop `batching`."
                )
            if self.target.staging is not None:
                raise ValueError(
                    f"model {name!r} is kind=VIEW but has staging — a view has "
                    f"nothing to stage. Drop `staging`."
                )
            if self.target.recreate_table or self.target.truncate_table:
                raise ValueError(
                    f"model {name!r} is kind=VIEW — recreate_table / "
                    f"truncate_table don't apply to views."
                )

    def _validate_upstream_requires_state(self) -> None:
        """Gating contracts are only enforced for state-tracked models (the
        state machine checks them), so a gated upstream without `state` would
        silently never enforce — make that a definition-time error. Ungated
        sources need no state."""
        if self.gated_upstreams and self.state is None:
            raise ValueError(
                f"model {self.target.name!r} declares a gated upstream (a Source "
                f"with a contract) but has no state — contracts are only checked "
                f"for state-tracked models. Add state=State(...), or drop the "
                f"contract."
            )

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
            f"  upstream:      {', '.join(self.upstream_names) or '(none)'}",
            f"  sources:       {', '.join(self.source_names) or '(none)'}",
            "",
            "  target:",
            f"    name:        {self.target.name_resolved}",
            f"    schema:      {self.target.schema_resolved}",
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
    def gated_upstreams(self) -> list[Source]:
        """The Sources that gate this model — those carrying a `contract`, the
        managed upstreams the state machine waits for before this model runs."""
        return [s for s in self.upstream if s.gated]

    @property
    def upstream_names(self) -> list[str]:
        """Names of this model's **gated** upstreams (Sources with a contract).
        Used for run ordering, the library `upstream` column, and gating."""
        return [s.name for s in self.upstream if s.gated]

    @property
    def source_names(self) -> list[str]:
        """Names of this model's **ungated** sources — external inputs with no
        contract and known provenance (the UNKNOWN sentinel is excluded).
        Resolved literally by `ref()`."""
        return [s.name for s in self.upstream if not s.gated and s.type is not None]

    @property
    def source_specs(self) -> list[dict]:
        """Typed ungated sources as `[{"name", "kind"}]` for the library's
        `sources` column and lineage. `kind` is the source type label
        (`model` / `file` / `api`). The UNKNOWN sentinel is excluded."""
        return [
            {"name": s.name, "kind": s.kind}
            for s in self.upstream
            if not s.gated and s.type is not None
        ]

    @property
    def declared_inputs(self) -> list[str]:
        """Every declared input by name — gated upstreams + ungated sources,
        excluding the UNKNOWN sentinel. Empty means provenance is untracked
        (`inputs_known` is False)."""
        return self.upstream_names + self.source_names

    @property
    def inputs_known(self) -> bool:
        """False when the model's only input is the auto-injected UNKNOWN
        sentinel (it declared no real provenance). Use it to audit which
        models have untracked inputs."""
        return not any(s.type is None for s in self.upstream)

    @property
    def upstream_specs(self) -> list[dict]:
        """Typed gated upstreams as `[{"name", "kind"}]`. `kind` is the
        contract kind (`interval` / `view` / `monolithic`). Symmetric with
        `source_specs`; together they're the model's typed lineage inputs."""
        return [
            {"name": s.name, "kind": s.contract.kind} for s in self.upstream if s.gated
        ]

    def lineage(self) -> dict:
        """This model's inputs as a structured dict — the basis for both
        `lineage_json()` and `lineage_tree()`. Upstreams are gated managed
        models (typed by contract kind); sources are ungated external inputs
        (typed by source kind). `inputs_known` is False when only the UNKNOWN
        sentinel is present."""
        return {
            "model": self.target.full_name,
            "kind": self.kind.value,
            "upstream": self.upstream_specs,
            "sources": self.source_specs,
            "inputs_known": self.inputs_known,
        }

    def lineage_json(self, *, indent: int | None = 2) -> str:
        """`lineage()` serialized to JSON."""
        return json.dumps(self.lineage(), indent=indent)

    def lineage_tree(self) -> str:
        """A little ASCII tree of this model's inputs, each labelled with its
        contract kind (upstream) or source kind. Print it:

            print(model.lineage_tree())

            warehouse.daily_summary (interval)
            ├─ upstream
            │  ├─ warehouse.orders (interval)
            │  └─ warehouse.customers (view)
            └─ sources
               ├─ raw.landing_orders (database)
               └─ vendor.orders (api)
        """
        lin = self.lineage()
        lines = [f"{lin['model']} ({lin['kind']})"]

        groups: list[tuple[str, list[dict]]] = []
        if lin["upstream"]:
            groups.append(("upstream", lin["upstream"]))
        if lin["sources"]:
            groups.append(("sources", lin["sources"]))

        if not groups:
            lines.append("└─ (no declared inputs — provenance unknown)")
            return "\n".join(lines)

        for gi, (label, items) in enumerate(groups):
            last_group = gi == len(groups) - 1
            lines.append(f"{'└─' if last_group else '├─'} {label}")
            child_prefix = "   " if last_group else "│  "
            for ii, item in enumerate(items):
                connector = "└─" if ii == len(items) - 1 else "├─"
                kind = item["kind"] or "unspecified"
                lines.append(f"{child_prefix}{connector} {item['name']} ({kind})")
        return "\n".join(lines)

    def _find_source(self, name: str) -> Source | None:
        for s in self.upstream:
            if s.name == name:
                return s
        return None

    def ref(self, name: str) -> str:
        """Resolve a declared input to a quoted table identifier for embedding
        in a read query — **suffix-aware when it's gated**, literal when it's
        not:

            # gated upstream (managed) — moves with the env's schema suffix
            f"...FROM {model.ref('warehouse.orders')}"  -> "warehouse_pr12"."orders"
            # ungated source (external) — fixed location, no suffix
            f"...FROM {model.ref('raw.landing')}"        -> "raw"."landing"

        `name` must be one of this model's declared `upstream` Sources, and its
        `type` must be a `SourceModel` (a relational input). A `SourceFile` /
        `SourceApi` has no `FROM`, so `ref()` on it raises — read those in your
        read function. Referencing an undeclared name raises, so the SQL and
        the dependency graph can't drift.

        A gated source (one with a contract) is a managed model whose schema
        gets the same suffix this run applied to the model's own target — so
        the query is portable across dev / prod / PR."""
        src = self._find_source(name)
        if src is None:
            declared = sorted(s.name for s in self.upstream if s.type is not None)
            raise ValueError(
                f"{name!r} is not a declared input of "
                f"{self.target.full_name!r} — add it to upstream=[...] before "
                f"referencing it with ref() (declared: {declared or 'none'})"
            )
        if not src.sql_addressable:
            raise ValueError(
                f"input {name!r} is a {src.kind} — not SQL-addressable, so it "
                f"can't go in a FROM. Read it in your read function instead; "
                f"ref() is only for SourceModel inputs."
            )
        return self._resolve_relation(name, apply_suffix=src.gated)

    def _resolve_relation(self, name: str, *, apply_suffix: bool) -> str:
        """Resolve a dotted `[catalog.]schema.table` name to a quoted
        `schema.table` identifier (the catalog is parsed but dropped — it's
        connection-level, not part of the FROM), optionally applying this
        model's active schema suffix (gated refs only), quoted per
        `target.database` (Postgres `"x"."y"`, MSSQL `[x].[y]`)."""
        from bollhav.model.database import Database
        from bollhav.model.target import resolve_schema_name

        parts = name.split(".")
        table = parts[-1]
        schema = parts[-2] if len(parts) >= 2 else None
        # A model may be *referenced* as catalog.schema.table (full identity,
        # for lineage / matching), but ref() always resolves to schema.table:
        # the catalog is connection-level (the DSN you're already on), not part
        # of the FROM. So parse it off the name and drop it from the output.

        if schema is not None and apply_suffix:
            schema = resolve_schema_name(
                schema,
                self.target.schema_suffix,
                self.target.schema_suffix_appendix,
            )

        idents = [p for p in (schema, table) if p]
        if self.target.database is Database.MSSQL:
            return ".".join(f"[{p}]" for p in idents)
        return ".".join('"' + p.replace('"', '""') + '"' for p in idents)

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
        lookback = self._lookback_or_raise()
        it = croniter(cron_expression, since)
        tick1 = it.get_next(datetime)
        tick2 = it.get_next(datetime)
        tick_size = tick2 - tick1
        return since - (tick_size * lookback)

    def _lookback_or_raise(self) -> int:
        """Return the configured lookback (the number of cron ticks to shift
        an interval's `since` back), or raise. Lookback is an interval
        feature, so it requires batching with an explicit non-negative
        `interval.lookback`."""
        if self.batching is None:
            raise ValueError(
                f"lookback is an interval feature, but model "
                f"{self.target.full_name!r} has no batching configured"
            )
        if self.batching.interval.lookback is None:
            raise ValueError(
                f"model {self.target.full_name!r} has batching.interval.lookback "
                f"unset — set a non-negative int to enable lookback"
            )
        return self.batching.interval.lookback

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

        def _norm(m: "Model") -> dict:
            # The auto-injected UNKNOWN sentinel carries a per-instance uuid
            # (so each is a distinct lineage node), which would make two
            # otherwise-identical models unequal — drop it before comparing.
            d = dict(m.__dict__)
            d["upstream"] = [s for s in m.upstream if s.type is not None]
            return d

        return _norm(self) == _norm(other)
