from __future__ import annotations

import json
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
from bollhav.model.upstream import Contract, Source, SourceKind
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
        sources: list[Source | str] | None = None,
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
        self.sources: list[Source | str] = sources or []
        self.directives = Directives()
        self.tags: set[str] = (tagging or Tags()).assemble(
            self.target.name, self.target.schema.name, self.target.catalog
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
        """Upstream contracts are only enforced for state-tracked models (the
        state machine checks them), so declaring `upstream` without `state`
        would silently never enforce it — make that a definition-time error."""
        if self.upstream and self.state is None:
            raise ValueError(
                f"model {self.target.name!r} declares upstream but has no "
                f"state — upstream contracts are only checked for state-tracked "
                f"models. Add state=State(...), or drop upstream."
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
    def source_names(self) -> list[str]:
        """The names of this model's declared external `sources`, whether
        each was declared as a bare string or a `Source`. External inputs
        bollhav reads but does not manage; resolved by `source_ref()`."""
        return [s.name if isinstance(s, Source) else s for s in self.sources]

    @property
    def source_specs(self) -> list[dict]:
        """Typed external sources as `[{"name", "kind"}]` — what the library
        stores for typed lineage. A bare-string source defaults to kind
        `database`. (Upstream edges don't need this: an upstream's type is the
        upstream model's own `kind`, joinable from its library row.)"""
        out: list[dict] = []
        for s in self.sources:
            if isinstance(s, Source):
                out.append({"name": s.name, "kind": s.kind.value})
            else:
                out.append({"name": s, "kind": SourceKind.DATABASE.value})
        return out

    @property
    def declared_inputs(self) -> list[str]:
        """Every declared input — managed `upstream`s + external `sources` —
        by name. The model's known provenance. Empty means the model declares
        nothing, so where its data comes from is untracked (`inputs_known` is
        False) — it reads from hardcoded SQL or a Python read with no
        declarations."""
        return self.upstream_names + self.source_names

    @property
    def inputs_known(self) -> bool:
        """False when the model declares no upstreams and no sources — its
        data provenance is unknown for lineage. Use it to audit which models
        have untracked inputs."""
        return bool(self.declared_inputs)

    @property
    def upstream_specs(self) -> list[dict]:
        """Typed managed upstreams as `[{"name", "kind"}]`. `kind` is the
        contract kind (`interval` / `view` / `monolithic`), or `None` for a
        bare-string upstream that didn't declare one. Symmetric with
        `source_specs`; together they're the model's typed lineage inputs."""
        out: list[dict] = []
        for u in self.upstream:
            if isinstance(u, Contract):
                out.append({"name": u.name, "kind": u.kind})
            else:
                out.append({"name": u, "kind": None})
        return out

    def lineage(self) -> dict:
        """This model's inputs as a structured dict — the basis for both
        `lineage_json()` and `lineage_tree()`. Upstreams are managed models
        (typed by contract kind); sources are external inputs (typed by
        `SourceKind`). `inputs_known` is False when neither is declared."""
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

    def ref(self, name: str) -> str:
        """Resolve a declared **managed upstream** to its quoted,
        schema-suffix-aware table identifier, for embedding in a read query:

            f"SELECT * FROM {model.ref('warehouse.orders')} WHERE ..."
            -> SELECT * FROM "warehouse"."orders" WHERE ...

        `name` must be one of this model's declared `upstream` entries (bare
        string or `Contract`) — referencing an undeclared upstream raises, so
        the SQL and the dependency graph can't drift. The upstream's schema
        gets the same suffix this run applied to the model's own target, so
        the query is portable across dev / prod / PR environments.

        Use `source()` for external (unmanaged) tables — those resolve
        literally, without the suffix."""
        if name not in set(self.upstream_names):
            declared = sorted(self.upstream_names)
            raise ValueError(
                f"{name!r} is not a declared upstream of "
                f"{self.target.full_name!r} — add it to upstream=[...] before "
                f"referencing it with ref() (declared: {declared or 'none'})"
            )
        return self._resolve_relation(name, apply_suffix=True)

    def source_ref(self, name: str) -> str:
        """Resolve a declared **external source** to its quoted, LITERAL
        table identifier (no schema suffix), for embedding in a read query:

            f"SELECT * FROM {model.source_ref('raw.landing_orders')} WHERE ..."
            -> SELECT * FROM "raw"."landing_orders" WHERE ...

        Named `source_ref` (not `source`) because `model.source` is already
        the model's own read-source definition (`SourceTable`/`SourceFile`).

        `name` must be one of this model's declared `sources` entries (bare
        string or `Source`). External tables bollhav doesn't manage live at
        the same fixed location in every environment, so the suffix is NOT
        applied. They are never gated; declaring them just records the
        lineage boundary where data enters the system.

        (You can always hardcode an external table directly in the SQL
        instead — `source_ref()` is the opt-in for lineage + resolution.)"""
        match: Source | str | None = None
        for s in self.sources:
            if (s.name if isinstance(s, Source) else s) == name:
                match = s
                break
        if match is None:
            declared = sorted(self.source_names)
            raise ValueError(
                f"{name!r} is not a declared source of "
                f"{self.target.full_name!r} — add it to sources=[...] before "
                f"referencing it with source_ref() (declared: {declared or 'none'})"
            )
        if isinstance(match, Source) and not match.sql_addressable:
            raise ValueError(
                f"source {name!r} is kind={match.kind.value} — not SQL-addressable, "
                f"so it can't go in a FROM. Read it in your read function instead; "
                f"source_ref() is only for DATABASE/VIEW sources."
            )
        return self._resolve_relation(name, apply_suffix=False)

    def _resolve_relation(self, name: str, *, apply_suffix: bool) -> str:
        """Split a dotted `[catalog.]schema.table` name, optionally apply
        this model's active schema suffix (managed refs only), and quote per
        `target.database` (Postgres `"x"."y"`, MSSQL `[x].[y]`)."""
        from bollhav.model.database import Database
        from bollhav.model.target_schema import TargetSchema

        parts = name.split(".")
        table = parts[-1]
        schema = parts[-2] if len(parts) >= 2 else None
        catalog = parts[-3] if len(parts) >= 3 else None

        if schema is not None and apply_suffix:
            schema = TargetSchema(
                name=schema,
                suffix=self.target.schema.suffix,
                suffix_appendix=self.target.schema.suffix_appendix,
            ).resolved

        idents = [p for p in (catalog, schema, table) if p]
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
        return self.__dict__ == other.__dict__
