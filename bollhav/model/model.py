from __future__ import annotations

import json
import logging
from uuid import uuid4

from bollhav.model.target import Target
from bollhav.model.bounds import Bounds
from bollhav.model.batch import Batch
from bollhav.model.kind import Kind
from bollhav.model.state import State
from bollhav.model.tags import Tags
from bollhav.model.source import Source
from bollhav.model.curfew import Curfew

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
        tags: set[str] | None = None,
        state: State | None = None,
        curfew: Curfew | None = None,
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
        self.curfew = curfew
        self.enabled = enabled
        self.debug = debug
        self.description = description
        self.upstream: list[Source] = upstream or [_unknown_source()]
        self.tags: set[str] = (
            tags
            if tags is not None
            else (tagging or Tags()).assemble(
                self.target.name, self.target.schema, self.target.catalog
            )
        )
        self.extra = kwargs
        self._validate_kind_consistency()
        self._validate_upstream_requires_state()

        logger.debug(
            "Initialized model %r (enabled=%s)", self.target.full_name, self.enabled
        )
        if self.debug:
            self.pretty()

        object.__setattr__(self, "_frozen", True)

    def __setattr__(self, name: str, value: object) -> None:
        if getattr(self, "_frozen", False):
            raise AttributeError(
                f"Model is frozen (an immutable definition); cannot set {name!r}. "
                f"Per-run state belongs on a ModelRun, and pipe/tag overrides "
                f"build a new Model via runtime.apply_runtime_overrides."
            )
        object.__setattr__(self, name, value)

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
                f"    chunk:       {self.batching.time.chunk}, "
                f"lookback={self.batching.time.lookback}",
                f"    size:        {self.batching.size}",
                f"    retries:     {self.batching.retries}",
            ]
        lines += [
            "",
            "  bounds:",
            f"    begin:       {self.bounds.begin}",
            f"    end:         {self.bounds.end}",
        ]
        if self.curfew is not None:
            sense = "allow only" if self.curfew.allowed else "deny"
            wins = (
                ", ".join(f"{s:%H:%M}–{e:%H:%M}" for s, e in self.curfew.windows)
                or "all day"
            )
            days = self.curfew.days
            day_part = "" if days is None else f" on weekdays {sorted(days)}"
            lines += [
                "",
                f"  curfew:        {sense} {wins}{day_part} ({self.curfew.tz})",
            ]
        logger.debug("\n".join(lines))

    @property
    def gated_upstreams(self) -> list[Source]:
        """The Sources that gate this model — those carrying a `contract`, the
        managed upstreams the state machine waits for before this model runs."""
        return [source for source in self.upstream if source.gated]

    @property
    def upstream_names(self) -> list[str]:
        """Names of this model's **gated** upstreams (Sources with a contract).
        Used for run ordering, the library `upstream` column, and gating."""
        return [source.name for source in self.upstream if source.gated]

    @property
    def source_names(self) -> list[str]:
        """Names of this model's **ungated** sources — external inputs with no
        contract and known provenance (the UNKNOWN sentinel is excluded).
        Resolved literally by `ref()`."""
        return [
            source.name
            for source in self.upstream
            if not source.gated and source.type is not None
        ]

    @property
    def source_specs(self) -> list[dict]:
        """Typed ungated sources as `[{"name", "kind"}]` for the library's
        `sources` column and lineage. `kind` is the source type label
        (`model` / `file` / `api`). The UNKNOWN sentinel is excluded."""
        return [
            {"name": source.name, "kind": source.kind}
            for source in self.upstream
            if not source.gated and source.type is not None
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
        return not any(source.type is None for source in self.upstream)

    @property
    def upstream_specs(self) -> list[dict]:
        """Typed gated upstreams as `[{"name", "kind"}]`. `kind` is the
        contract kind (`interval` / `view` / `monolithic`). Symmetric with
        `source_specs`; together they're the model's typed lineage inputs."""
        return [
            {"name": source.name, "kind": source.contract.kind}
            for source in self.upstream
            if source.contract is not None
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
        for source in self.upstream:
            if source.name == name:
                return source
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
        the query is portable across dev / prod / PR.

        A gated source declared `assume_ok=True` is pinned to its canonical
        (unsuffixed) location instead — a shared upstream read from prod even in
        a dev run (where gating also assumes it's okay); see `Source.assume_ok`.

            model.ref('shared.calendar')  ->  "shared"."calendar"   # assume_ok=True source
        """
        src = self._find_source(name)
        if src is None:
            declared = sorted(
                source.name for source in self.upstream if source.type is not None
            )
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
        # Gated upstreams move with the env suffix — unless `assume_ok` pins
        # them to their canonical (prod) location.
        return self._resolve_relation(
            name, apply_suffix=src.gated and not src.assume_ok
        )

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

        A live derivation, not a cached flag. The run-level `run_id` lives on
        the `ModelRun`, not here — the model definition is run-agnostic."""
        return self.state is not None

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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Model):
            return NotImplemented

        def _norm(m: "Model") -> dict:
            # The auto-injected UNKNOWN sentinel carries a per-instance uuid
            # (so each is a distinct lineage node), which would make two
            # otherwise-identical models unequal — drop it before comparing.
            d = dict(m.__dict__)
            d["upstream"] = [source for source in m.upstream if source.type is not None]
            return d

        return _norm(self) == _norm(other)


def _unknown_source() -> Source:
    """The fallback input for a model that declares none — provenance is total,
    so even "declares nothing" is one typeless UNKNOWN source. The uuid name
    makes each unknown a distinct lineage node. A non-empty `upstream` passes
    through untouched, so runtime copies never re-trigger this."""
    return Source(name=f"unknown-{uuid4()}", type=None)
