from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from bollhav.model.upstream import Freshness, UpstreamContract


@dataclass
class SourceModel:
    """Relational input — a managed model, an external table, or a view.
    Carries the config to read it. The `query` is set on the `Model` itself."""

    schema: str | None = None
    catalog: str | None = None
    dsn_env_var: str | None = None
    partitioned_by: str | None = None
    infer_schema_length: int | None = None
    """Passed to polars as infer_schema_length — the max rows to scan for
    schema inference (None may scan everything)."""
    extra: dict = field(default_factory=dict)


@dataclass
class SourceFile:
    """File input — CSV / Parquet / JSON, local or object storage. Read by
    the read function (never SQL-addressable)."""

    path: Path
    encoding: str | None = None
    separator: str | None = None
    infer_schema_length: int | None = None
    remove_top_rows: int = 0
    archive_folder: Path | None = None
    dateformat: str | None = None
    file_ending: str | None = None
    extra: dict = field(default_factory=dict)


@dataclass
class SourceApi:
    """API input — REST / HTTP. Read by the read function (never
    SQL-addressable)."""

    base_url: str | None = None
    extra: dict = field(default_factory=dict)


@dataclass
class Source:
    """One input on a model's `upstream` list. `type` says what it is (and
    holds its read config); `contract` says whether it gates.

    A `contract` requires `type` to be a `SourceModel` — files and APIs
    can't be state-gated. `type=None` marks unknown provenance (auto-injected
    when a model declares nothing).

    `deactivate_for_dev` (gated upstreams only) is a dev convenience: in a **suffixed**
    (dev/PR) run, read this upstream from its canonical (prod) location and
    assume its state is okay — don't wait on a copy in your dev env that you
    never built. It has **no effect without a schema suffix**: a prod run reads
    and gates it normally, so the flag never needs flipping between
    environments. To assume a source is okay in *every* environment (prod
    included), don't gate it at all — leave off the contract (an ungated
    Source)."""

    name: str
    type: SourceModel | SourceFile | SourceApi | None = None
    contract: UpstreamContract | None = None
    freshness: Freshness | None = None
    deactivate_for_dev: bool = False

    def __post_init__(self) -> None:
        if self.contract is not None and not isinstance(self.type, SourceModel):
            raise ValueError(
                f"source {self.name!r} has a contract but type="
                f"{type(self.type).__name__} — only a SourceModel can be gated "
                f"(files / APIs aren't state-tracked). Drop the contract or make "
                f"it a SourceModel."
            )
        if self.freshness is not None:
            # Freshness reads the upstream's applied_at, so it needs a gated
            # upstream with state — and EXISTS never inspects state at all.
            if self.contract is None:
                raise ValueError(
                    f"source {self.name!r} sets `freshness` but has no contract "
                    f"— freshness is a recency bound on a gated upstream's state. "
                    f"Add a contract (WINDOW / THROUGH / WHOLE) or drop freshness."
                )
            if self.contract is UpstreamContract.EXISTS:
                raise ValueError(
                    f"source {self.name!r} sets `freshness` with contract=EXISTS, "
                    f"but EXISTS never inspects state (registration is the whole "
                    f"gate) — there's no applied_at to age. Use WINDOW / THROUGH / "
                    f"WHOLE, or drop freshness."
                )

    @property
    def gated(self) -> bool:
        """True when this source carries a contract — the state machine waits
        for it (a managed upstream)."""
        return self.contract is not None

    @property
    def sql_addressable(self) -> bool:
        """True when this source can go in a `FROM` (a SourceModel)."""
        return isinstance(self.type, SourceModel)

    @property
    def kind(self) -> str:
        """A short label for lineage: model / file / api / unknown."""
        if isinstance(self.type, SourceModel):
            return "model"
        if isinstance(self.type, SourceFile):
            return "file"
        if isinstance(self.type, SourceApi):
            return "api"
        return "unknown"


__all__ = [
    "Source",
    "SourceModel",
    "SourceFile",
    "SourceApi",
]
