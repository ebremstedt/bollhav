"""A model's inputs — one list, one class.

Every input a model has is a `Source` in its `upstream` list. A `Source`
answers two independent questions:

* **what is it?** — its `type`: a `SourceModel` (relational: a managed
  model, an external table, or a view), a `SourceFile`, or a `SourceApi`.
  `type=None` is the sentinel for unknown provenance.
* **is it gated?** — its `contract`: present ⇒ the state machine waits for
  it before the downstream runs (a managed upstream); absent ⇒ ungated
  (an external source that's assumed always present).

A `contract` is only valid on a `SourceModel` — files and APIs aren't
state-tracked, so they can never gate.

Only a `SourceModel` is SQL-addressable: `model.ref(name)` resolves it into
a `FROM`, suffix-aware when it's gated (a managed model that moves across
dev / prod / PR) and literal when it isn't (an external table at a fixed
location). Files and APIs are read by the read function, not in SQL.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from bollhav.model.upstream import Contract


@dataclass
class SourceModel:
    """Relational input — a managed model, an external table, or a view.
    Carries the config to read it. Set `query` to define a view."""

    schema: str | None = None
    catalog: str | None = None
    dsn_env_var: str | None = None
    query: str | None = None
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
    when a model declares nothing)."""

    name: str
    type: SourceModel | SourceFile | SourceApi | None = None
    contract: Contract | None = None

    def __post_init__(self) -> None:
        if self.contract is not None and not isinstance(self.type, SourceModel):
            raise ValueError(
                f"source {self.name!r} has a contract but type="
                f"{type(self.type).__name__} — only a SourceModel can be gated "
                f"(files / APIs aren't state-tracked). Drop the contract or make "
                f"it a SourceModel."
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
