from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from bollhav.model.upstream import Freshness, UpstreamContract


# ── errors ──


class SourceContractWithoutModelError(ValueError):
    """A `Source` carries a `contract` but its `type` isn't a `SourceModel` —
    only managed models can be state-gated (files / APIs / hardcoded data
    aren't state-tracked). `name` is the source name; `type_name` is the
    actual type."""

    def __init__(self, name: str, type_name: str) -> None:
        super().__init__(
            f"source {name!r} has a contract but type="
            f"{type_name} — only a SourceModel can be gated "
            f"(files / APIs / hardcoded data aren't state-tracked). Drop the "
            f"contract or make it a SourceModel."
        )


class FreshnessWithoutContractError(ValueError):
    """A `Source` sets `freshness` but has no `contract` — freshness is a
    recency bound on a gated upstream's state, so it needs a contract."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"source {name!r} sets `freshness` but has no contract "
            f"— freshness is a recency bound on a gated upstream's state. "
            f"Add a contract (ENCAPSULATE / THROUGH / WHOLE) or drop freshness."
        )


class FreshnessWithExistsContractError(ValueError):
    """A `Source` sets `freshness` with `contract=EXISTS` — EXISTS never
    inspects state (registration is the whole gate), so there's no
    `applied_at` to age."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"source {name!r} sets `freshness` with contract=EXISTS, "
            f"but EXISTS never inspects state (registration is the whole "
            f"gate) — there's no applied_at to age. Use ENCAPSULATE / "
            f"THROUGH / WHOLE, or drop freshness."
        )


class HardcodedSourceFormError(ValueError):
    """A `SourceHardcoded` didn't set exactly one of `rows` (inline Python
    rows) or `sql` (an inline SQL literal). `rows_set` says whether `rows`
    was the one provided (used to phrase 'both' vs 'neither')."""

    def __init__(self, rows_set: bool) -> None:
        super().__init__(
            "SourceHardcoded needs exactly one of `rows` (inline Python "
            "rows) or `sql` (an inline SQL literal) — "
            + ("both were set." if rows_set else "neither was set.")
        )


class HardcodedSqlWithoutConnError(ValueError):
    """A `SourceHardcoded(sql=...)` was materialized without a `conn` — the
    SQL literal needs the data connection to run against."""

    def __init__(self) -> None:
        super().__init__(
            "SourceHardcoded(sql=...) needs a `conn` to materialize "
            "(it runs the SQL); pass the data connection to to_dataframe()."
        )


if TYPE_CHECKING:
    import polars as pl


@dataclass
class SourceModel:
    """Relational input — a managed model or an external table. Carries the
    config for reading it.

    `read_query` is an optional custom SELECT for reading this upstream. It is
    **declarative config only — bollhav never executes it**; your read code in
    `execute` picks it up (`source.read_query`) instead of hardcoding the SQL.
    (A *view's* defining body lives on the model as `Model.query_builder`, not here —
    this object models an input you read, not an output you produce.)"""

    schema: str | None = None
    catalog: str | None = None
    dsn_env_var: str | None = None
    read_query: str | None = None
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
class SourceHardcoded:
    """Hardcoded input — the data is written **inline, in code**, not read from
    a database / file / API. For small constants maintained in the repo: seed /
    reference / lookup tables, enum mappings, fixtures.

    The data is carried in exactly one of two forms (set one, not both):

    - `rows` — a Python literal: a list of dicts (one per row). bollhav builds
      the DataFrame from it; column names come from the keys.
    - `sql` — a **self-contained** SQL literal that yields the rows: a `VALUES`
      / `SELECT`-literal with *no FROM a real table*. Run on the data
      connection to materialize.

    Like files / APIs it is **never gated** (a contract is rejected — hardcoded
    data is always present, there's nothing to wait on) and **never
    SQL-addressable** (`ref()` raises — it has no stable table identifier; read
    it via `to_dataframe()` and write it like any other DataFrame).

    Because the content fully defines the data, `content_hash` is a stable
    fingerprint of it — edit the literal and the hash moves, which is the natural
    version for change-detection (there's no `_data_modified` on a constant)."""

    rows: Sequence[Mapping[str, Any]] | None = None
    sql: str | None = None
    extra: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if (self.rows is None) == (self.sql is None):
            raise HardcodedSourceFormError(self.rows is not None)

    @property
    def content_hash(self) -> str:
        """A stable (cross-process) fingerprint of the inline content — the
        constant's version. Changes iff the `rows` / `sql` change, so a state
        layer can re-apply on edit. Not Python's salted `hash()`: a SHA-256 over
        a canonical serialization, so it's identical run-to-run."""
        import hashlib
        import json

        if self.sql is not None:
            payload = "sql:" + self.sql
        else:
            payload = "rows:" + json.dumps(
                [dict(r) for r in self.rows or []], sort_keys=True, default=str
            )
        return hashlib.sha256(payload.encode()).hexdigest()[:16]

    def to_dataframe(self, conn: Any | None = None) -> "pl.DataFrame":
        """Materialize the hardcoded data into a polars DataFrame — the
        read-replacement. `rows` builds one directly (no connection needed);
        `sql` runs on `conn` (a DBAPI connection — psycopg or pyodbc) and reads
        the result. Hand the result straight to `write(conn, run, df)`."""
        import polars as pl

        if self.rows is not None:
            return pl.DataFrame([dict(r) for r in self.rows])

        if conn is None:
            raise HardcodedSqlWithoutConnError()
        cur = conn.cursor()
        try:
            cur.execute(self.sql)
            columns = [d[0] for d in cur.description]
            data = [tuple(row) for row in cur.fetchall()]
        finally:
            cur.close()
        return pl.DataFrame(data, schema=columns, orient="row")


@dataclass
class Source:
    """One input on a model's `upstream` list. `type` says what it is (and
    holds its read config); `contract` says whether it gates

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
    type: SourceModel | SourceFile | SourceApi | SourceHardcoded | None = None
    contract: UpstreamContract | None = None
    freshness: Freshness | None = None
    deactivate_for_dev: bool = False

    def __post_init__(self) -> None:
        if self.contract is not None and not isinstance(self.type, SourceModel):
            raise SourceContractWithoutModelError(self.name, type(self.type).__name__)
        if self.freshness is not None:
            # Freshness reads the upstream's applied_at, so it needs a gated
            # upstream with state — and EXISTS never inspects state at all.
            if self.contract is None:
                raise FreshnessWithoutContractError(self.name)
            if self.contract is UpstreamContract.EXISTS:
                raise FreshnessWithExistsContractError(self.name)

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
        """A short label for lineage: model / file / api / hardcoded / unknown."""
        if isinstance(self.type, SourceModel):
            return "model"
        if isinstance(self.type, SourceFile):
            return "file"
        if isinstance(self.type, SourceApi):
            return "api"
        if isinstance(self.type, SourceHardcoded):
            return "hardcoded"
        return "unknown"


__all__ = [
    "Source",
    "SourceModel",
    "SourceFile",
    "SourceApi",
    "SourceHardcoded",
]
