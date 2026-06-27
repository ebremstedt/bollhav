from __future__ import annotations

from typing import NamedTuple, TYPE_CHECKING

import psycopg


from ._naming import state_table_name
from ._ddl import LIBRARY_SCHEMA

if TYPE_CHECKING:
    from bollhav.model.model import Model


# ── errors ──
class MissingStateConnError(ValueError):
    """`PostgresState` was used without an injected connection. It doesn't open
    its own — the caller owns it (opened in `main()`, threaded through the
    lifecycle hooks) — so a missing connection is a wiring error."""

    def __init__(self) -> None:
        super().__init__(
            "a state connection is required — construct "
            "PostgresState(model, conn=<state_conn>). PostgresState "
            "does not self-connect."
        )


class LibraryEntry(NamedTuple):
    """A row read from the library. `temporality` (`temporal` | `timeless`) drives
    how an upstream's satisfaction is checked. Every state-tracked model now
    has a state table, so `state_schema` /
    `state_table` are set for all of them; they are None only for
    library-only rows written by older bollhav images.

    `upstream` is managed-model edges (names; the edge's type is the upstream
    model's own `temporality`, joinable). `sources` is external boundary inputs,
    each `{"name", "kind"}` — typed here because a source isn't a model and
    so has no row of its own to carry its kind. Together they're the model's
    typed lineage inputs."""

    upstream: list[str]
    model_type: str
    state_schema: str | None
    state_table: str | None
    temporality: str
    fixed_intervals: bool = True
    sources: list[dict] = []
    metadata: dict = {}


class _PostgresStateBase:
    """Postgres-backed state store for a single model.

    Construct with the model and the caller-owned state connection
    (opened in `main()`, threaded through the lifecycle hooks). The
    naming helpers (`_state_schema`, `_state_table`) work with the
    connection unset; every DB method requires it."""

    def __init__(self, model: "Model", conn: psycopg.Connection | None = None) -> None:
        self.model = model
        self.conn = conn

    if TYPE_CHECKING:
        # Provided at runtime by the `Library` mixin. Declared here (type-only,
        # no runtime body) so the other mixins type-check `self.lookup_model(...)`
        # under the composed `PostgresState` — and so test mocks patching
        # `PostgresState.lookup_model` still intercept it (a direct
        # `Library.lookup_model` call would bypass them).
        @staticmethod
        def lookup_model(
            conn: psycopg.Connection,
            full_name: str,
            library_schema: str = LIBRARY_SCHEMA,
        ) -> "LibraryEntry | None": ...

    def _require_conn(self) -> psycopg.Connection:
        """Return the injected state connection. `PostgresState` doesn't
        open its own — the caller owns it (opened in `main()`, threaded
        through the lifecycle hooks). Raises if none was passed."""
        if self.conn is None:
            raise MissingStateConnError()
        return self.conn

    def _env_schema(self, base: str) -> str:
        """Suffix a bollhav-owned schema with this model's schema suffix, so a
        `SCHEMA_SUFFIX` run gets its own isolated state + library environment
        (`z_bollhav` → `z_bollhav_pr123_2614_`). No suffix → unchanged, so prod
        is untouched."""
        from bollhav.model.target import resolve_schema_name

        return resolve_schema_name(
            base,
            self.model.target.schema_suffix,
            self.model.target.schema_suffix_appendix,
        )

    def _state_schema(self) -> str:
        """State tables live in the one bollhav schema, alongside the library +
        errors — `z_bollhav` (prod), or `z_bollhav_<suffix>` for a dev branch.
        State tables are digest-named, so they never clash with the fixed
        `library` / `errors` tables sharing the schema."""
        return self._library_schema()

    def _library_schema(self) -> str:
        """Cross-pipeline library + errors schema, per-environment under a
        `SCHEMA_SUFFIX` (`z_bollhav` → `z_bollhav_pr123_2614_`). A dev branch
        thus registers and gates against its OWN library, never prod's."""
        return self._env_schema(LIBRARY_SCHEMA)

    def _suffix_upstream_name(self, full_name: str) -> str:
        """Apply this model's schema suffix to an upstream's dotted name so the
        gating lookup matches how that upstream registered in THIS environment.
        The declared reference stays canonical (so `ref()` still resolves it);
        only the lookup key is suffixed."""
        if not self.model.target.schema_suffix:
            return full_name
        from bollhav.model.target import resolve_schema_name

        parts = full_name.split(".")
        if len(parts) >= 2:
            parts[-2] = resolve_schema_name(
                parts[-2],
                self.model.target.schema_suffix,
                self.model.target.schema_suffix_appendix,
            )
        return ".".join(parts)

    def _state_table(self) -> str:
        """Deterministic state-table name
        (`state_table_name(canonical_full_name)`). Computed from the model's
        *canonical* name — base schema, no `schema_suffix`/appendix — so the
        name is stable across the suffix+week rotation that the enclosing
        schema (`z_bollhav_<suffix>_<week>_`) carries. One cleanly-named state
        table per model inside each schema; any process recomputes it without
        a lookup."""
        return state_table_name(self.model.target.canonical_full_name)
