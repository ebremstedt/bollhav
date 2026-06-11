from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, NamedTuple
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

if TYPE_CHECKING:
    from bollhav.model.intervals import TZInterval
    from bollhav.model.model import Model
    from bollhav.model.upstream import UpstreamCheck

logger = logging.getLogger(__name__)


# ── deterministic state-table naming ─────────────────────────────────
#
# Per-model STATE tables live in the one bollhav schema (`z_bollhav`, or
# `z_bollhav_<suffix>` for a dev branch — see `_library_schema`), named by a
# pure function of the model's `full_name`. ERRORS are NOT per-model: every
# model logs to one shared `errors` table in the same schema, keyed by
# `full_name`.

_VOWELS = frozenset("aeiou")
_SLUG_CAP = 44  # max slug chars; + "_" + 16-hex digest = ≤ 61 (under 63)
_CONTEXT_CAP = 16  # max chars for the de-vowelled catalog/schema context
_HEX_LEN = 16  # 64-bit digest → ~1e-12 collision risk at 10k models


def _devowel(s: str) -> str:
    return "".join(c for c in s if c.lower() not in _VOWELS)


def _name_digest(full_name: str) -> str:
    """16-hex (64-bit) blake2b digest of the model's full name — the
    uniqueness tail of every state/error table name and the stem of their
    index names. Deterministic (blake2b, not the salted built-in hash)."""
    return hashlib.blake2b(
        full_name.encode("utf-8"), digest_size=_HEX_LEN // 2
    ).hexdigest()


def state_table_name(full_name: str) -> str:
    """Deterministic, collision-safe, ≤63-char name for a model's STATE table
    in `z_bollhav_state`.

    Catalog/schema are de-vowelled (compressed, cosmetic) and the table name
    is kept readable; the budget favours the table so it survives. Identity
    rides in the digest, which is hashed over the FULL, unmodified name — so
    truncating the readable slug can never cause a collision.

        intelligence_raw_dan.vPatInfo
            -> ntllgnc_rw_dn_vpatinfo_de90fb57d928ba26
    """
    digest = _name_digest(full_name)
    parts = full_name.lower().replace("-", "_").split(".")
    table = parts[-1]
    context = "_".join(_devowel(p) for p in parts[:-1])[:_CONTEXT_CAP]
    table_budget = _SLUG_CAP - len(context) - 1 if context else _SLUG_CAP
    table = table[:table_budget]
    slug = f"{context}_{table}" if context else table
    return f"{slug}_{digest}"


# ── DDL ──────────────────────────────────────────────────────────────


# `since`/`until` are nullable: an interval row carries its window, while
# a monolithic (whole-table) or view row carries a NULL window — the unit of
# work is "the whole table" / "the view exists", not a time slice. `kind`
# discriminates the three (`interval` | `monolithic` | `view`). The table
# UNIQUE keeps one row per interval window; the partial index below keeps
# exactly one NULL-window row (the single monolithic / view row).
_STATE_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id              BIGSERIAL PRIMARY KEY,
    model_name      TEXT NOT NULL,
    run_id          UUID NOT NULL,
    since           TIMESTAMPTZ,
    until           TIMESTAMPTZ,
    status          TEXT NOT NULL,
    blocked_reason  TEXT,
    applied_at      TIMESTAMPTZ,
    kind            TEXT NOT NULL DEFAULT 'interval',
    UNIQUE (since, until)
)
"""

_STATE_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (status)
"""

# Exactly one NULL-window (monolithic / view) row per state table. Indexing
# the expression `(since IS NULL)` — always TRUE for matching rows — makes
# the uniqueness independent of `kind`, so a table can't hold two NULL-window
# rows. `ON CONFLICT ((since IS NULL)) WHERE since IS NULL` upserts it.
_STATE_SINGLETON_INDEX_DDL = """
CREATE UNIQUE INDEX IF NOT EXISTS {index} ON {schema}.{table} ((since IS NULL)) WHERE since IS NULL
"""

# One shared, central errors table — every model logs here, keyed by
# `full_name`. `since`/`until` are nullable: a monolithic / view failure
# logs a NULL window (it has no time slice).
_ERRORS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id              BIGSERIAL PRIMARY KEY,
    full_name       TEXT NOT NULL,
    run_id          UUID NOT NULL,
    since           TIMESTAMPTZ,
    until           TIMESTAMPTZ,
    error_type      TEXT NOT NULL,
    error_message   TEXT,
    traceback       TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

_ERRORS_MODEL_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (full_name)
"""

_ERRORS_TIME_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (created_at DESC)
"""


# Matches each `upstream 'name' (kind)` descriptor in a blocked_reason —
# `findall` returns every (name, kind) so the banner lists all the missing
# upstreams, not just the first. `kind` is optional for older reasons.
_BLOCK_UPSTREAM_RE = re.compile(r"upstream '([^']+)'(?: \(([^)]+)\))?")


def _overlay_covers(windows, kind: str, interval) -> bool:
    """The `DRY_STATE` cascade rule: True when an upstream's would-run
    `windows` cover this downstream `interval` for the given contract `kind`.
    A view/monolithic upstream (or a whole-table `None` window) covers any
    downstream window; an interval upstream needs a would-run window that
    contains the downstream's."""
    if not windows:
        return False
    if kind in ("view", "monolithic") or interval is None:
        return True
    for w in windows:
        if w is None:
            return True
        if w.since <= interval.since and w.until >= interval.until:
            return True
    return False


# ── cross-pipeline model library ─────────────────────────────────────
#
# The central registry of every bollhav model the state DB has ever
# seen — one shared table, keyed by full target name. Used during the
# state bootstrap to register each model and to resolve upstream
# satisfaction (a TABLE upstream is satisfied by an applied state row
# covering the window; a VIEW / library-only upstream by mere presence).
# The library is shared across pipelines run by different bollhav images,
# so its schema changes must be **additive** (see `_migrate_library_additively`).

LIBRARY_SCHEMA = "z_bollhav"
LIBRARY_TABLE = "library"
# Central errors table, shared by every model (keyed by full_name), in the
# same bollhav-owned schema as the library.
ERRORS_TABLE = "errors"

_LIBRARY_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    full_name      TEXT PRIMARY KEY,
    upstream       TEXT[] NOT NULL,
    sources        JSONB NOT NULL DEFAULT '[]'::jsonb,
    model_type     TEXT NOT NULL,
    state_schema   TEXT,
    state_table    TEXT,
    kind           TEXT NOT NULL DEFAULT 'interval',
    last_seen      TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


class LibraryEntry(NamedTuple):
    """A row read from the library. `kind` (`interval` | `monolithic` |
    `view`) drives how an upstream's satisfaction is checked. Every
    state-tracked model now has a state table, so `state_schema` /
    `state_table` are set for all of them; they are None only for
    library-only rows written by older bollhav images.

    `upstream` is managed-model edges (names; the edge's type is the upstream
    model's own `kind`, joinable). `sources` is external boundary inputs,
    each `{"name", "kind"}` — typed here because a source isn't a model and
    so has no row of its own to carry its kind. Together they're the model's
    typed lineage inputs."""

    upstream: list[str]
    model_type: str
    state_schema: str | None
    state_table: str | None
    kind: str
    sources: list[dict] = []


class PostgresState:
    """Postgres-backed state store for a single model.

    Construct with the model and the caller-owned state connection
    (opened in `main()`, threaded through the lifecycle hooks). The
    naming helpers (`_state_schema`, `_state_table`) work with the
    connection unset; every DB method requires it."""

    def __init__(self, model: "Model", conn: psycopg.Connection | None = None) -> None:
        self.model = model
        self.conn = conn

    # ── connection / naming ──────────────────────────────────────────

    def _require_conn(self) -> psycopg.Connection:
        """Return the injected state connection. `PostgresState` doesn't
        open its own — the caller owns it (opened in `main()`, threaded
        through the lifecycle hooks). Raises if none was passed."""
        if self.conn is None:
            raise ValueError(
                "a state connection is required — construct "
                "PostgresState(model, conn=<state_conn>). PostgresState "
                "does not self-connect."
            )
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
        """Deterministic state-table name (`state_table_name(full_name)`).
        Computed from the model's full name, so any process recomputes it
        without a lookup."""
        return state_table_name(self.model.target.full_name)

    # ── DDL ──────────────────────────────────────────────────────────

    def ensure_tables(self) -> None:
        """Create the central state schema (`z_bollhav_state`), the model's
        state table, and its indexes if absent. Idempotent. (Errors are NOT
        per-model — the shared `z_bollhav.errors` table is created once by
        `ensure_library`.) Index names are keyed off the name digest (short,
        unique) rather than the table name, which can be up to 61 chars and
        would push an index name past 63.

        After creating, asserts the state table isn't already owned by a
        different model (a hash collision) via its `model_name` column."""
        state_schema = self._state_schema()
        state_table = self._state_table()
        digest = _name_digest(self.model.target.full_name)
        state_index = f"{digest}_status_idx"
        state_singleton_index = f"{digest}_singleton_idx"

        conn = self._require_conn()
        with conn.transaction():
            conn.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(
                    schema=sql.Identifier(state_schema),
                )
            )
            conn.execute(
                sql.SQL(_STATE_DDL).format(
                    schema=sql.Identifier(state_schema),
                    table=sql.Identifier(state_table),
                )
            )
            # Bring older state tables up to the current shape (nullable
            # window + `kind` + `model_name`) before building the partial
            # singleton index, which needs those columns to exist.
            self._migrate_state_additively(state_schema, state_table)
            conn.execute(
                sql.SQL(_STATE_INDEX_DDL).format(
                    index=sql.Identifier(state_index),
                    schema=sql.Identifier(state_schema),
                    table=sql.Identifier(state_table),
                )
            )
            conn.execute(
                sql.SQL(_STATE_SINGLETON_INDEX_DDL).format(
                    index=sql.Identifier(state_singleton_index),
                    schema=sql.Identifier(state_schema),
                    table=sql.Identifier(state_table),
                )
            )
            self._assert_no_hash_collision(state_schema, state_table)
        logger.debug(
            "state: ensured state table for %s (%s.%s)",
            self.model.target.full_name,
            state_schema,
            state_table,
        )

    def _assert_no_hash_collision(self, schema: str, state_table: str) -> None:
        """Guard the ~1e-12 case where two different models hash to the same
        state table. The table self-identifies via `model_name`; if it
        already holds rows for a *different* model, raise loudly instead of
        silently sharing state."""
        conn = self._require_conn()
        row = conn.execute(
            sql.SQL(
                "SELECT model_name FROM {schema}.{table} WHERE model_name <> %s LIMIT 1"
            ).format(
                schema=sql.Identifier(schema),
                table=sql.Identifier(state_table),
            ),
            [self.model.target.full_name],
        ).fetchone()
        if row is not None:
            raise ValueError(
                f"state hash collision: {schema}.{state_table} already holds "
                f"state for {row[0]!r}, not {self.model.target.full_name!r}. "
                f"Rename one model or widen the digest in state_table_name."
            )

    def _migrate_state_additively(
        self,
        state_schema: str,
        state_table: str,
    ) -> None:
        """Bring an older state table up to the current shape without breaking
        concurrent old-image writers. Runs inside the caller's transaction.
        Additive only (per the shared-table rule): add the `kind` and
        `model_name` columns, and relax `since`/`until` to nullable so
        monolithic / view rows carry a NULL window. Each step checks
        information_schema first, so it's idempotent."""
        conn = self._require_conn()

        has_kind = conn.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "  AND column_name = 'kind' LIMIT 1",
            [state_schema, state_table],
        ).fetchone()
        if not has_kind:
            conn.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} "
                    "ADD COLUMN kind TEXT NOT NULL DEFAULT 'interval'"
                ).format(
                    schema=sql.Identifier(state_schema),
                    table=sql.Identifier(state_table),
                )
            )
            logger.info(
                "state: migrated %s.%s — added kind column (default "
                "'interval' so older images keep writing interval rows)",
                state_schema,
                state_table,
            )

        # `model_name` was added so the table self-identifies. On a
        # pre-existing table we add it nullable (existing rows have no value);
        # fresh tables get it NOT NULL from the DDL.
        conn.execute(
            sql.SQL(
                "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS model_name TEXT"
            ).format(
                schema=sql.Identifier(state_schema),
                table=sql.Identifier(state_table),
            )
        )

        for col in ("since", "until"):
            is_nullable = conn.execute(
                "SELECT is_nullable FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "  AND column_name = %s LIMIT 1",
                [state_schema, state_table, col],
            ).fetchone()
            if is_nullable and is_nullable[0] == "NO":
                conn.execute(
                    sql.SQL(
                        "ALTER TABLE {schema}.{table} ALTER COLUMN {col} DROP NOT NULL"
                    ).format(
                        schema=sql.Identifier(state_schema),
                        table=sql.Identifier(state_table),
                        col=sql.Identifier(col),
                    )
                )
                logger.info(
                    "state: migrated %s.%s — relaxed NOT NULL on %s "
                    "(monolithic / view rows carry a NULL window)",
                    state_schema,
                    state_table,
                    col,
                )

    # ── pre-fill ─────────────────────────────────────────────────────

    def insert_intervals(self, *, run_id: UUID, intervals: tuple) -> None:
        """Insert one row per interval. Each item in `intervals` is either:
          * a bare `TZInterval` (treated as pending, no blocked reason), OR
          * a 3-tuple `(TZInterval, status, blocked_reason)` where status
            is one of `'pending'` or `'blocked'` and `blocked_reason` is
            a string when status is `'blocked'`, else `None`.

        DISCOVER mode:
          * Applied rows are preserved untouched.
          * Pending/blocked rows are re-evaluated against the new status
            — so a blocked row whose upstreams now satisfy flips to
            pending, and vice versa.

        BULLDOZER mode:
          * Every row is reset to the new computed status, regardless of
            prior value (applied_at cleared too)."""
        model = self.model
        if model.state is None:
            raise ValueError(
                "prefill_intervals requires a state-enabled model "
                f"({model.target.full_name!r} has no `state`)"
            )

        if not intervals:
            return

        rows = [self._normalize_prefill_row(item) for item in intervals]

        schema = self._state_schema()
        table = self._state_table()

        from bollhav.model.state import StateMode

        on_conflict = ""
        if model.state.mode is StateMode.DISCOVER:
            on_conflict = sql.SQL(
                "ON CONFLICT (since, until) DO UPDATE SET "
                "status = CASE WHEN {schema}.{table}.status = 'applied' "
                "             THEN 'applied' ELSE EXCLUDED.status END, "
                "blocked_reason = CASE WHEN {schema}.{table}.status = 'applied' "
                "                      THEN NULL ELSE EXCLUDED.blocked_reason END, "
                "run_id = CASE WHEN {schema}.{table}.status = 'applied' "
                "              THEN {schema}.{table}.run_id ELSE EXCLUDED.run_id END"
            ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))

        if model.state.mode is StateMode.BULLDOZER:
            on_conflict = sql.SQL(
                "ON CONFLICT (since, until) DO UPDATE SET "
                "status = EXCLUDED.status, "
                "blocked_reason = EXCLUDED.blocked_reason, "
                "applied_at = NULL, "
                "run_id = EXCLUDED.run_id"
            )

        insert = sql.SQL(
            "INSERT INTO {schema}.{table} "
            "(model_name, run_id, since, until, status, blocked_reason) "
            "VALUES (%s, %s, %s, %s, %s, %s) {on_conflict}"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            on_conflict=on_conflict,
        )

        conn = self._require_conn()
        with conn.transaction():
            for interval, status, reason in rows:
                conn.execute(
                    insert,
                    [
                        model.target.full_name,
                        str(run_id),
                        interval.since,
                        interval.until,
                        status,
                        reason,
                    ],
                )

        pending = sum(1 for _, s, _ in rows if s == "pending")
        blocked = sum(1 for _, s, _ in rows if s == "blocked")
        logger.debug(
            "state: prefilled %d intervals for %s (mode=%s, pending=%d, blocked=%d)",
            len(rows),
            model.target.full_name,
            model.state.mode.value,
            pending,
            blocked,
        )

    def insert_singleton(self, *, run_id: UUID) -> None:
        """Prefill the single NULL-window row for a monolithic (whole-table)
        or view model — the counterpart to `insert_intervals` for the
        non-interval kinds. The row carries `kind = model.kind.value` and a NULL
        `since`/`until`; the partial unique index keeps it unique.

        Same DISCOVER / BULLDOZER semantics as interval prefill: DISCOVER
        preserves an already-applied row (so a loaded table / existing view
        isn't re-run), BULLDOZER resets it to pending."""
        model = self.model
        if model.state is None:
            raise ValueError(
                "insert_singleton requires a state-enabled model "
                f"({model.target.full_name!r} has no `state`)"
            )

        schema = self._state_schema()
        table = self._state_table()

        from bollhav.model.state import StateMode

        on_conflict = sql.SQL("")
        if model.state.mode is StateMode.DISCOVER:
            on_conflict = sql.SQL(
                "ON CONFLICT ((since IS NULL)) WHERE since IS NULL DO UPDATE SET "
                "status = CASE WHEN {schema}.{table}.status = 'applied' "
                "             THEN 'applied' ELSE EXCLUDED.status END, "
                "run_id = CASE WHEN {schema}.{table}.status = 'applied' "
                "              THEN {schema}.{table}.run_id ELSE EXCLUDED.run_id END"
            ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))
        elif model.state.mode is StateMode.BULLDOZER:
            on_conflict = sql.SQL(
                "ON CONFLICT ((since IS NULL)) WHERE since IS NULL DO UPDATE SET "
                "status = EXCLUDED.status, applied_at = NULL, run_id = EXCLUDED.run_id"
            )

        insert = sql.SQL(
            "INSERT INTO {schema}.{table} "
            "(model_name, run_id, since, until, status, blocked_reason, kind) "
            "VALUES (%s, %s, NULL, NULL, 'pending', NULL, %s) {on_conflict}"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            on_conflict=on_conflict,
        )

        conn = self._require_conn()
        with conn.transaction():
            conn.execute(
                insert, [model.target.full_name, str(run_id), model.kind.value]
            )
        logger.debug(
            "state: prefilled %s singleton row for %s (mode=%s)",
            model.kind.value,
            model.target.full_name,
            model.state.mode.value,
        )

    @staticmethod
    def _normalize_prefill_row(item) -> tuple:
        """Accept either bare TZInterval (→ pending) or 3-tuple."""
        if isinstance(item, tuple) and len(item) == 3:
            interval, status, reason = item
            if status not in ("pending", "blocked"):
                raise ValueError(
                    f"prefill status must be 'pending' or 'blocked', got {status!r}"
                )
            if status == "blocked" and not reason:
                raise ValueError("blocked rows require a non-empty blocked_reason")
            return (interval, status, reason if status == "blocked" else None)
        # Bare TZInterval — backward-compat: treat as pending.
        return (item, "pending", None)

    # ── decorator hooks ──────────────────────────────────────────────

    def read_pending(self) -> list:
        """Return every (since, until) row with status='pending' as a list
        of `TZInterval`, ordered oldest first. Kept for backward
        compatibility — `get_actionable_intervals` is now the default for
        `@load_models` because the decorator re-evaluates blocked rows at
        runtime."""
        from bollhav.model.intervals import TZInterval

        schema = self._state_schema()
        table = self._state_table()
        query = sql.SQL(
            "SELECT since, until FROM {schema}.{table} "
            "WHERE status = 'pending' "
            "ORDER BY since, until"
        ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))

        conn = self._require_conn()
        rows = conn.execute(query).fetchall()
        logger.debug(
            "state: read %d pending intervals for %s",
            len(rows),
            self.model.target.full_name,
        )
        return [TZInterval(since=since, until=until) for since, until in rows]

    def get_actionable_intervals(self) -> tuple:
        """Return every row whose status is NOT `'applied'` — i.e.
        pending, blocked, running, or error — ordered oldest first.

        Used by `@load_models` to populate `model.intervals`. The
        decorator re-evaluates each row at run time: blocked rows whose
        upstream now satisfies become processable; pending rows whose
        upstream regressed get marked blocked. So `blocked` is transient
        — a snapshot of what's currently waiting, not a terminal state.

        A monolithic / view model has a single NULL-window row: this yields
        `(None,)` when it's actionable (so the user's loop runs once with
        `interval=None`) and `()` when it's already applied (loop skips)."""
        from bollhav.model.intervals import TZInterval

        schema = self._state_schema()
        table = self._state_table()
        query = sql.SQL(
            "SELECT since, until FROM {schema}.{table} "
            "WHERE status <> 'applied' "
            "ORDER BY since, until"
        ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))

        conn = self._require_conn()
        rows = conn.execute(query).fetchall()
        logger.debug(
            "state: read %d actionable intervals for %s",
            len(rows),
            self.model.target.full_name,
        )
        actionable = []
        for since, until in rows:
            if since is None or until is None:
                actionable.append(None)
            else:
                actionable.append(TZInterval(since=since, until=until))
        return tuple(actionable)

    def read_status_summary(self) -> dict:
        """One-shot summary of the model's state table, used by the
        state banner.

        Returns:
            {
                "counts": {"pending": int, "applied": int, "blocked": int},
                "blocked_groups": {(code, upstream | None, kind | None): count}
            }

        Blocked rows are bucketed by `(code, upstream, kind)` so the banner
        can list every missing upstream — e.g. a row blocked by two
        upstreams contributes to two buckets — and say
        "STATE_002 × 3 · upstream 'warehouse.orders' (interval)". Code is
        the substring before the first `:`; every `upstream 'X' (kind)`
        descriptor in the reason is extracted (not just the first). A
        blocked row with no parseable upstream falls into `(code, None,
        None)` so it's still counted."""
        schema = self._state_schema()
        table = self._state_table()
        query = sql.SQL("SELECT status, blocked_reason FROM {schema}.{table}").format(
            schema=sql.Identifier(schema), table=sql.Identifier(table)
        )

        counts: dict[str, int] = {
            "pending": 0,
            "running": 0,
            "applied": 0,
            "blocked": 0,
            "error": 0,
        }
        blocked_groups: dict[tuple[str, str | None, str | None], int] = {}

        def bump(key: tuple[str, str | None, str | None]) -> None:
            blocked_groups[key] = blocked_groups.get(key, 0) + 1

        conn = self._require_conn()
        for status, reason in conn.execute(query).fetchall():
            counts[status] = counts.get(status, 0) + 1
            if status == "blocked" and reason:
                code = reason.split(":", 1)[0].strip() or "?"
                matches = _BLOCK_UPSTREAM_RE.findall(reason)
                if matches:
                    for name, kind in matches:
                        bump((code, name, kind or None))
                else:
                    bump((code, None, None))

        return {"counts": counts, "blocked_groups": blocked_groups}

    @staticmethod
    def _window_match(interval: "TZInterval | None"):
        """SQL predicate + params identifying one state row by its window.
        `interval` is a TZInterval, or None for a monolithic / view row whose
        window is NULL — matched on `IS NULL` rather than `=` (which never
        matches NULL)."""
        if interval is None:
            return sql.SQL("since IS NULL AND until IS NULL"), []
        return sql.SQL("since = %s AND until = %s"), [interval.since, interval.until]

    def is_applied(self, interval: "TZInterval | None") -> bool:
        schema = self._state_schema()
        table = self._state_table()
        match, params = self._window_match(interval)
        query = sql.SQL(
            "SELECT 1 FROM {schema}.{table} "
            "WHERE {match} AND status = 'applied' LIMIT 1"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            match=match,
        )

        conn = self._require_conn()
        row = conn.execute(query, params).fetchone()
        return row is not None

    def record_failure(
        self,
        *,
        run_id: UUID,
        interval: "TZInterval | None",
        error_type: str,
        error_message: str,
        traceback_text: str | None,
        update_state: bool = True,
    ) -> None:
        """Atomically log a failure to the errors table and (optionally)
        flip the state row to `'error'`.

        `update_state=False` is for the rare case where the staging flush
        already set state to `'applied'` (data is in target) but user code
        after the merge raised. We still log the error for debugging, but
        the state stays `applied` — we shouldn't downgrade a successful
        write."""
        state_schema = self._state_schema()
        state_table = self._state_table()
        since = interval.since if interval is not None else None
        until = interval.until if interval is not None else None

        # Errors go to the one shared central table, keyed by full_name.
        insert_error = sql.SQL(
            "INSERT INTO {schema}.{table} "
            "(full_name, run_id, since, until, error_type, error_message, traceback) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        ).format(
            schema=sql.Identifier(self._library_schema()),
            table=sql.Identifier(ERRORS_TABLE),
        )
        match, match_params = self._window_match(interval)
        update_state_sql = sql.SQL(
            "UPDATE {schema}.{table} SET status = 'error', run_id = %s WHERE {match}"
        ).format(
            schema=sql.Identifier(state_schema),
            table=sql.Identifier(state_table),
            match=match,
        )

        conn = self._require_conn()
        with conn.transaction():
            conn.execute(
                insert_error,
                [
                    self.model.target.full_name,
                    str(run_id),
                    since,
                    until,
                    error_type,
                    error_message,
                    traceback_text,
                ],
            )
            if update_state:
                conn.execute(update_state_sql, [str(run_id), *match_params])
        logger.debug(
            "state: recorded error for %s (%s) %s: %s (state_updated=%s)",
            self.model.target.full_name,
            interval,
            error_type,
            error_message,
            update_state,
        )

    def mark_running(self, *, run_id: UUID, interval: "TZInterval | None") -> None:
        """Flip the state row to `'running'` — set by `@execute_lifecycle`
        just before invoking the user's execute. On success the same
        row flips to `'applied'`; on exception, to `'error'`. A row left
        as `'running'` after a process crash is treated like `'pending'`
        by the next DISCOVER-mode pre-fill (auto-recovered)."""
        schema = self._state_schema()
        table = self._state_table()
        match, match_params = self._window_match(interval)
        update = sql.SQL(
            "UPDATE {schema}.{table} SET status = 'running', run_id = %s WHERE {match}"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            match=match,
        )

        conn = self._require_conn()
        with conn.transaction():
            conn.execute(update, [str(run_id), *match_params])
        logger.debug(
            "state: marked running %s for %s",
            interval,
            self.model.target.full_name,
        )

    def mark_blocked(
        self, *, run_id: UUID, interval: "TZInterval | None", reason: str
    ) -> None:
        """Flip the state row to `'blocked'` with the given reason. Set
        by `@execute_lifecycle` when the live upstream check fails at run
        time. Re-evaluated on the next DISCOVER-mode bootstrap or run."""
        schema = self._state_schema()
        table = self._state_table()
        match, match_params = self._window_match(interval)
        update = sql.SQL(
            "UPDATE {schema}.{table} "
            "SET status = 'blocked', blocked_reason = %s, run_id = %s "
            "WHERE {match}"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            match=match,
        )

        conn = self._require_conn()
        with conn.transaction():
            conn.execute(update, [reason, str(run_id), *match_params])
        logger.debug(
            "state: marked blocked %s for %s — %s",
            interval,
            self.model.target.full_name,
            reason,
        )

    def mark_applied(self, *, run_id: UUID, interval: "TZInterval | None") -> None:
        schema = self._state_schema()
        table = self._state_table()
        match, match_params = self._window_match(interval)
        update = sql.SQL(
            "UPDATE {schema}.{table} "
            "SET status = 'applied', applied_at = now(), run_id = %s "
            "WHERE {match}"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            match=match,
        )

        conn = self._require_conn()
        with conn.transaction():
            conn.execute(update, [str(run_id), *match_params])
        logger.debug(
            "state: marked applied %s for %s",
            interval,
            self.model.target.full_name,
        )

    # ── live upstream check (used by @execute_lifecycle when polling) ─

    def is_upstream_satisfied_live(
        self, interval: "TZInterval | None"
    ) -> "UpstreamCheck":
        """Live upstream-satisfaction check for one unit of work.

        Checks *every* declared upstream (no short-circuit) and returns an
        `UpstreamCheck` verdict carrying one block reason per
        unsatisfied upstream. For each upstream `Contract`, look it up in
        the library and check satisfaction by the contract's `kind`
        (interval → window cover; view / monolithic → existence row
        applied). A bare-string upstream has no declared kind, so it falls
        back to the upstream's own registered `kind`. `interval` is the
        downstream's window, or None for a monolithic / view downstream
        (whole-table / existence work).

        A declared `Contract` whose upstream is not registered raises — an
        explicit demand on something that has never run is a hard error. A
        bare-string upstream that isn't registered is documentation and
        does not block.

        Each blocker is a short `upstream 'name' (kind)` descriptor;
        `UpstreamCheck.reason` composes them into the single concise
        `blocked_reason` the row stores, and `read_status_summary` parses
        each back out so the banner can list every missing upstream."""
        from bollhav.model.upstream import UpstreamCheck

        conn = self._require_conn()
        blockers: list[str] = []
        for src in self.model.gated_upstreams:
            name = src.name
            kind = src.contract.kind  # gated ⇒ contract is not None
            # Look the upstream up under THIS env's identity + library: a
            # suffixed run resolves against its own env, never prod's.
            match = PostgresState.lookup_model(
                conn, self._suffix_upstream_name(name), self._library_schema()
            )
            if match is None:
                # A gated upstream (a Source with a contract) is a hard demand:
                # an unregistered upstream is a real error (a typo, or the
                # upstream was never deployed / run). (An ungated source isn't
                # checked here at all — it's never iterated.)
                raise ValueError(
                    f"upstream contract {name!r} ({kind}) on "
                    f"{self.model.target.full_name!r} is not registered in "
                    f"the library — it has never run. A gated upstream demands "
                    f"the upstream exists; fix the name or run the upstream "
                    f"first. (An ungated source would not block.)"
                )
            if not PostgresState.is_satisfied(
                conn, entry=match, interval=interval, kind=kind
            ):
                blockers.append(f"upstream {name!r} ({kind})")
        return UpstreamCheck(blockers=tuple(blockers))

    def dry_state_classify(
        self, interval: "TZInterval | None", assume_applied: dict | None = None
    ) -> tuple[str, list[str]]:
        """Classify one actionable interval for `DRY_STATE` into three outcomes,
        accounting for the cascade. `assume_applied` is
        `{upstream_full_name: [windows that would run this pass]}` — an upstream
        that hasn't run yet but WOULD run earlier in the same pass counts as
        satisfied.

        Returns `(status, upstreams)`:

          * `("run", [])`           — runnable now (every gate already applied)
          * `("after", [u, …])`     — gated only on upstreams that would run
                                      this pass → would run *after* them
          * `("blocked", [u, …])`   — gated on upstreams that would NOT run

        Unlike `is_upstream_satisfied_live`, an unregistered upstream is reported
        as blocked rather than raised — a preview shouldn't crash."""
        conn = self._require_conn()
        after: list[str] = []
        blocked: list[str] = []
        for src in self.model.gated_upstreams:
            name = src.name
            kind = src.contract.kind
            lookup = self._suffix_upstream_name(name)
            if assume_applied and _overlay_covers(
                assume_applied.get(lookup), kind, interval
            ):
                after.append(f"{name} ({kind})")
                continue
            match = PostgresState.lookup_model(conn, lookup, self._library_schema())
            if match is not None and PostgresState.is_satisfied(
                conn, entry=match, interval=interval, kind=kind
            ):
                continue  # already applied
            blocked.append(f"{name} ({kind})")
        if blocked:
            return ("blocked", blocked)
        if after:
            return ("after", after)
        return ("run", [])

    # ── advisory locks ───────────────────────────────────────────────

    def _interval_lock_key(self, interval: "TZInterval | None") -> str:
        """Hash key for an interval-scoped lock: identifies the specific
        `(model, since, until)` triple. Used by `@execute_lifecycle` so two
        workers can race on the same model but different intervals
        without conflict. A monolithic / view row has a NULL window, so its
        key collapses to a single per-model `…|singleton` slot."""
        if interval is None:
            return f"{self.model.target.full_name}|singleton"
        return (
            f"{self.model.target.full_name}"
            f"|{interval.since.isoformat()}|{interval.until.isoformat()}"
        )

    def try_acquire_interval_lock(self, interval: "TZInterval | None") -> bool:
        """Try to take a session-scoped advisory lock for ONE interval.
        Returns True if acquired, False if another session already holds
        it. Released by `release_interval_lock` or automatically when
        the connection closes."""
        conn = self._require_conn()
        row = conn.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s))",
            [self._interval_lock_key(interval)],
        ).fetchone()
        return bool(row and row[0])

    def release_interval_lock(self, interval: "TZInterval | None") -> None:
        conn = self._require_conn()
        conn.execute(
            "SELECT pg_advisory_unlock(hashtext(%s))",
            [self._interval_lock_key(interval)],
        )

    # Optional model-wide lock — kept for power users who want exclusive
    # access to a whole model run. Not used by `@execute_lifecycle` (which
    # uses per-interval locks above).

    def try_acquire_lock(self) -> bool:
        """Try to take a session-scoped advisory lock keyed by the model's
        `full_name`. Returns True if acquired, False otherwise. Released
        by `release_lock` or automatically when the connection closes."""
        conn = self._require_conn()
        row = conn.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s))",
            [self.model.target.full_name],
        ).fetchone()
        return bool(row and row[0])

    def release_lock(self) -> None:
        conn = self._require_conn()
        conn.execute(
            "SELECT pg_advisory_unlock(hashtext(%s))",
            [self.model.target.full_name],
        )

    def acquire_model_lock(self) -> bool:
        """Take the exclusive model-wide lock unless the model opts into
        concurrent runs (`State.allow_concurrent_runs`). Returns True if
        the lock was acquired — the caller must `release_lock()` it when
        the run ends — or False when concurrent runs are allowed and no
        lock was taken. Raises `ModelLockedError` if another run already
        holds the lock. Only call on a state-activated model — the
        lifecycle hook guards the call on `model.stateful`."""
        assert self.model.state is not None, (
            "acquire_model_lock requires a state-activated model (model.state is None)"
        )
        if self.model.state.allow_concurrent_runs:
            return False
        if not self.try_acquire_lock():
            from bollhav.model.state import ModelLockedError

            raise ModelLockedError(
                f"another pipeline holds the lock on "
                f"{self.model.target.full_name!r} — concurrent runs of "
                f"the same model are not allowed"
            )
        return True

    # ── cross-pipeline library ────────────────────────────────────────

    def ensure_library(self) -> None:
        """Create the shared library schema + table if absent. Idempotent.

        The library is shared across many pipelines run by different
        bollhav images — possibly different versions of bollhav at the
        same time. Schema changes therefore have to be **additive**:
        new columns must have a safe default so old images can keep
        inserting without filling them in, and `NOT NULL` may only be
        relaxed, never tightened. Drops are forbidden — they would
        brick concurrent writers. See `_migrate_library_additively`."""
        conn = self._require_conn()
        with conn.transaction():
            conn.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(
                    schema=sql.Identifier(self._library_schema()),
                )
            )
            conn.execute(
                sql.SQL(_LIBRARY_DDL).format(
                    schema=sql.Identifier(self._library_schema()),
                    table=sql.Identifier(LIBRARY_TABLE),
                )
            )
            self._migrate_library_additively()
            # The one shared, central errors table lives in the same schema.
            conn.execute(
                sql.SQL(_ERRORS_DDL).format(
                    schema=sql.Identifier(self._library_schema()),
                    table=sql.Identifier(ERRORS_TABLE),
                )
            )
            conn.execute(
                sql.SQL(_ERRORS_MODEL_INDEX_DDL).format(
                    index=sql.Identifier(f"{ERRORS_TABLE}_full_name_idx"),
                    schema=sql.Identifier(self._library_schema()),
                    table=sql.Identifier(ERRORS_TABLE),
                )
            )
            conn.execute(
                sql.SQL(_ERRORS_TIME_INDEX_DDL).format(
                    index=sql.Identifier(f"{ERRORS_TABLE}_created_at_idx"),
                    schema=sql.Identifier(self._library_schema()),
                    table=sql.Identifier(ERRORS_TABLE),
                )
            )
        logger.debug(
            "library: ensured %s.%s + %s.%s",
            self._library_schema(),
            LIBRARY_TABLE,
            self._library_schema(),
            ERRORS_TABLE,
        )

    def _migrate_library_additively(self) -> None:
        """Apply additive ALTERs to bring an older `library` table up to
        the current shape without breaking concurrent old-image writers.
        Runs inside the caller's transaction. Each step checks the
        information_schema before issuing the ALTER, so the function is
        idempotent and cheap to call on every bootstrap."""
        conn = self._require_conn()
        # `sources` (typed external-input lineage) — additive, with a safe
        # default so older images keep registering without filling it in.
        conn.execute(
            sql.SQL(
                "ALTER TABLE {schema}.{table} "
                "ADD COLUMN IF NOT EXISTS sources JSONB NOT NULL DEFAULT '[]'::jsonb"
            ).format(
                schema=sql.Identifier(self._library_schema()),
                table=sql.Identifier(LIBRARY_TABLE),
            )
        )
        has_model_type = conn.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "  AND column_name = 'model_type' LIMIT 1",
            [self._library_schema(), LIBRARY_TABLE],
        ).fetchone()
        if not has_model_type:
            conn.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} "
                    "ADD COLUMN model_type TEXT NOT NULL DEFAULT 'TABLE'"
                ).format(
                    schema=sql.Identifier(self._library_schema()),
                    table=sql.Identifier(LIBRARY_TABLE),
                )
            )
            logger.info(
                "library: migrated %s.%s — added model_type column "
                "(default 'TABLE' so older images can keep writing)",
                self._library_schema(),
                LIBRARY_TABLE,
            )

        has_kind = conn.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "  AND column_name = 'kind' LIMIT 1",
            [self._library_schema(), LIBRARY_TABLE],
        ).fetchone()
        if not has_kind:
            conn.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} "
                    "ADD COLUMN kind TEXT NOT NULL DEFAULT 'interval'"
                ).format(
                    schema=sql.Identifier(self._library_schema()),
                    table=sql.Identifier(LIBRARY_TABLE),
                )
            )
            logger.info(
                "library: migrated %s.%s — added kind column (default "
                "'interval' so older images keep registering interval models)",
                self._library_schema(),
                LIBRARY_TABLE,
            )

        for col in ("state_schema", "state_table"):
            is_nullable = conn.execute(
                "SELECT is_nullable FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "  AND column_name = %s LIMIT 1",
                [self._library_schema(), LIBRARY_TABLE, col],
            ).fetchone()
            if is_nullable and is_nullable[0] == "NO":
                conn.execute(
                    sql.SQL(
                        "ALTER TABLE {schema}.{table} ALTER COLUMN {col} DROP NOT NULL"
                    ).format(
                        schema=sql.Identifier(self._library_schema()),
                        table=sql.Identifier(LIBRARY_TABLE),
                        col=sql.Identifier(col),
                    )
                )
                logger.info(
                    "library: migrated %s.%s — relaxed NOT NULL on %s "
                    "(view / library-only rows store NULL here)",
                    self._library_schema(),
                    LIBRARY_TABLE,
                    col,
                )

    def register_model(self) -> None:
        """Upsert this model's library row. Overwrites every field
        except `full_name` (the PK) and `last_seen` (always `now()`).
        So renaming an upstream, moving the state table, or flipping a
        TABLE to a VIEW all propagate the next time `register_model` runs.

        Every state-tracked model — interval, monolithic, or view — now has
        a state table, so all of them write `state_schema` / `state_table`
        and a `kind`. `kind` drives how an upstream's satisfaction is
        resolved (`is_satisfied`): interval covers a window, monolithic /
        view check the single existence row."""
        model = self.model
        conn = self._require_conn()
        has_state_table = model.state is not None
        state_schema = self._state_schema() if has_state_table else None
        state_table = self._state_table() if has_state_table else None
        model_type = "VIEW" if model.is_view else "TABLE"
        upsert = sql.SQL(
            "INSERT INTO {schema}.{table} "
            "(full_name, upstream, model_type, state_schema, state_table, kind, "
            "sources, last_seen) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, now()) "
            "ON CONFLICT (full_name) DO UPDATE SET "
            "upstream = EXCLUDED.upstream, "
            "model_type = EXCLUDED.model_type, "
            "state_schema = EXCLUDED.state_schema, "
            "state_table = EXCLUDED.state_table, "
            "kind = EXCLUDED.kind, "
            "sources = EXCLUDED.sources, "
            "last_seen = EXCLUDED.last_seen"
        ).format(
            schema=sql.Identifier(self._library_schema()),
            table=sql.Identifier(LIBRARY_TABLE),
        )
        with conn.transaction():
            conn.execute(
                upsert,
                [
                    model.target.full_name,
                    model.upstream_names,
                    model_type,
                    state_schema,
                    state_table,
                    model.kind.value,
                    Jsonb(model.source_specs),
                ],
            )
        logger.debug(
            "library: registered %s (model_type=%s, kind=%s, upstream=%s, sources=%s)",
            model.target.full_name,
            model_type,
            model.kind.value,
            model.upstream_names,
            model.source_names,
        )

    @staticmethod
    def lookup_model(
        conn: psycopg.Connection,
        full_name: str,
        library_schema: str = LIBRARY_SCHEMA,
    ) -> "LibraryEntry | None":
        """Return the `LibraryEntry` for a model, or None if unregistered.

        Registry-level (not scoped to one model), so it's a static method
        taking the connection — the caller looks up arbitrary upstream
        names, not `self.model`. `library_schema` selects the environment's
        library (the gating caller passes `self._library_schema()`); defaults
        to the prod `z_bollhav`."""
        row = conn.execute(
            sql.SQL(
                "SELECT upstream, model_type, state_schema, state_table, kind, sources "
                "FROM {schema}.{table} WHERE full_name = %s"
            ).format(
                schema=sql.Identifier(library_schema),
                table=sql.Identifier(LIBRARY_TABLE),
            ),
            [full_name],
        ).fetchone()
        if row is None:
            return None
        upstream, model_type, state_schema, state_table, kind, sources = row
        return LibraryEntry(
            upstream=list(upstream),
            model_type=model_type,
            state_schema=state_schema,
            state_table=state_table,
            kind=kind,
            sources=list(sources) if sources else [],
        )

    @staticmethod
    def is_satisfied(
        conn: psycopg.Connection,
        *,
        entry: "LibraryEntry",
        interval: "TZInterval | None",
        kind: str | None = None,
    ) -> bool:
        """Is the upstream satisfied for `interval` (the downstream's
        window, or None for whole-table / existence work)?

        The check is keyed by `kind`. When the downstream declared a
        `Contract`, `kind` is the contract's kind (the downstream's
        explicit expectation); otherwise it falls back to the upstream's
        own registered `entry.kind`. Either way:

        * library-only rows (`state_schema` / `state_table` NULL — older
          images, or models registered without state tracking): presence
          in the library is the proof. Always satisfied.
        * `kind == 'interval'`: look for an `applied` row whose window
          matches or fully encapsulates `interval`. A daily-cadence
          upstream thus covers an hourly downstream without coordination.
        * `kind == 'view'` or `'monolithic'`: the upstream has a single
          NULL-window existence row — satisfied iff that row is `applied`
          (the view exists / the whole table has been loaded). The
          downstream window is irrelevant.

        If the upstream's state table doesn't exist yet (registered but
        never bootstrapped), returns False."""
        kind = kind or entry.kind

        if entry.state_schema is None or entry.state_table is None:
            return True

        exists = conn.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname=%s AND tablename=%s LIMIT 1",
            [entry.state_schema, entry.state_table],
        ).fetchone()
        if not exists:
            return False

        if kind in ("view", "monolithic"):
            query = sql.SQL(
                "SELECT 1 FROM {schema}.{table} "
                "WHERE status = 'applied' AND since IS NULL AND until IS NULL "
                "LIMIT 1"
            ).format(
                schema=sql.Identifier(entry.state_schema),
                table=sql.Identifier(entry.state_table),
            )
            return conn.execute(query).fetchone() is not None

        since = interval.since if interval is not None else None
        until = interval.until if interval is not None else None
        query = sql.SQL(
            "SELECT 1 FROM {schema}.{table} "
            "WHERE status = 'applied' "
            "  AND since <= %s AND until >= %s "
            "LIMIT 1"
        ).format(
            schema=sql.Identifier(entry.state_schema),
            table=sql.Identifier(entry.state_table),
        )
        row = conn.execute(query, [since, until]).fetchone()
        return row is not None

    def clear_state(self) -> None:
        """Wipe this model's state with no trace — drop its state table and
        delete its library registration + error rows (all keyed by `full_name`,
        in this model's env schema `z_bollhav_<suffix>`). Unlike BULLDOZER
        (which resets *existing* interval rows to `pending`, keeping the table,
        history and registration), the next run's bootstrap rebuilds everything
        from nothing and re-discovers every interval. Does NOT touch the model's
        data — only its state.

        Refuses unless the model carries a schema suffix: clearing prod state
        (`z_bollhav`) is intentionally not offered — the suffix marks the
        ephemeral environment this is meant for. Do it by hand if you must."""
        if not self.model.target.schema_suffix:
            raise ValueError(
                f"clear_state refuses to run on {self.model.target.full_name!r}: "
                f"it has no schema suffix, so its state lives in prod "
                f"({LIBRARY_SCHEMA}). Clearing prod state isn't offered — set "
                f"SCHEMA_SUFFIX for an ephemeral environment, or delete the rows "
                f"by hand if you truly must."
            )
        conn = self._require_conn()
        schema = self._library_schema()
        full_name = self.model.target.full_name
        with conn.transaction():
            conn.execute(
                sql.SQL("DROP TABLE IF EXISTS {schema}.{table}").format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(self._state_table()),
                )
            )
            # library + errors are shared tables keyed by full_name — delete
            # only this model's rows, and only once the table actually exists.
            for table in (LIBRARY_TABLE, ERRORS_TABLE):
                present = conn.execute(
                    "SELECT to_regclass(%s)", [f"{schema}.{table}"]
                ).fetchone()
                if present and present[0] is not None:
                    conn.execute(
                        sql.SQL(
                            "DELETE FROM {schema}.{table} WHERE full_name = %s"
                        ).format(
                            schema=sql.Identifier(schema),
                            table=sql.Identifier(table),
                        ),
                        [full_name],
                    )
        logger.info("state: cleared all state for %s (schema %s)", full_name, schema)


def drop_environment(conn: psycopg.Connection, models: Sequence["Model"]) -> None:
    """Tear down an ephemeral suffixed environment with no trace: each model's
    suffixed target schema + its staging schema (`z_<target>`), plus the shared
    per-suffix state/library/errors schema (`z_bollhav_<suffix>`). Drops DATA
    too (the target schemas), so it's a full teardown, not a state reset.

    Refuses unless the models carry a schema suffix — it must be structurally
    impossible to drop prod's `z_bollhav` or an unsuffixed target schema. Models
    without a suffix are skipped; if none have one, it raises rather than no-op
    silently.

    Note: schema names are recomputed from each model's suffix. With a *date*
    `schema_suffix_appendix` the name embeds `now()`, so a later teardown can
    miss the originally-created schema — for that case discover by
    `LIKE 'z_bollhav_<suffix>%'` instead. Plain suffixes (the local-test case)
    are exact."""
    from bollhav.model.target import resolve_schema_name

    suffixed = [m for m in models if m.target.schema_suffix]
    if not suffixed:
        raise ValueError(
            "drop_environment refuses to run: no model carries a schema suffix, "
            f"so it would target prod schemas ({LIBRARY_SCHEMA} + unsuffixed "
            "targets). Set SCHEMA_SUFFIX for an ephemeral environment, or drop "
            "prod schemas by hand if you must."
        )
    target_schemas: set[str] = set()
    state_schemas: set[str] = set()
    for m in suffixed:
        target_schemas.add(m.target.schema_resolved)
        state_schemas.add(
            resolve_schema_name(
                LIBRARY_SCHEMA,
                m.target.schema_suffix,
                m.target.schema_suffix_appendix,
            )
        )
    with conn.transaction():
        for s in sorted(target_schemas):
            for schema in (s, f"z_{s}"):  # target + its staging schema
                conn.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(schema)
                    )
                )
        for s in sorted(state_schemas):
            conn.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(s))
            )
    logger.info(
        "dropped environment: target schemas %s, state schemas %s",
        sorted(target_schemas),
        sorted(state_schemas),
    )


__all__ = [
    "PostgresState",
    "LibraryEntry",
    "LIBRARY_SCHEMA",
    "LIBRARY_TABLE",
    "ERRORS_TABLE",
    "state_table_name",
    "drop_environment",
]
