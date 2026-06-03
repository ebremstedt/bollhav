"""Postgres backend for bollhav state tracking.

Each state-enabled model gets one table in the target DB:

    z_<target_schema>.<target_name>_state

The `z_` prefix keeps bollhav-owned tables out of the user's schemas
and sorts them to the bottom of a DB editor's schema list.

The caller owns the connection: a `PostgresState` is constructed with the
model and the state connection (opened in `main()` and threaded through
the lifecycle hooks). `PostgresState` does not open its own connections.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, NamedTuple
from uuid import UUID

import psycopg
from psycopg import sql

if TYPE_CHECKING:
    from bollhav.model.intervals import TZInterval
    from bollhav.model.model import Model
    from bollhav.model.upstream import UpstreamCheck

logger = logging.getLogger(__name__)


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

# `since`/`until` are nullable here too: a monolithic / view failure logs an
# error row with a NULL window (it has no time slice).
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

_ERRORS_INTERVAL_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (since, until)
"""

_ERRORS_TIME_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (created_at DESC)
"""


# Matches each `upstream 'name' (kind)` descriptor in a blocked_reason —
# `findall` returns every (name, kind) so the banner lists all the missing
# upstreams, not just the first. `kind` is optional for older reasons.
_BLOCK_UPSTREAM_RE = re.compile(r"upstream '([^']+)'(?: \(([^)]+)\))?")


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
LIBRARY_TABLE = "model_library"

_LIBRARY_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    full_name      TEXT PRIMARY KEY,
    upstream       TEXT[] NOT NULL,
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
    library-only rows written by older bollhav images."""

    upstream: list[str]
    model_type: str
    state_schema: str | None
    state_table: str | None
    kind: str


class PostgresState:
    """Postgres-backed state store for a single model.

    Construct with the model and the caller-owned state connection
    (opened in `main()`, threaded through the lifecycle hooks). The
    naming helpers (`_state_schema`, `_state_table`, `_errors_table`)
    work with the connection unset; every DB method requires it."""

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

    def _state_schema(self) -> str:
        """State schema name. `State.schema_prefix` overrides the default
        `"z_"` prefix on the target's schema name."""
        model = self.model
        prefix = (
            model.state.schema_prefix
            if model.state is not None and model.state.schema_prefix is not None
            else "z_"
        )
        return f"{prefix}{model.target.schema.resolved}"

    def _state_table(self) -> str:
        """State table name. `State.table_suffix` overrides the default
        `"_state"` suffix on the target's table name."""
        model = self.model
        suffix = (
            model.state.table_suffix
            if model.state is not None and model.state.table_suffix is not None
            else "_state"
        )
        return f"{model.target.name}{suffix}"

    def _errors_table(self) -> str:
        """Errors table name. Lives in the same state schema as the state
        table; suffix derived from the model's name (just like the state
        table's `_state` suffix, but `_errors`)."""
        return f"{self.model.target.name}_errors"

    # ── DDL ──────────────────────────────────────────────────────────

    def ensure_tables(self) -> None:
        """Create the state schema, state table, AND errors table if
        absent. Idempotent. Both tables live in the same state schema —
        join them on (since, until) for per-interval inspection or on
        run_id for per-invocation lookups."""
        schema = self._state_schema()
        state_table = self._state_table()
        state_index = f"{state_table}_status_idx"
        state_singleton_index = f"{state_table}_singleton_idx"
        errors_table = self._errors_table()
        errors_iv_index = f"{errors_table}_interval_idx"
        errors_time_index = f"{errors_table}_created_at_idx"

        conn = self._require_conn()
        with conn.transaction():
            conn.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(
                    schema=sql.Identifier(schema),
                )
            )
            conn.execute(
                sql.SQL(_STATE_DDL).format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(state_table),
                )
            )
            conn.execute(
                sql.SQL(_ERRORS_DDL).format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(errors_table),
                )
            )
            # Bring older tables up to the current shape (nullable window +
            # `kind`) before building the partial singleton index, which
            # needs those columns to exist.
            self._migrate_state_additively(schema, state_table, errors_table)
            conn.execute(
                sql.SQL(_STATE_INDEX_DDL).format(
                    index=sql.Identifier(state_index),
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(state_table),
                )
            )
            conn.execute(
                sql.SQL(_STATE_SINGLETON_INDEX_DDL).format(
                    index=sql.Identifier(state_singleton_index),
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(state_table),
                )
            )
            conn.execute(
                sql.SQL(_ERRORS_INTERVAL_INDEX_DDL).format(
                    index=sql.Identifier(errors_iv_index),
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(errors_table),
                )
            )
            conn.execute(
                sql.SQL(_ERRORS_TIME_INDEX_DDL).format(
                    index=sql.Identifier(errors_time_index),
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(errors_table),
                )
            )
        logger.debug(
            "state: ensured tables for %s (%s.%s + %s)",
            self.model.target.full_name,
            schema,
            state_table,
            errors_table,
        )

    def _migrate_state_additively(
        self, schema: str, state_table: str, errors_table: str
    ) -> None:
        """Bring older state/errors tables up to the current shape without
        breaking concurrent old-image writers. Runs inside the caller's
        transaction. Additive only (per the shared-table rule): add the
        state table's `kind` column with a safe default, and relax
        `since`/`until` to nullable on both tables so monolithic / view rows
        carry a NULL window. Each step checks information_schema first, so
        it's idempotent."""
        conn = self._require_conn()

        has_kind = conn.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "  AND column_name = 'kind' LIMIT 1",
            [schema, state_table],
        ).fetchone()
        if not has_kind:
            conn.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} "
                    "ADD COLUMN kind TEXT NOT NULL DEFAULT 'interval'"
                ).format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(state_table),
                )
            )
            logger.info(
                "state: migrated %s.%s — added kind column (default "
                "'interval' so older images keep writing interval rows)",
                schema,
                state_table,
            )

        for table in (state_table, errors_table):
            for col in ("since", "until"):
                is_nullable = conn.execute(
                    "SELECT is_nullable FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s "
                    "  AND column_name = %s LIMIT 1",
                    [schema, table, col],
                ).fetchone()
                if is_nullable and is_nullable[0] == "NO":
                    conn.execute(
                        sql.SQL(
                            "ALTER TABLE {schema}.{table} "
                            "ALTER COLUMN {col} DROP NOT NULL"
                        ).format(
                            schema=sql.Identifier(schema),
                            table=sql.Identifier(table),
                            col=sql.Identifier(col),
                        )
                    )
                    logger.info(
                        "state: migrated %s.%s — relaxed NOT NULL on %s "
                        "(monolithic / view rows carry a NULL window)",
                        schema,
                        table,
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
            "(run_id, since, until, status, blocked_reason) "
            "VALUES (%s, %s, %s, %s, %s) {on_conflict}"
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
            "(run_id, since, until, status, blocked_reason, kind) "
            "VALUES (%s, NULL, NULL, 'pending', NULL, %s) {on_conflict}"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            on_conflict=on_conflict,
        )

        conn = self._require_conn()
        with conn.transaction():
            conn.execute(insert, [str(run_id), model.kind.value])
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
        schema = self._state_schema()
        errors_table = self._errors_table()
        state_table = self._state_table()
        since = interval.since if interval is not None else None
        until = interval.until if interval is not None else None

        insert_error = sql.SQL(
            "INSERT INTO {schema}.{table} "
            "(full_name, run_id, since, until, error_type, error_message, traceback) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(errors_table),
        )
        match, match_params = self._window_match(interval)
        update_state_sql = sql.SQL(
            "UPDATE {schema}.{table} SET status = 'error', run_id = %s WHERE {match}"
        ).format(
            schema=sql.Identifier(schema),
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
        from bollhav.model.upstream import Contract, UpstreamCheck

        conn = self._require_conn()
        blockers: list[str] = []
        for upstream in self.model.upstream:
            is_contract = isinstance(upstream, Contract)
            name = upstream.name if is_contract else upstream
            kind = upstream.kind if is_contract else None
            match = PostgresState.lookup_model(conn, name)
            if match is None:
                # Not in the library — the upstream has never registered.
                if is_contract:
                    # A declared Contract is a demand: an unregistered
                    # upstream is a real error (a typo, or the upstream was
                    # never deployed / run), not something to skip.
                    raise ValueError(
                        f"upstream contract {name!r} ({kind}) on "
                        f"{self.model.target.full_name!r} is not registered in "
                        f"the library — it has never run. A declared Contract "
                        f"demands the upstream exists; fix the name or run the "
                        f"upstream first. (A bare-string upstream is treated as "
                        f"documentation and would not block.)"
                    )
                # Bare string → documentation, not an enforced dependency.
                # Only state-tracked models register, so an unregistered name
                # (a view, a state-less table, or a typo) does not block.
                continue
            if not PostgresState.is_satisfied(
                conn, entry=match, interval=interval, kind=kind
            ):
                blockers.append(f"upstream {name!r} ({kind or match.kind})")
        return UpstreamCheck(blockers=tuple(blockers))

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
                    schema=sql.Identifier(LIBRARY_SCHEMA),
                )
            )
            conn.execute(
                sql.SQL(_LIBRARY_DDL).format(
                    schema=sql.Identifier(LIBRARY_SCHEMA),
                    table=sql.Identifier(LIBRARY_TABLE),
                )
            )
            self._migrate_library_additively()
        logger.debug("library: ensured %s.%s", LIBRARY_SCHEMA, LIBRARY_TABLE)

    def _migrate_library_additively(self) -> None:
        """Apply additive ALTERs to bring an older `model_library` up to
        the current shape without breaking concurrent old-image writers.
        Runs inside the caller's transaction. Each step checks the
        information_schema before issuing the ALTER, so the function is
        idempotent and cheap to call on every bootstrap."""
        conn = self._require_conn()
        has_model_type = conn.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "  AND column_name = 'model_type' LIMIT 1",
            [LIBRARY_SCHEMA, LIBRARY_TABLE],
        ).fetchone()
        if not has_model_type:
            conn.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} "
                    "ADD COLUMN model_type TEXT NOT NULL DEFAULT 'TABLE'"
                ).format(
                    schema=sql.Identifier(LIBRARY_SCHEMA),
                    table=sql.Identifier(LIBRARY_TABLE),
                )
            )
            logger.info(
                "library: migrated %s.%s — added model_type column "
                "(default 'TABLE' so older images can keep writing)",
                LIBRARY_SCHEMA,
                LIBRARY_TABLE,
            )

        has_kind = conn.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "  AND column_name = 'kind' LIMIT 1",
            [LIBRARY_SCHEMA, LIBRARY_TABLE],
        ).fetchone()
        if not has_kind:
            conn.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} "
                    "ADD COLUMN kind TEXT NOT NULL DEFAULT 'interval'"
                ).format(
                    schema=sql.Identifier(LIBRARY_SCHEMA),
                    table=sql.Identifier(LIBRARY_TABLE),
                )
            )
            logger.info(
                "library: migrated %s.%s — added kind column (default "
                "'interval' so older images keep registering interval models)",
                LIBRARY_SCHEMA,
                LIBRARY_TABLE,
            )

        for col in ("state_schema", "state_table"):
            is_nullable = conn.execute(
                "SELECT is_nullable FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "  AND column_name = %s LIMIT 1",
                [LIBRARY_SCHEMA, LIBRARY_TABLE, col],
            ).fetchone()
            if is_nullable and is_nullable[0] == "NO":
                conn.execute(
                    sql.SQL(
                        "ALTER TABLE {schema}.{table} ALTER COLUMN {col} DROP NOT NULL"
                    ).format(
                        schema=sql.Identifier(LIBRARY_SCHEMA),
                        table=sql.Identifier(LIBRARY_TABLE),
                        col=sql.Identifier(col),
                    )
                )
                logger.info(
                    "library: migrated %s.%s — relaxed NOT NULL on %s "
                    "(view / library-only rows store NULL here)",
                    LIBRARY_SCHEMA,
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
            "(full_name, upstream, model_type, state_schema, state_table, kind, last_seen) "
            "VALUES (%s, %s, %s, %s, %s, %s, now()) "
            "ON CONFLICT (full_name) DO UPDATE SET "
            "upstream = EXCLUDED.upstream, "
            "model_type = EXCLUDED.model_type, "
            "state_schema = EXCLUDED.state_schema, "
            "state_table = EXCLUDED.state_table, "
            "kind = EXCLUDED.kind, "
            "last_seen = EXCLUDED.last_seen"
        ).format(
            schema=sql.Identifier(LIBRARY_SCHEMA),
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
                ],
            )
        logger.debug(
            "library: registered %s (model_type=%s, kind=%s, upstream=%s)",
            model.target.full_name,
            model_type,
            model.kind.value,
            model.upstream_names,
        )

    @staticmethod
    def lookup_model(conn: psycopg.Connection, full_name: str) -> "LibraryEntry | None":
        """Return the `LibraryEntry` for a model, or None if unregistered.

        Registry-level (not scoped to one model), so it's a static method
        taking the connection — the caller looks up arbitrary upstream
        names, not `self.model`."""
        row = conn.execute(
            sql.SQL(
                "SELECT upstream, model_type, state_schema, state_table, kind "
                "FROM {schema}.{table} WHERE full_name = %s"
            ).format(
                schema=sql.Identifier(LIBRARY_SCHEMA),
                table=sql.Identifier(LIBRARY_TABLE),
            ),
            [full_name],
        ).fetchone()
        if row is None:
            return None
        upstream, model_type, state_schema, state_table, kind = row
        return LibraryEntry(
            upstream=list(upstream),
            model_type=model_type,
            state_schema=state_schema,
            state_table=state_table,
            kind=kind,
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


__all__ = [
    "PostgresState",
    "LibraryEntry",
    "LIBRARY_SCHEMA",
    "LIBRARY_TABLE",
]
