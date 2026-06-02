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
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

import psycopg
from psycopg import sql

if TYPE_CHECKING:
    from bollhav.model.model import Model

logger = logging.getLogger(__name__)


# ── DDL ──────────────────────────────────────────────────────────────


_STATE_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL,
    since           TIMESTAMPTZ NOT NULL,
    until           TIMESTAMPTZ NOT NULL,
    status          TEXT NOT NULL,
    blocked_reason  TEXT,
    applied_at      TIMESTAMPTZ,
    UNIQUE (since, until)
)
"""

_STATE_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} (status)
"""

_ERRORS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id              BIGSERIAL PRIMARY KEY,
    full_name       TEXT NOT NULL,
    run_id          UUID NOT NULL,
    since           TIMESTAMPTZ NOT NULL,
    until           TIMESTAMPTZ NOT NULL,
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


_BLOCK_UPSTREAM_RE = re.compile(r"upstream '([^']+)'")


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
                sql.SQL(_STATE_INDEX_DDL).format(
                    index=sql.Identifier(state_index),
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
        — a snapshot of what's currently waiting, not a terminal state."""
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
        return tuple(TZInterval(since=since, until=until) for since, until in rows)

    def read_status_summary(self) -> dict:
        """One-shot summary of the model's state table, used by the
        state banner.

        Returns:
            {
                "counts": {"pending": int, "applied": int, "blocked": int},
                "blocked_groups": {(code, upstream_name | None): count, ...}
            }

        Blocked rows are bucketed by `(code, upstream)` so the banner can
        say "STATE_001 × 3 · upstream 'warehouse.orders'" — one actionable
        line per distinct blocker, regardless of how many intervals it
        covers. Code is parsed as the substring before the first `:`;
        upstream name is extracted from the message text via the standard
        `upstream 'X'` shape that `format_block_reason` callers use."""
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
        blocked_groups: dict[tuple[str, str | None], int] = {}

        conn = self._require_conn()
        for status, reason in conn.execute(query).fetchall():
            counts[status] = counts.get(status, 0) + 1
            if status == "blocked" and reason:
                code = reason.split(":", 1)[0].strip()
                upstream_match = _BLOCK_UPSTREAM_RE.search(reason)
                upstream = upstream_match.group(1) if upstream_match else None
                if code:
                    key = (code, upstream)
                    blocked_groups[key] = blocked_groups.get(key, 0) + 1

        return {"counts": counts, "blocked_groups": blocked_groups}

    def is_applied(self, *, since: datetime, until: datetime) -> bool:
        schema = self._state_schema()
        table = self._state_table()
        query = sql.SQL(
            "SELECT 1 FROM {schema}.{table} "
            "WHERE since = %s AND until = %s AND status = 'applied' "
            "LIMIT 1"
        ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))

        conn = self._require_conn()
        row = conn.execute(query, [since, until]).fetchone()
        return row is not None

    def record_failure(
        self,
        *,
        run_id: UUID,
        since: datetime,
        until: datetime,
        error_type: str,
        error_message: str,
        traceback_text: str | None,
        update_state: bool = True,
    ) -> None:
        """Atomically log a failure to the errors table and (optionally)
        flip the state row to `'error'`.

        `update_state=False` is for the rare case where the staging flush
        already set state to `'applied'` (data is in target) but user code
        AFTER the `with stage(...)` block raised. We still log the error
        for debugging, but the state stays `applied` — we shouldn't
        downgrade a successful write."""
        schema = self._state_schema()
        errors_table = self._errors_table()
        state_table = self._state_table()

        insert_error = sql.SQL(
            "INSERT INTO {schema}.{table} "
            "(full_name, run_id, since, until, error_type, error_message, traceback) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(errors_table),
        )
        update_state_sql = sql.SQL(
            "UPDATE {schema}.{table} "
            "SET status = 'error', run_id = %s "
            "WHERE since = %s AND until = %s"
        ).format(schema=sql.Identifier(schema), table=sql.Identifier(state_table))

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
                conn.execute(update_state_sql, [str(run_id), since, until])
        logger.debug(
            "state: recorded error for %s (%s..%s) %s: %s (state_updated=%s)",
            self.model.target.full_name,
            since,
            until,
            error_type,
            error_message,
            update_state,
        )

    def mark_running(self, *, run_id: UUID, since: datetime, until: datetime) -> None:
        """Flip the state row to `'running'` — set by `@interval_lifecycle`
        just before invoking the user's execute. On success the same
        row flips to `'applied'`; on exception, to `'error'`. A row left
        as `'running'` after a process crash is treated like `'pending'`
        by the next DISCOVER-mode pre-fill (auto-recovered)."""
        schema = self._state_schema()
        table = self._state_table()
        update = sql.SQL(
            "UPDATE {schema}.{table} "
            "SET status = 'running', run_id = %s "
            "WHERE since = %s AND until = %s"
        ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))

        conn = self._require_conn()
        with conn.transaction():
            conn.execute(update, [str(run_id), since, until])
        logger.debug(
            "state: marked running %s..%s for %s",
            since,
            until,
            self.model.target.full_name,
        )

    def mark_blocked(
        self, *, run_id: UUID, since: datetime, until: datetime, reason: str
    ) -> None:
        """Flip the state row to `'blocked'` with the given reason. Set
        by `@interval_lifecycle` when the live upstream check fails at run
        time. Re-evaluated on the next DISCOVER-mode bootstrap or run."""
        schema = self._state_schema()
        table = self._state_table()
        update = sql.SQL(
            "UPDATE {schema}.{table} "
            "SET status = 'blocked', blocked_reason = %s, run_id = %s "
            "WHERE since = %s AND until = %s"
        ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))

        conn = self._require_conn()
        with conn.transaction():
            conn.execute(update, [reason, str(run_id), since, until])
        logger.debug(
            "state: marked blocked %s..%s for %s — %s",
            since,
            until,
            self.model.target.full_name,
            reason,
        )

    def mark_applied(self, *, run_id: UUID, since: datetime, until: datetime) -> None:
        schema = self._state_schema()
        table = self._state_table()
        update = sql.SQL(
            "UPDATE {schema}.{table} "
            "SET status = 'applied', applied_at = now(), run_id = %s "
            "WHERE since = %s AND until = %s"
        ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))

        conn = self._require_conn()
        with conn.transaction():
            conn.execute(update, [str(run_id), since, until])
        logger.debug(
            "state: marked applied %s..%s for %s",
            since,
            until,
            self.model.target.full_name,
        )

    # ── live upstream check (used by @interval_lifecycle when polling) ─

    def is_upstream_satisfied_live(
        self, since: datetime, until: datetime
    ) -> tuple[bool, str | None]:
        """Live per-interval upstream-satisfaction check.

        For each declared upstream, look it up in the library and query
        its state table for an applied row that exactly matches or fully
        encapsulates `(since, until)`.

        Returns `(True, None)` when every upstream satisfies, otherwise
        `(False, reason)` keyed by the first unsatisfied upstream.

        `reason` follows the same `format_block_reason(BlockCode, ...)`
        shape as bootstrap-time blocked rows, so the banner / errors view
        look consistent."""
        from bollhav.model.state import BlockCode, format_block_reason
        from bollhav.postgres import library as pg_library

        conn = self._require_conn()
        for upstream_name in self.model.upstream:
            entry = pg_library.lookup(conn, upstream_name)
            if entry is None:
                # Not in the library → not an enforced dependency. Only
                # state-tracked models register, so an unregistered name
                # (a view, a state-less table, or a typo) is documentation
                # and does not block.
                continue
            if not pg_library.is_satisfied(conn, entry=entry, since=since, until=until):
                return (
                    False,
                    format_block_reason(
                        BlockCode.UPSTREAM_NOT_SATISFIED,
                        (
                            f"upstream {upstream_name!r} ({entry.model_type}) has no "
                            f"applied row covering {since.isoformat()} → "
                            f"{until.isoformat()}"
                        ),
                    ),
                )
        return (True, None)

    # ── advisory locks ───────────────────────────────────────────────

    def _interval_lock_key(self, since: datetime, until: datetime) -> str:
        """Hash key for an interval-scoped lock: identifies the specific
        `(model, since, until)` triple. Used by `@interval_lifecycle` so two
        workers can race on the same model but different intervals
        without conflict."""
        return f"{self.model.target.full_name}|{since.isoformat()}|{until.isoformat()}"

    def try_acquire_interval_lock(self, since: datetime, until: datetime) -> bool:
        """Try to take a session-scoped advisory lock for ONE interval.
        Returns True if acquired, False if another session already holds
        it. Released by `release_interval_lock` or automatically when
        the connection closes."""
        conn = self._require_conn()
        row = conn.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s))",
            [self._interval_lock_key(since, until)],
        ).fetchone()
        return bool(row and row[0])

    def release_interval_lock(self, since: datetime, until: datetime) -> None:
        conn = self._require_conn()
        conn.execute(
            "SELECT pg_advisory_unlock(hashtext(%s))",
            [self._interval_lock_key(since, until)],
        )

    # Optional model-wide lock — kept for power users who want exclusive
    # access to a whole model run. Not used by `@interval_lifecycle` (which
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


__all__ = ["PostgresState"]
