from __future__ import annotations

import logging
import re

from typing import TYPE_CHECKING
from uuid import UUID

from psycopg import sql

from bollhav.postgres.messages.error import (
    StateHashCollisionError,
    PrefillRequiresStateError,
    OneshotRequiresStateError,
    InvalidPrefillStatusError,
    BlockedRowRequiresReasonError,
    ClearStateRefusedError,
)

from ._base import _PostgresStateBase
from ._naming import _name_digest
from ._ddl import (
    _STATE_DDL,
    _STATE_INDEX_DDL,
    _STATE_ONESHOT_INDEX_DDL,
    LIBRARY_SCHEMA,
    LIBRARY_TABLE,
    ERRORS_TABLE,
)

if TYPE_CHECKING:
    import psycopg

    from bollhav.model.intervals import TZInterval

logger = logging.getLogger(__name__)

# `findall` returns every (name, kind) so the banner lists all the missing
# upstreams, not just the first. `kind` is optional for older reasons.
_BLOCK_UPSTREAM_RE = re.compile(r"upstream '([^']+)'(?: \(([^)]+)\))?")


class StateTable(_PostgresStateBase):
    """The per-model state table and the interval-status lifecycle over it:
    create/migrate the table, prefill interval rows, select the actionable
    ones, transition statuses (running → applied / blocked / error), and
    torch/clear. One row per interval."""

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
        state_oneshot_index = f"{digest}_oneshot_idx"

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
            # oneshot index, which needs those columns to exist.
            self._migrate_state_additively(state_schema, state_table)
            conn.execute(
                sql.SQL(_STATE_INDEX_DDL).format(
                    index=sql.Identifier(state_index),
                    schema=sql.Identifier(state_schema),
                    table=sql.Identifier(state_table),
                )
            )
            conn.execute(
                sql.SQL(_STATE_ONESHOT_INDEX_DDL).format(
                    index=sql.Identifier(state_oneshot_index),
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
            raise StateHashCollisionError(
                schema, state_table, row[0], self.model.target.full_name
            )

    def _migrate_state_additively(
        self,
        state_schema: str,
        state_table: str,
    ) -> None:
        """Bring an older state table up to the current shape without breaking
        concurrent old-image writers. Runs inside the caller's transaction.
        Additive only (per the shared-table rule): add the `temporality` and
        `model_name` columns, and relax `since`/`until` to nullable so
        oneshot / view rows carry a NULL window. Each step checks
        information_schema first, so it's idempotent."""
        conn = self._require_conn()

        has_temporality = conn.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "  AND column_name = 'temporality' LIMIT 1",
            [state_schema, state_table],
        ).fetchone()
        if not has_temporality:
            conn.execute(
                sql.SQL(
                    "ALTER TABLE {schema}.{table} "
                    "ADD COLUMN temporality TEXT NOT NULL DEFAULT 'temporal'"
                ).format(
                    schema=sql.Identifier(state_schema),
                    table=sql.Identifier(state_table),
                )
            )
            logger.info(
                "state: migrated %s.%s — added temporality column (default 'temporal' "
                "so older images keep writing temporal rows)",
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
                    "(oneshot / view rows carry a NULL window)",
                    state_schema,
                    state_table,
                    col,
                )

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
            raise PrefillRequiresStateError(model.target.full_name)

        if not intervals:
            return

        rows = [self._normalize_prefill_row(item) for item in intervals]

        schema = self._state_schema()
        table = self._state_table()

        from bollhav.model.state import StateMode

        on_conflict = ""
        # TORCH deletes every row up front (in the lifecycle), so its prefill
        # hits no conflicts — fall in with DISCOVER's preserve-applied upsert as
        # a harmless safety net for any stray row.
        if model.state.mode in (StateMode.DISCOVER, StateMode.TORCH):
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

    def prefill(self, *, run_id: UUID, intervals: tuple) -> None:
        """Prefill the state table: insert one **pending** row per interval that
        isn't already present, leaving every existing row exactly as it was
        (`ON CONFLICT (since, until) DO NOTHING`).

        This is *materialization only* — keep the table complete against the
        contract — and it is **mode-independent**: a row that's `applied`,
        `blocked`, or `pending` is never disturbed here. It is also incremental:
        prior runs have already inserted most of the declared intervals, so a
        run only adds the new ticks at the contract's advancing edge.

        Invalidation — redoing rows that are already `applied` — is a separate,
        explicit step: `reset_window` (bulldozer, the run's window) or
        `torch_rows` (torch, everything). Decoupling the two is what lets a
        plain `latest` run keep the table honest without re-running history."""
        model = self.model
        if model.state is None:
            raise PrefillRequiresStateError(model.target.full_name)
        if not intervals:
            return

        rows = [self._normalize_prefill_row(item) for item in intervals]
        schema = self._state_schema()
        table = self._state_table()

        insert = sql.SQL(
            "INSERT INTO {schema}.{table} "
            "(model_name, run_id, since, until, status, blocked_reason) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (since, until) DO NOTHING"
        ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))

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
        logger.debug(
            "state: prefilled %s from contract (%d interval(s) offered; only "
            "missing rows inserted, existing preserved)",
            model.target.full_name,
            len(rows),
        )

    def reset_window(self, *, run_id: UUID, window: "TZInterval") -> int:
        """Bulldozer invalidation: flip every row whose `[since, until)` falls
        inside `window` back to `pending` — clearing the applied/blocked marks —
        so it re-runs. Rows outside the window are left untouched, and a
        `running` row is skipped. Returns the number of rows reset.

        Scoped to the run window on purpose: bulldozer redoes exactly what the
        run named, never the whole table (that's `torch_rows`)."""
        model = self.model
        schema = self._state_schema()
        table = self._state_table()
        conn = self._require_conn()
        n = conn.execute(
            sql.SQL(
                "UPDATE {schema}.{table} SET status = 'pending', "
                "applied_at = NULL, blocked_reason = NULL, run_id = %s "
                "WHERE status <> 'running' AND since >= %s AND until <= %s"
            ).format(schema=sql.Identifier(schema), table=sql.Identifier(table)),
            [str(run_id), window.since, window.until],
        ).rowcount
        logger.debug(
            "state: bulldozed %d row(s) in [%s, %s) of %s back to pending",
            n,
            window.since,
            window.until,
            model.target.full_name,
        )
        return n

    def insert_oneshot(self, *, run_id: UUID) -> None:
        """Prefill the single NULL-window row for a model with no window to
        track — a timeless model, or a temporal one with no declared range.
        The counterpart to `insert_intervals`. The row carries
        `temporality = model.temporality.value` and a NULL `since`/`until`; the partial
        unique index keeps it unique.

        Same DISCOVER / BULLDOZER semantics as interval prefill: DISCOVER
        preserves an already-applied row (so a loaded table / existing view
        isn't re-run), BULLDOZER resets it to pending."""
        model = self.model
        if model.state is None:
            raise OneshotRequiresStateError(model.target.full_name)

        schema = self._state_schema()
        table = self._state_table()

        from bollhav.model.state import StateMode

        on_conflict = sql.SQL("")
        # TORCH clears the row first (in the lifecycle), so its prefill hits no
        # conflict — share DISCOVER's preserve-applied upsert as a safety net.
        if model.state.mode in (StateMode.DISCOVER, StateMode.TORCH):
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
            "(model_name, run_id, since, until, status, blocked_reason, temporality) "
            "VALUES (%s, %s, NULL, NULL, 'pending', NULL, %s) {on_conflict}"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            on_conflict=on_conflict,
        )

        conn = self._require_conn()
        with conn.transaction():
            conn.execute(
                insert, [model.target.full_name, str(run_id), model.temporality.value]
            )
        logger.debug(
            "state: prefilled %s oneshot row for %s (mode=%s)",
            model.temporality.value,
            model.target.full_name,
            model.state.mode.value,
        )

    def torch_rows(self) -> None:
        """Delete every state row for this model (keeping the state table and
        the library registration), so the next prefill rediscovers intervals
        from scratch at the current chunk. The `STATE_MODE=torch` escape hatch:
        unlike BULLDOZER (reset existing rows to pending — granularity fixed) it
        clears the rows entirely, so a chunk-granularity change or a stale
        backlog starts clean. Lighter than `clear_state` (which also drops the
        table + deregisters). Does NOT touch the model's data.

        Destructive — applied history is lost, so the next run reprocesses
        every interval (idempotent for MERGE / recreate; APPEND would
        duplicate)."""
        schema = self._state_schema()
        table = self._state_table()
        conn = self._require_conn()
        # `ensure_tables` runs before this in the lifecycle, but stay safe if the
        # table was never bootstrapped — nothing to torch.
        present = conn.execute(
            "SELECT to_regclass(%s)", [f"{schema}.{table}"]
        ).fetchone()
        if not (present and present[0] is not None):
            return
        with conn.transaction():
            deleted = conn.execute(
                sql.SQL("DELETE FROM {schema}.{table}").format(
                    schema=sql.Identifier(schema), table=sql.Identifier(table)
                )
            ).rowcount
        logger.warning(
            "state: TORCH deleted %d row(s) for %s — re-prefilling from scratch",
            deleted,
            self.model.target.full_name,
        )

    @staticmethod
    def _normalize_prefill_row(item) -> tuple:
        """Accept either bare TZInterval (→ pending) or 3-tuple."""
        if isinstance(item, tuple) and len(item) == 3:
            interval, status, reason = item
            if status not in ("pending", "blocked"):
                raise InvalidPrefillStatusError(status)
            if status == "blocked" and not reason:
                raise BlockedRowRequiresReasonError()
            return (interval, status, reason if status == "blocked" else None)
        # Bare TZInterval — backward-compat: treat as pending.
        return (item, "pending", None)

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

    def get_actionable_intervals(self, window: "TZInterval | None" = None) -> tuple:
        """Return rows whose status is NOT `'applied'` — i.e. pending, blocked,
        running, or error — ordered oldest first.

        **Window-scoped.** When `window` is given, only rows that fall fully
        inside `[window.since, window.until)` are returned — the run executes
        exactly its window, nothing leaks in from the backlog. When `window` is
        `None` (a oneshot / view row, or a deliberate reconcile), every
        non-applied row is returned (the historical "drain everything").

        Used by `@load_models` to populate `model.intervals`. The decorator
        re-evaluates each row at run time: blocked rows whose upstream now
        satisfies become processable; pending rows whose upstream regressed get
        marked blocked. So `blocked` is transient — a snapshot of what's
        currently waiting, not a terminal state.

        A oneshot / view model has a single NULL-window row: with no
        `window` this yields `(None,)` when actionable (the user's loop runs
        once with `interval=None`) and `()` when already applied."""
        from bollhav.model.intervals import TZInterval

        schema = self._state_schema()
        table = self._state_table()
        # Rows fully inside the half-open run window — the honest bound. The
        # `%s` are bound params; the rest of the string is constant.
        where = "status <> 'applied'"
        params: list = []
        if window is not None:
            where += " AND since >= %s AND until <= %s"
            params = [window.since, window.until]
        query = sql.SQL(
            "SELECT since, until FROM {schema}.{table} "
            "WHERE " + where + " ORDER BY since, until"
        ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))

        conn = self._require_conn()
        rows = conn.execute(query, params).fetchall()
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
        `interval` is a TZInterval, or None for a oneshot / view row whose
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
            raise ClearStateRefusedError(self.model.target.full_name, LIBRARY_SCHEMA)
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

    def uncover(self, since, until) -> int:
        """Range-subtraction invalidation for a flexible (coverage) model: remove
        `[since, until)` from this model's applied coverage, splitting an applied
        row that straddles a boundary. The flexible counterpart of flipping a
        fixed grid row to `pending`. Returns the number of applied rows removed."""
        return uncover_span(
            self._require_conn(),
            self._state_schema(),
            self._state_table(),
            since,
            until,
        )

    def uncovered_gaps(self, since, until) -> list:
        """The uncovered ranges of `[since, until)` for a flexible (coverage)
        model — the complement of this model's `applied` coverage within that
        horizon, as maximal gaps. The bootstrap slices each gap by the run's
        chunk into the units to run, so work is *the holes in coverage*, not a
        fixed grid. A fully-covered model returns `[]`.

        This is the flexible counterpart of `contract_intervals` (which is the
        whole grid): here only what isn't `applied` yet becomes work."""
        from bollhav.model.intervals import TZInterval

        schema = self._state_schema()
        table = self._state_table()
        # multirange([since, until)) − range_agg(applied) = the gaps. Mirrors
        # `uncover_span`'s set math, reversed (horizon minus coverage). `%(s)s` /
        # `%(u)s` are bound params; the rest is constant.
        rows = (
            self._require_conn()
            .execute(
                sql.SQL(
                    "SELECT lower(g), upper(g) FROM unnest("
                    "tstzmultirange(tstzrange(%(s)s, %(u)s, '[)')) - ("
                    "SELECT COALESCE("
                    "range_agg(tstzrange(since, until, '[)')), '{{}}'::tstzmultirange"
                    ") FROM {schema}.{table} "
                    "WHERE status = 'applied' AND since IS NOT NULL"
                    ")) AS g ORDER BY lower(g)"
                ).format(schema=sql.Identifier(schema), table=sql.Identifier(table)),
                {"s": since, "u": until},
            )
            .fetchall()
        )
        # Return gaps in the chunk's own timezone: PG hands timestamps back in the
        # session tz, and `split_window` walks cron ticks in whatever tz the
        # boundary carries — so a daily/monthly slice off a session-tz boundary
        # would land off midnight (a partial first/last unit). Re-frame to the
        # chunk's tz, the same frame `resolve_window` builds the grid in.
        tz = self.model.batching.time.tz if self.model.batching else None
        gaps = [
            TZInterval(
                s.astimezone(tz) if tz else s,
                u.astimezone(tz) if tz else u,
            )
            for s, u in rows
        ]
        logger.debug(
            "state: %d coverage gap(s) in [%s, %s) for %s",
            len(gaps),
            since,
            until,
            self.model.target.full_name,
        )
        return gaps

    def clear_window(self, window: "TZInterval") -> int:
        """Flexible bulldozer invalidation: make `window` a clean gap so the next
        prefill re-discovers the whole window as work. Two steps:

          1. `uncover` its applied coverage (range-subtraction, straddle-safe —
             a coalesced island wider than the window is split, not lost).
          2. delete any leftover non-`applied` rows fully inside it (stale
             pending/blocked/error from an earlier, possibly differently-chunked
             run), so they don't linger at the old grain. A `running` row is
             spared — a live worker owns it.

        The flexible counterpart of `reset_window`. Returns the number of applied
        rows uncovered."""
        removed = self.uncover(window.since, window.until)
        schema = self._state_schema()
        table = self._state_table()
        self._require_conn().execute(
            sql.SQL(
                "DELETE FROM {schema}.{table} WHERE status <> 'applied' "
                "AND status <> 'running' AND since >= %s AND until <= %s"
            ).format(schema=sql.Identifier(schema), table=sql.Identifier(table)),
            [window.since, window.until],
        )
        logger.debug(
            "state: cleared window [%s, %s) of %s (uncovered %d applied island(s))",
            window.since,
            window.until,
            self.model.target.full_name,
            removed,
        )
        return removed


# Overlap predicate shared by uncover's read/delete: an applied, windowed row
# whose range intersects the target span. `%(s)s` / `%(u)s` are bound params.
_UNCOVER_OVERLAP = (
    "status = 'applied' AND since IS NOT NULL "
    "AND tstzrange(since, until, '[)') && tstzrange(%(s)s, %(u)s, '[)')"
)


def uncover_span(
    conn: psycopg.Connection, schema: str, table: str, since, until
) -> int:
    """Subtract `[since, until)` from a state table's applied coverage (the
    flexible invalidation primitive — the engine's `uncover(span)`).

    Coverage is the union of the `applied` rows' ranges. Subtracting a span
    re-materializes that union minus the span: rows fully inside the span
    vanish, and a row straddling a boundary is **split** so only the overlap is
    uncovered. PG multirange arithmetic does the set math (`range_agg(...) −
    span`); `running` rows are never considered. Returns the number of applied
    rows removed (0 if nothing overlapped).

    Cases (`█` = applied/covered, `·` = uncovered, `^^^` = the span removed)::

        1. span strictly inside one row  →  the row splits in two
           before:   ████████████████████        [Jan ─────────── Apr)
           span:           ^^^^^^                      [Feb ─ Mar)
           after:    █████·······████████        [Jan,Feb)  ·  [Mar,Apr)

        2. span flush to a boundary      →  the row shrinks (one remainder)
           before:   ████████████████              [Jan ─────── Mar)
           span:               ^^^^^^                       [Feb ─ Apr)
           after:    ████████····                  [Jan ─ Feb)

        3. span ⊇ the row                →  the row is deleted (no remainder)
           before:        ████████                      [Feb ─ Mar)
           span:        ^^^^^^^^^^^^                  [Jan ───── Apr)
           after:        ········                   (fully uncovered)

        4. span spans several rows       →  each trimmed/merged via the union
           before:   ███████   ██████   ███       three coalesced islands
           span:        ^^^^^^^^^^^^^^                a span across the gap
           after:    ███····· · ·····██       outer parts kept, middle gone

    A re-materialized remainder row keeps `status='applied'`; the uncovered
    slice becomes a gap the next run's coverage query re-discovers."""
    ident = {"schema": sql.Identifier(schema), "table": sql.Identifier(table)}
    params = {"s": since, "u": until}
    with conn.transaction():
        meta = conn.execute(
            sql.SQL(
                "SELECT model_name, run_id, temporality "
                "FROM {schema}.{table} WHERE " + _UNCOVER_OVERLAP + " LIMIT 1"
            ).format(**ident),
            params,
        ).fetchone()
        if meta is None:
            return 0
        model_name, run_id, temporality = meta
        # remainder = coverage(overlapping rows) − span
        remainder = conn.execute(
            sql.SQL(
                "SELECT lower(r), upper(r) FROM unnest(("
                "SELECT COALESCE("
                "range_agg(tstzrange(since, until, '[)')), '{{}}'::tstzmultirange"
                ") FROM {schema}.{table} WHERE " + _UNCOVER_OVERLAP + ") "
                "- tstzmultirange(tstzrange(%(s)s, %(u)s, '[)'))) AS r"
            ).format(**ident),
            params,
        ).fetchall()
        removed = conn.execute(
            sql.SQL("DELETE FROM {schema}.{table} WHERE " + _UNCOVER_OVERLAP).format(
                **ident
            ),
            params,
        ).rowcount
        for s, u in remainder:
            conn.execute(
                sql.SQL(
                    "INSERT INTO {schema}.{table} (model_name, run_id, since, until, "
                    "status, applied_at, temporality) "
                    "VALUES (%s, %s, %s, %s, 'applied', now(), %s)"
                ).format(**ident),
                [model_name, run_id, s, u, temporality],
            )
    return removed
