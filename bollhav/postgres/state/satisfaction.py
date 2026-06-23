from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import psycopg
from psycopg import sql

from ._base import _PostgresStateBase
from ._base import LibraryEntry

if TYPE_CHECKING:
    from bollhav.model.intervals import TZInterval
    from bollhav.model.upstream import UpstreamCheck

logger = logging.getLogger(__name__)


def _windows_cover(windows, interval) -> bool:
    """Do the would-run `windows` tile `interval` gap-free (a finer **union**)?
    The Python mirror of the ENCAPSULATE coverage SQL, for the `DRY_STATE`
    preview: sort the segments overlapping `interval`, sweep left-to-right, and
    fail on the first hole or if coverage never reaches `interval.until`."""
    segs = sorted(
        (w.since, w.until)
        for w in windows
        if w is not None and w.until > interval.since and w.since < interval.until
    )
    if not segs:
        return False
    cursor = interval.since
    for since, until in segs:
        if since > cursor:  # a hole before this segment
            return False
        if until > cursor:
            cursor = until
        if cursor >= interval.until:
            return True
    return cursor >= interval.until


def _overlay_covers(windows, level: str, interval) -> bool:
    """The `DRY_STATE` cascade rule: True when an upstream's would-run
    `windows` cover this downstream `interval` for the given contract `level`.
    A timeless upstream (or a whole-table `None` window) covers any downstream
    window. EXACT needs a would-run window that *equals* mine; the window-scoped
    cover levels need a would-run window that *contains* mine (and ENCAPSULATE
    also accepts a gap-free union of finer would-run windows)."""
    if not windows:
        return False
    if level in ("timeless", "exists", "whole") or interval is None:
        return True
    if level == "exact":
        return any(
            w is not None and w.since == interval.since and w.until == interval.until
            for w in windows
        )
    # A single would-run window that contains mine satisfies any cover level;
    # ENCAPSULATE additionally accepts a gap-free union of finer would-run rows.
    if any(
        w is None or (w.since <= interval.since and w.until >= interval.until)
        for w in windows
    ):
        return True
    if level in ("encapsulate", "temporal"):
        return _windows_cover(windows, interval)
    return False


class Satisfaction(_PostgresStateBase):
    def is_upstream_satisfied_live(
        self, interval: "TZInterval | None"
    ) -> "UpstreamCheck":
        """Live upstream-satisfaction check for one unit of work.

        Checks *every* declared upstream (no short-circuit) and returns an
        `UpstreamCheck` verdict carrying one block reason per
        unsatisfied upstream. For each upstream `UpstreamContract`, look it up
        in the library and check satisfaction by the contract's level
        (interval → window cover; view / monolithic → existence row
        applied). A bare-string upstream has no declared level, so it falls
        back to the upstream's own registered `temporality`. `interval` is the
        downstream's window, or None for a monolithic / view downstream
        (whole-table / existence work).

        A declared `UpstreamContract` whose upstream is not registered raises — an
        explicit demand on something that has never run is a hard error. A
        bare-string upstream that isn't registered is documentation and
        does not block.

        Each blocker is a short `upstream 'name' (level)` descriptor;
        `UpstreamCheck.reason` composes them into the single concise
        `blocked_reason` the row stores, and `read_status_summary` parses
        each back out so the banner can list every missing upstream."""
        from bollhav.model.upstream import UpstreamCheck

        conn = self._require_conn()
        downstream = self.model.target.full_name
        gated = self.model.gated_upstreams
        logger.debug(
            "contract: checking %d gated upstream(s) of %s for window %s",
            len(gated),
            downstream,
            interval,
        )
        blockers: list[str] = []
        for src in gated:
            if src.deactivate_for_dev and self.model.target.schema_suffix:
                # Shared upstream (`Source(..., deactivate_for_dev=True)`) in a SUFFIXED
                # (dev/PR) run: it lives in prod, not this env's library, so its
                # state is assumed okay — never looked up or waited on. In a prod
                # (unsuffixed) run schema_suffix is empty, so this is False and it
                # gates normally below — the flag needs no flipping between
                # environments.
                continue
            name = src.name
            assert src.contract is not None  # gated ⇒ contract is not None
            level = src.contract.value
            # Look the upstream up under THIS env's identity + library: a
            # suffixed run resolves against its own env, never prod's.
            match = self.lookup_model(
                conn, self._suffix_upstream_name(name), self._library_schema()
            )
            if match is None:
                # A gated upstream (a Source with a contract) is a hard demand:
                # an unregistered upstream is a real error (a typo, or the
                # upstream was never deployed / run). (An ungated source isn't
                # checked here at all — it's never iterated.)
                raise ValueError(
                    f"upstream contract {name!r} ({level}) on "
                    f"{self.model.target.full_name!r} is not registered in "
                    f"the library — it has never run. A gated upstream demands "
                    f"the upstream exists; fix the name or run the upstream "
                    f"first. (An ungated source would not block.)"
                )
            if (
                level in ("exact", "encapsulate", "through")
                and match.temporality == "timeless"
            ):
                # EXACT / ENCAPSULATE / THROUGH gate on a per-window match, but a
                # TIMELESS upstream has no window to match. Make it a definition
                # error, not a silent never-satisfied: the author must say WHOLE
                # (loaded) or EXISTS (registered) instead.
                raise ValueError(
                    f"upstream contract {name!r} ({level}) on "
                    f"{self.model.target.full_name!r} targets a TIMELESS "
                    f"upstream, which has no window to match. Use WHOLE "
                    f"(loaded) or EXISTS (registered) instead."
                )
            satisfied = self.is_satisfied(
                conn, entry=match, interval=interval, level=level
            )
            # Freshness is a second gate on top of completeness: even a fully
            # applied upstream is BLOCKED if its relevant rows are too old.
            stale = False
            if satisfied and src.freshness is not None:
                if not self.is_fresh(
                    conn,
                    entry=match,
                    interval=interval,
                    level=level,
                    freshness=src.freshness,
                ):
                    satisfied = False
                    stale = True
            logger.debug(
                "contract: %s upstream %r (%s%s, upstream temporality=%s) for %s window %s",
                "SATISFIED" if satisfied else ("STALE" if stale else "BLOCKED"),
                name,
                level,
                f", freshness {src.freshness.scope.value}≤{src.freshness.within}"
                if src.freshness is not None
                else "",
                match.temporality,
                downstream,
                interval,
            )
            if not satisfied:
                # Distinct descriptor so the banner / status summary shows that
                # the upstream is present-but-stale, not missing.
                blockers.append(
                    f"upstream {name!r} ({level}, stale)"
                    if stale
                    else f"upstream {name!r} ({level})"
                )
        if blockers:
            logger.debug(
                "contract: %s for window %s is BLOCKED by %s",
                downstream,
                interval,
                ", ".join(blockers),
            )
        else:
            logger.debug(
                "contract: %s for window %s is SATISFIED (all gates open)",
                downstream,
                interval,
            )
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
            if src.deactivate_for_dev and self.model.target.schema_suffix:
                # Suffixed run → shared prod upstream assumed okay (see
                # is_upstream_satisfied_live). A prod run gates it normally.
                continue
            name = src.name
            assert src.contract is not None  # gated ⇒ contract is not None
            level = src.contract.value
            lookup = self._suffix_upstream_name(name)
            if assume_applied and _overlay_covers(
                assume_applied.get(lookup), level, interval
            ):
                after.append(f"{name} ({level})")
                continue
            match = self.lookup_model(conn, lookup, self._library_schema())
            if match is not None and self.is_satisfied(
                conn, entry=match, interval=interval, level=level
            ):
                continue  # already applied
            blocked.append(f"{name} ({level})")
        if blocked:
            return ("blocked", blocked)
        if after:
            return ("after", after)
        return ("run", [])

    @staticmethod
    def is_satisfied(
        conn: psycopg.Connection,
        *,
        entry: "LibraryEntry",
        interval: "TZInterval | None",
        level: str | None = None,
    ) -> bool:
        """Is the upstream satisfied for `interval` (the downstream's
        window, or None for whole-table / existence work)?

        The check is keyed by `level`. When the downstream declared an
        `UpstreamContract`, `level` is the contract's level value (the
        downstream's explicit expectation); otherwise it falls back to the
        upstream's own registered `entry.temporality`. Either way:

        * library-only rows (`state_schema` / `state_table` NULL — older
          images, or models registered without state tracking): presence
          in the library is the proof. Always satisfied.
        * `level == 'exact'`: an `applied` row whose window *equals*
          `interval` (same since/until). Only an exact-grain match counts.
        * `level == 'encapsulate'` / `'temporal'` (bare): the `applied` rows
          *cover* `interval` — a single row containing it (fast path), or a
          gap-free union of finer rows. A daily-cadence upstream covers an
          hourly downstream; 24 hourly rows cover a daily one.
        * `level == 'timeless'`: the upstream has a single NULL-window
          existence row — satisfied iff that row is `applied` (the view
          exists / the whole table has been loaded). The downstream window
          is irrelevant.

        EXACT / ENCAPSULATE / THROUGH against a TIMELESS upstream is rejected
        upstream of here (`is_upstream_satisfied_live`), so by the time a
        contract level reaches this function it is shape-compatible. A
        windowless downstream (`interval is None`) reads *all* of an interval
        upstream, so the window-scoped levels degenerate to WHOLE.

        If the upstream's state table doesn't exist yet (registered but
        never bootstrapped), returns False."""
        level = level or entry.temporality

        # 'exists' (UpstreamContract.EXISTS): reaching here means the upstream
        # was found in the library — that registration is all this gate
        # requires, regardless of state. A windowless way to depend on a
        # temporal model.
        if level == "exists":
            return True

        if entry.state_schema is None or entry.state_table is None:
            return True

        exists = conn.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname=%s AND tablename=%s LIMIT 1",
            [entry.state_schema, entry.state_table],
        ).fetchone()
        if not exists:
            return False

        # 'whole' (UpstreamContract.WHOLE): the whole upstream is loaded —
        # at least one applied row and no outstanding (non-applied) ones.
        # Recomputed from state on every check, never stored: because the
        # in-progress (not-yet-elapsed) tick isn't written to state, "no
        # non-applied rows" means every *elapsed* interval — up to and
        # including the latest complete one — has run. Windowless, so a
        # timeless downstream can wait for full load where the single-window
        # interval coverage check can't express it.
        if level == "whole":
            return Satisfaction._whole_applied(
                conn, entry.state_schema, entry.state_table
            )

        if level == "timeless":
            query = sql.SQL(
                "SELECT 1 FROM {schema}.{table} "
                "WHERE status = 'applied' AND since IS NULL AND until IS NULL "
                "LIMIT 1"
            ).format(
                schema=sql.Identifier(entry.state_schema),
                table=sql.Identifier(entry.state_table),
            )
            return conn.execute(query).fetchone() is not None

        # 'through' (UpstreamContract.THROUGH): a gap-free *prefix* — every
        # upstream interval up to and including my window is `applied`, with no
        # outstanding interval in that prefix. For additive / cumulative models
        # whose window N sums history 1..N: WINDOW (only N) undercounts, WHOLE
        # over-waits. Anchored to my window's `until`, so intervals the upstream
        # grows *past* me don't block it (unlike WHOLE). A windowless downstream
        # (interval is None) has no anchor, so it degenerates to WHOLE.
        if level == "through":
            bound = sql.SQL(" AND until <= %s") if interval is not None else sql.SQL("")
            params = [interval.until] if interval is not None else []
            query = sql.SQL(
                "SELECT 1 WHERE EXISTS ("
                "  SELECT 1 FROM {schema}.{table} WHERE status = 'applied'{bound}"
                ") AND NOT EXISTS ("
                "  SELECT 1 FROM {schema}.{table} WHERE status <> 'applied'{bound}"
                ")"
            ).format(
                schema=sql.Identifier(entry.state_schema),
                table=sql.Identifier(entry.state_table),
                bound=bound,
            )
            return conn.execute(query, params * 2).fetchone() is not None

        # A windowless downstream (interval is None) reading an interval upstream
        # reads *all* of it — EXACT / ENCAPSULATE have no window to scope to, so
        # they degenerate to WHOLE (the documented "reads the whole thing" case).
        if interval is None:
            return Satisfaction._whole_applied(
                conn, entry.state_schema, entry.state_table
            )

        # 'exact' (UpstreamContract.EXACT): one applied row at *exactly* my grain
        # — same (since, until). A coarser row that merely contains my window does
        # not count (no per-my-grain row exists), nor does a union of finer rows.
        if level == "exact":
            query = sql.SQL(
                "SELECT 1 FROM {schema}.{table} "
                "WHERE status = 'applied' AND since = %s AND until = %s "
                "LIMIT 1"
            ).format(
                schema=sql.Identifier(entry.state_schema),
                table=sql.Identifier(entry.state_table),
            )
            return (
                conn.execute(query, [interval.since, interval.until]).fetchone()
                is not None
            )

        # 'encapsulate' (UpstreamContract.ENCAPSULATE; also the bare-temporal
        # fallback): my window's data is present — the applied rows COVER
        # [since, until). Fast path: one applied row already contains it (the
        # common same-or-coarser-grain case, a cheap indexed lookup). Else: do the
        # applied rows overlapping my window tile it gap-free (a union of finer
        # rows — 24 hourly rows covering my day)? Nothing outside my window is
        # consulted, so a hole elsewhere in the upstream never blocks me.
        fast = sql.SQL(
            "SELECT 1 FROM {schema}.{table} "
            "WHERE status = 'applied' AND since <= %s AND until >= %s "
            "LIMIT 1"
        ).format(
            schema=sql.Identifier(entry.state_schema),
            table=sql.Identifier(entry.state_table),
        )
        if conn.execute(fast, [interval.since, interval.until]).fetchone() is not None:
            return True

        cover = sql.SQL(
            "WITH ov AS ("
            "  SELECT since, until FROM {schema}.{table} "
            "  WHERE status = 'applied' AND until > %s AND since < %s"
            "), chain AS ("
            "  SELECT since, max(until) OVER ("
            "    ORDER BY since, until ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING"
            "  ) AS prev_max FROM ov"
            ") "
            "SELECT (SELECT min(since) FROM ov) <= %s "  # covers the left edge
            "   AND (SELECT max(until) FROM ov) >= %s "  # covers the right edge
            "   AND NOT EXISTS ("  # no internal hole
            "     SELECT 1 FROM chain WHERE prev_max IS NOT NULL AND since > prev_max"
            "   )"
        ).format(
            schema=sql.Identifier(entry.state_schema),
            table=sql.Identifier(entry.state_table),
        )
        row = conn.execute(
            cover, [interval.since, interval.until, interval.since, interval.until]
        ).fetchone()
        return bool(row and row[0])

    @staticmethod
    def _whole_applied(conn: psycopg.Connection, schema: str, table: str) -> bool:
        """WHOLE: at least one `applied` row and no outstanding (non-applied)
        one — the whole upstream is loaded. Shared by the WHOLE level and by the
        window-scoped levels when the downstream is windowless (reads all)."""
        query = sql.SQL(
            "SELECT 1 WHERE EXISTS ("
            "  SELECT 1 FROM {schema}.{table} WHERE status = 'applied'"
            ") AND NOT EXISTS ("
            "  SELECT 1 FROM {schema}.{table} WHERE status <> 'applied'"
            ")"
        ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))
        return conn.execute(query).fetchone() is not None

    @staticmethod
    def is_fresh(
        conn: psycopg.Connection,
        *,
        entry: "LibraryEntry",
        interval: "TZInterval | None",
        level: str | None,
        freshness,
    ) -> bool:
        """Recency gate, applied AFTER `is_satisfied` confirms completeness.

        `freshness` is a `Freshness(within, scope)`. The relevant applied rows
        — selected exactly the way `is_satisfied` selects them for `level` —
        must be recent: `scope=LATEST` requires the newest applied row to be
        within `within` of now; `scope=ALL` requires every relevant applied
        row to be (i.e. the oldest one). The age is computed in SQL against the
        DB clock — `applied_at` is stamped with `now()`, so there's no
        client/server skew.

        EXISTS, and library-only rows with no state table, have no `applied_at`
        to age, so freshness is vacuously satisfied (the same shapes
        `is_satisfied` short-circuits on)."""
        from bollhav.model.upstream import FreshnessScope

        level = level or entry.temporality
        if level == "exists" or entry.state_schema is None or entry.state_table is None:
            return True

        # Select the same applied rows the level treats as "the data I read".
        if level == "timeless":
            bound, params = sql.SQL(" AND since IS NULL AND until IS NULL"), []
        elif level == "whole" or interval is None:
            bound, params = sql.SQL(""), []
        elif level == "through":
            bound, params = sql.SQL(" AND until <= %s"), [interval.until]
        elif level == "exact":
            bound = sql.SQL(" AND since = %s AND until = %s")
            params = [interval.since, interval.until]
        else:  # encapsulate, or a bare temporal upstream → the rows covering my
            # window (every row overlapping it — the single container or the union)
            bound = sql.SQL(" AND until > %s AND since < %s")
            params = [interval.since, interval.until]

        agg = (
            sql.SQL("min") if freshness.scope is FreshnessScope.ALL else sql.SQL("max")
        )
        query = sql.SQL(
            "SELECT {agg}(applied_at) >= now() - %s "
            "FROM {schema}.{table} WHERE status = 'applied'{bound}"
        ).format(
            agg=agg,
            schema=sql.Identifier(entry.state_schema),
            table=sql.Identifier(entry.state_table),
            bound=bound,
        )
        row = conn.execute(query, [freshness.within, *params]).fetchone()
        # No applied rows → aggregate is NULL → row[0] is None → not fresh.
        return bool(row and row[0])
