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


def _overlay_covers(windows, kind: str, interval) -> bool:
    """The `DRY_STATE` cascade rule: True when an upstream's would-run
    `windows` cover this downstream `interval` for the given contract `kind`.
    A timeless upstream (or a whole-table `None` window) covers any downstream
    window; a temporal upstream needs a would-run window that contains the
    downstream's."""
    if not windows:
        return False
    if kind in ("timeless", "exists", "whole") or interval is None:
        return True
    for w in windows:
        if w is None:
            return True
        if w.since <= interval.since and w.until >= interval.until:
            return True
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
        back to the upstream's own registered `kind`. `interval` is the
        downstream's window, or None for a monolithic / view downstream
        (whole-table / existence work).

        A declared `UpstreamContract` whose upstream is not registered raises — an
        explicit demand on something that has never run is a hard error. A
        bare-string upstream that isn't registered is documentation and
        does not block.

        Each blocker is a short `upstream 'name' (kind)` descriptor;
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
            kind = src.contract.value
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
                    f"upstream contract {name!r} ({kind}) on "
                    f"{self.model.target.full_name!r} is not registered in "
                    f"the library — it has never run. A gated upstream demands "
                    f"the upstream exists; fix the name or run the upstream "
                    f"first. (An ungated source would not block.)"
                )
            if kind in ("window", "through") and match.kind == "timeless":
                # WINDOW / THROUGH gate on a per-window match, but a TIMELESS
                # upstream has no window to match. Make it a definition error,
                # not a silent never-satisfied: the author must say WHOLE
                # (loaded) or EXISTS (registered) instead.
                raise ValueError(
                    f"upstream contract {name!r} ({kind}) on "
                    f"{self.model.target.full_name!r} targets a TIMELESS "
                    f"upstream, which has no window to match. Use WHOLE "
                    f"(loaded) or EXISTS (registered) instead."
                )
            satisfied = self.is_satisfied(
                conn, entry=match, interval=interval, kind=kind
            )
            logger.debug(
                "contract: %s upstream %r (%s, upstream kind=%s) for %s window %s",
                "SATISFIED" if satisfied else "BLOCKED",
                name,
                kind,
                match.kind,
                downstream,
                interval,
            )
            if not satisfied:
                blockers.append(f"upstream {name!r} ({kind})")
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
            kind = src.contract.value
            lookup = self._suffix_upstream_name(name)
            if assume_applied and _overlay_covers(
                assume_applied.get(lookup), kind, interval
            ):
                after.append(f"{name} ({kind})")
                continue
            match = self.lookup_model(conn, lookup, self._library_schema())
            if match is not None and self.is_satisfied(
                conn, entry=match, interval=interval, kind=kind
            ):
                continue  # already applied
            blocked.append(f"{name} ({kind})")
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
        kind: str | None = None,
    ) -> bool:
        """Is the upstream satisfied for `interval` (the downstream's
        window, or None for whole-table / existence work)?

        The check is keyed by `kind`. When the downstream declared an
        `UpstreamContract`, `kind` is the contract's level value (the
        downstream's explicit expectation); otherwise it falls back to the
        upstream's own registered `entry.kind`. Either way:

        * library-only rows (`state_schema` / `state_table` NULL — older
          images, or models registered without state tracking): presence
          in the library is the proof. Always satisfied.
        * `kind == 'temporal'` / `'window'` / `'through'`:
          look for an `applied` row whose window matches or fully
          encapsulates `interval`. A daily-cadence upstream thus covers an
          hourly downstream without coordination.
        * `kind == 'timeless'`: the upstream has a single NULL-window
          existence row — satisfied iff that row is `applied` (the view
          exists / the whole table has been loaded). The downstream window
          is irrelevant.

        WINDOW / THROUGH against a TIMELESS upstream is rejected upstream of
        here (`is_upstream_satisfied_live`), so by the time a contract level
        reaches this function it is shape-compatible.

        If the upstream's state table doesn't exist yet (registered but
        never bootstrapped), returns False."""
        kind = kind or entry.kind

        # 'exists' (UpstreamContract.EXISTS): reaching here means the upstream
        # was found in the library — that registration is all this gate
        # requires, regardless of state. A windowless way to depend on a
        # temporal model.
        if kind == "exists":
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
        if kind == "whole":
            query = sql.SQL(
                "SELECT 1 WHERE EXISTS ("
                "  SELECT 1 FROM {schema}.{table} WHERE status = 'applied'"
                ") AND NOT EXISTS ("
                "  SELECT 1 FROM {schema}.{table} WHERE status <> 'applied'"
                ")"
            ).format(
                schema=sql.Identifier(entry.state_schema),
                table=sql.Identifier(entry.state_table),
            )
            return conn.execute(query).fetchone() is not None

        if kind == "timeless":
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
        if kind == "through":
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
