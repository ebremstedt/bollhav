from __future__ import annotations

from typing import TYPE_CHECKING

from ._base import _PostgresStateBase

if TYPE_CHECKING:
    from bollhav.model.intervals import TZInterval


class Locks(_PostgresStateBase):
    def _interval_lock_key(self, interval: "TZInterval | None") -> str:
        """Hash key for an interval-scoped lock: identifies the specific
        `(model, since, until)` triple. Used by `@execute_lifecycle` so two
        workers can race on the same model but different intervals
        without conflict. A monolithic / view row has a NULL window, so its
        key collapses to a single per-model `…|oneshot` slot."""
        if interval is None:
            return f"{self.model.target.full_name}|oneshot"
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
        state = self.model.state
        if state is None:
            from bollhav.postgres.messages.error import StateActivationRequiredError

            raise StateActivationRequiredError(self.model.target.full_name)
        if state.allow_concurrent_runs:
            return False
        if not self.try_acquire_lock():
            from bollhav.model.state import ModelLockedError

            raise ModelLockedError(
                f"another pipeline holds the lock on "
                f"{self.model.target.full_name!r} — concurrent runs of "
                f"the same model are not allowed"
            )
        return True
