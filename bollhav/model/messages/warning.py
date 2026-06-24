"""Centralized `logger.warning` emitters for state-lifecycle events.

Each function owns its message (and the rationale behind it) so the call
sites stay free of warning prose — they just `warn_<thing>(logger, ...)`.
Sibling of `error.py` (raised, fatal conditions) and `info.py` (routine
log lines): this one is for non-fatal warnings logged in passing. The caller
passes its own `logger` so each record stays namespaced to the emitting
module."""

from __future__ import annotations

import logging


def warn_torch_deleted(logger: logging.Logger, full_name: str, deleted: int) -> None:
    """`STATE_MODE=torch` wiped the model's state rows before re-prefill."""
    logger.warning(
        "state: TORCH deleted %d row(s) for %s — re-prefilling from scratch",
        deleted,
        full_name,
    )


def warn_mark_applied(logger: logging.Logger, full_name: str, count: int) -> None:
    """`STATE_MARK_APPLIED` stamped intervals applied without running them —
    an assertion that the data was loaded out of band, not a verification."""
    logger.warning(
        "STATE_MARK_APPLIED: stamped %d interval(s) applied for %s "
        "WITHOUT running it",
        count,
        full_name,
    )


def warn_lock_release_failed(logger: logging.Logger, full_name: str) -> None:
    """The model advisory lock couldn't be released explicitly. Best-effort:
    the lock is released when the session ends too, so this isn't fatal —
    but a persistent failure means runs serialize harder than intended, so
    it's surfaced (with traceback) rather than swallowed silently."""
    logger.warning(
        "state: failed to release model lock for %s (will release on session end)",
        full_name,
        exc_info=True,
    )


def warn_interval_lock_release_failed(
    logger: logging.Logger, interval, full_name: str
) -> None:
    """The per-interval lock couldn't be released explicitly after the unit
    finished. Best-effort: the lock is released when the session ends too, so
    this isn't fatal — but a persistent failure means intervals stay locked
    against other workers longer than intended, so it's surfaced (with
    traceback) rather than swallowed silently."""
    logger.warning(
        "state: failed to release interval lock %s for %s "
        "(will release on session end)",
        interval,
        full_name,
        exc_info=True,
    )
