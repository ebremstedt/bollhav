"""Centralized `logger.debug` emitters for state-lifecycle events.

Each function owns its message so the call sites stay free of log prose —
they just `debug_<thing>(logger, ...)`. Sibling of `error.py`, `warning.py`,
and `info.py`: this one is for the fine-grained, per-unit diagnostics that
are off by default (gate decisions, lock contention, mode defaulting). The
caller passes its own `logger` so each record stays namespaced to the
emitting module."""

from __future__ import annotations

import logging


def debug_gate_skipped_applied(
    logger: logging.Logger, interval, full_name: str
) -> None:
    """One interval was skipped because state already has it `applied` — the
    idempotency gate doing its job. The common no-op on a re-run."""
    logger.debug("state: gate skipped applied %s for %s", interval, full_name)


def debug_interval_lock_held(
    logger: logging.Logger, interval, full_name: str
) -> None:
    """An interval was skipped because another worker holds its lock — normal
    under concurrency; this run leaves it to the other worker."""
    logger.debug(
        "state: lock held by another worker, skipping %s on %s",
        interval,
        full_name,
    )


def debug_defaulting_to_backfill(logger: logging.Logger) -> None:
    """No run mode was set, so the run defaults to backfill. Hints how to opt
    into latest-tick mode instead (`LATEST_ENABLED=true`)."""
    logger.debug(
        "no run mode set — defaulting to backfill; "
        "set LATEST_ENABLED=true for latest-tick mode"
    )
