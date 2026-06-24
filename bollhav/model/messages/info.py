"""Centralized `logger.info` emitters for state-lifecycle events.

Each function owns its message so the call sites stay free of log prose —
they just `info_<thing>(logger, ...)`. Sibling of `error.py` (raised, fatal
conditions) and `warning.py` (non-fatal warnings): this one is for the
routine, expected log lines a healthy run emits (schema migrations, curfew
skips, environment teardown). The caller passes its own `logger` so each
record stays namespaced to the emitting module."""

from __future__ import annotations

import logging


# --- state-table migrations (additive ALTERs on the shared bollhav tables) ---


def info_state_temporality_added(
    logger: logging.Logger, schema: str, table: str
) -> None:
    """Logged once when the state table gains its `temporality` column via an
    in-place ALTER. The column defaults to `'temporal'` so images running an
    older schema keep writing valid rows during the rollover."""
    logger.info(
        "state: migrated %s.%s — added temporality column (default 'temporal' "
        "so older images keep writing temporal rows)",
        schema,
        table,
    )


def info_state_notnull_relaxed(
    logger: logging.Logger, schema: str, table: str, col: str
) -> None:
    """Logged when a `NOT NULL` constraint is dropped from a state-table
    window column, so monolithic / view models (which carry a NULL window)
    can store their rows. An additive, backward-compatible migration."""
    logger.info(
        "state: migrated %s.%s — relaxed NOT NULL on %s "
        "(monolithic / view rows carry a NULL window)",
        schema,
        table,
        col,
    )


def info_library_model_type_added(
    logger: logging.Logger, schema: str, table: str
) -> None:
    """Logged once when the library table gains its `model_type` column via an
    in-place ALTER. Defaults to `'TABLE'` so images on the older schema keep
    registering models during the rollover."""
    logger.info(
        "library: migrated %s.%s — added model_type column "
        "(default 'TABLE' so older images can keep writing)",
        schema,
        table,
    )


def info_library_temporality_added(
    logger: logging.Logger, schema: str, table: str
) -> None:
    """Logged once when the library table gains its `temporality` column via an
    in-place ALTER. Defaults to `'temporal'` so older images keep registering
    temporal models during the rollover."""
    logger.info(
        "library: migrated %s.%s — added temporality column (default 'temporal' "
        "so older images keep registering temporal models)",
        schema,
        table,
    )


def info_library_notnull_relaxed(
    logger: logging.Logger, schema: str, table: str, col: str
) -> None:
    """Logged when a `NOT NULL` constraint is dropped from a library-table
    column, so view / library-only rows (which store NULL there) are
    accepted. An additive, backward-compatible migration."""
    logger.info(
        "library: migrated %s.%s — relaxed NOT NULL on %s "
        "(view / library-only rows store NULL here)",
        schema,
        table,
        col,
    )


# --- state teardown ---------------------------------------------------------


def info_state_cleared(logger: logging.Logger, full_name: str, schema: str) -> None:
    """Logged by `clear_state` after every state row for one model is deleted
    (the table + library registration are kept). The targeted, single-model
    reset — lighter than dropping the whole environment."""
    logger.info("state: cleared all state for %s (schema %s)", full_name, schema)


def info_dropped_environment(
    logger: logging.Logger, target_schemas: list[str], state_schemas: list[str]
) -> None:
    """Logged after `drop_environment` tears down a whole suffixed test
    environment — both the target data schemas and the state schemas. The
    nuclear local-testing cleanup; reports exactly what was dropped."""
    logger.info(
        "dropped environment: target schemas %s, state schemas %s",
        target_schemas,
        state_schemas,
    )


def info_state_disabled(logger: logging.Logger, count: int) -> None:
    """Logged under `STATE_DISABLED` after state + staging are cleared for the
    matched models. Confirms how many models ran with state tracking off."""
    logger.info(
        "STATE_DISABLED: state + staging cleared on %d matched model(s)",
        count,
    )


# --- curfew skips -----------------------------------------------------------


def info_curfew_skip_model(logger: logging.Logger, full_name: str) -> None:
    """Logged when a whole model run is skipped because its curfew is active.
    The model's intervals stay `pending` and are picked up on a later run."""
    logger.info("curfew: skipping model %s (stays pending)", full_name)


def info_curfew_skip_interval(
    logger: logging.Logger, window: str, full_name: str
) -> None:
    """Logged when a single interval is skipped because the model's curfew is
    active for it. That window stays `pending` for a later run; the rest of
    the model may still proceed."""
    logger.info("curfew: skipping %s for %s (stays pending)", window, full_name)
