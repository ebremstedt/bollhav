"""Typed, named exceptions for the model runtime, centralized in `error.py` so
call sites just `raise <SpecificError>(...)` instead of carrying message prose.

Only errors are centralized — `logger.info`/`warning`/`debug` lines are written
inline at their call sites (they read fine in context and don't benefit from the
indirection). The error classes are re-exported here for convenience."""

from __future__ import annotations

from bollhav.model.messages.error import *  # noqa: F401,F403
from bollhav.model.messages.error import __all__ as _error_all

__all__ = list(_error_all)
