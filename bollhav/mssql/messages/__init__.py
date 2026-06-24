"""Centralized message text for the MSSQL backend.

Mirrors `bollhav.model.messages`: error prose lives here, not at the call
site. Currently only `error.py` (typed exception classes) — call sites import
a class and `raise` it. Re-exports the `error.py` public API so the classes are
importable from `bollhav.mssql.messages`."""

from bollhav.mssql.messages.error import *  # noqa: F401,F403
from bollhav.mssql.messages.error import __all__ as _e

__all__ = list(_e)
