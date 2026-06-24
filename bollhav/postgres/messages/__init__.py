"""Centralized message text for the Postgres backend.

Mirrors `bollhav.model.messages`: error prose lives here, not at the call
sites, so every Postgres-backend message is greppable in one place and carries
a hover docstring. Re-exports the `error.py` public API so the exception
classes are importable from `bollhav.postgres.messages`."""

from bollhav.postgres.messages.error import *  # noqa: F401,F403
from bollhav.postgres.messages.error import __all__ as _e

__all__ = list(_e)
