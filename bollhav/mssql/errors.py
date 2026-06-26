"""Shared base for errors raised by the MSSQL backend.

Holds only `MssqlError`, the abstract base every MSSQL backend error
subclasses (a `ValueError`, so callers catching `ValueError` keep working
unchanged). The concrete, raised error classes now live in the module that
raises each one, so an exception's `__module__` points at its origin.
Sibling-style module to `bollhav.model.errors`."""

from __future__ import annotations


class MssqlError(ValueError):
    """Base for an MSSQL backend error.

    Subclasses `ValueError`, so existing `except ValueError` handlers keep
    catching every MSSQL config/usage error unchanged."""


__all__ = [
    "MssqlError",
]
