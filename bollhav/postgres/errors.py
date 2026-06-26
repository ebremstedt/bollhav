"""Shared base for errors raised by the Postgres backend.

Sibling of `bollhav.model.errors`, but scoped to the Postgres package
(writes, staging, columns, and state coordination). This module now holds only
the shared abstract base — each leaf error lives with the code that raises it,
so the exception's `__module__` reveals its origin.

Most leaf errors share `PostgresError(ValueError)` as their base, so callers
catching `ValueError` (and the existing tests) keep working unchanged. A few
can't and subclass their builtin directly (e.g. the `create_indexes` guard is a
`RuntimeError`, and the write-mode dispatch fall-throughs are
`NotImplementedError`) — those live in their home modules too."""

from __future__ import annotations


class PostgresError(ValueError):
    """Base for a Postgres-backend error that callers catch as `ValueError`.

    Subclasses `ValueError`, so existing `except ValueError` handlers keep
    catching every backend error unchanged."""


__all__ = [
    "PostgresError",
]
