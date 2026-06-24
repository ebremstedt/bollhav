"""Postgres-backed state store, split by concern.

`PostgresState` (in `state`) is assembled from per-concern building blocks:
`library`, `state_table`, `satisfaction`, `locks`, over `_base` (connection / naming).
This module is only the public re-export surface — every existing
`from bollhav.postgres.state import …` keeps working.
"""

from ._base import LibraryEntry
from ._ddl import ERRORS_TABLE, LIBRARY_SCHEMA, LIBRARY_TABLE
from ._naming import _CONTEXT_CAP, _devowel, _name_digest, state_table_name
from .state import PostgresState, drop_environment

__all__ = [
    "PostgresState",
    "LibraryEntry",
    "LIBRARY_SCHEMA",
    "LIBRARY_TABLE",
    "ERRORS_TABLE",
    "state_table_name",
    "drop_environment",
    "_CONTEXT_CAP",
    "_devowel",
    "_name_digest",
]
