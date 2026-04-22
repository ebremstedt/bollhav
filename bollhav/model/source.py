from dataclasses import dataclass


@dataclass
class Source:
    """Base class for source kinds (table, file, ...).

    Carries the one field every source has: `name`. Use `SourceTable` or
    `SourceFile` to construct a concrete source."""

    name: str
