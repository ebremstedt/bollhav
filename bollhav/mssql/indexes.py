from dataclasses import dataclass, field
from bollhav.model.database import DatabaseIndex
from bollhav.mssql.errors import MssqlError


# ── errors ──────────────────────────────────────────────────────────


class EmptyIndexColumnsError(MssqlError):
    """An `Index` was declared with no key columns. An index needs at least
    one column to order/seek on, so an empty `columns` list is invalid."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Index {name!r}: columns must be non-empty")


class OverlappingIndexColumnsError(MssqlError):
    """An `Index`'s key `columns` and `included` columns overlap. Included
    columns are stored at the leaf only and must be disjoint from the key,
    so the same column can't appear in both."""

    def __init__(self, name: str, overlap: list[str]) -> None:
        super().__init__(
            f"Index {name!r}: columns and included must be disjoint, "
            f"got overlap: {overlap}"
        )


@dataclass
class MssqlIndex(DatabaseIndex):
    """
    Defines a non-clustered index on an MSSQL table.

    Inherits name from DatabaseIndex.

    Args:
        columns:  Key columns, in index order.
        unique:   Emit as UNIQUE index.
        filter:   Raw SQL predicate for a filtered index (WHERE ...). None = no filter.
        included: Non-key columns stored in the leaf (INCLUDE) for covering queries.
    """

    columns: list[str] = field(default_factory=list)
    unique: bool = False
    filter: str | None = None
    included: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.columns:
            raise EmptyIndexColumnsError(self.name)
        overlap = set(self.columns) & set(self.included)
        if overlap:
            raise OverlappingIndexColumnsError(self.name, sorted(overlap))
