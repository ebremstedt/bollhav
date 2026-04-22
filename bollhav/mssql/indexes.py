from dataclasses import dataclass, field
from bollhav.model.database import DatabaseIndex


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
            raise ValueError(f"Index {self.name!r}: columns must be non-empty")
        overlap = set(self.columns) & set(self.included)
        if overlap:
            raise ValueError(
                f"Index {self.name!r}: columns and included must be disjoint, "
                f"got overlap: {sorted(overlap)}"
            )
