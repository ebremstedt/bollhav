from dataclasses import dataclass
from datetime import datetime



# ── errors ──


class NaiveContractBeginError(ValueError):
    """A `Contract.begin` was set to a timezone-naive datetime. Contract
    bounds are absolute instants, so a set `begin` must be timezone-aware."""

    def __init__(self) -> None:
        super().__init__("contract.begin must be timezone-aware")


class NaiveContractEndError(ValueError):
    """A `Contract.end` was set to a timezone-naive datetime. Contract bounds
    are absolute instants, so a set `end` must be timezone-aware."""

    def __init__(self) -> None:
        super().__init__("contract.end must be timezone-aware")


@dataclass
class Contract:
    """A model's time bounds — the outer `begin`/`end` window its runs may
    target. Both ends are optional and, when set, must be timezone-aware."""

    begin: datetime | None = None
    end: datetime | None = None

    def __post_init__(self) -> None:
        if self.begin is not None and self.begin.tzinfo is None:
            raise NaiveContractBeginError()
        if self.end is not None and self.end.tzinfo is None:
            raise NaiveContractEndError()
