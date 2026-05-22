from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class TargetSchema:
    name: str = ""
    suffix: str = ""
    suffix_appendix: str | None = "%y%V"
    _base_name: str = field(init=False, default="", repr=False, compare=False)

    def __post_init__(self) -> None:
        self._base_name = self.name

    @property
    def resolved(self) -> str:
        if self.suffix:
            s = self._base_name + "_" + self.suffix
            if self.suffix_appendix:
                s = (
                    s
                    + "_"
                    + datetime.now(tz=timezone.utc).strftime(self.suffix_appendix)
                    + "_"
                )
            return s
        return self._base_name
