from dataclasses import dataclass
from datetime import datetime


@dataclass
class Schema:
    name: str = ""
    suffix: str = ""
    suffix_appendix: str | None = "%y%W"

    @property
    def resolved(self) -> str:
        if self.suffix:
            s = self.name + "_" + self.suffix
            if self.suffix_appendix:
                s = s + "_" + datetime.now().strftime(self.suffix_appendix)
            return s
        return self.name
