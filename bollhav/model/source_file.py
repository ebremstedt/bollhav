from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class SourceFile:
    name: str
    path: Path
    encoding: str | None = None
    separator: str | None = None
    infer_schema_length: int | None = None
    remove_top_rows: int = 0
    archive_folder: Path | None = None
    dateformat: str | None = None
    file_ending: str | None = None
    extra: dict = field(default_factory=dict)
