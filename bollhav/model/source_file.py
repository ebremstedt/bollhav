from dataclasses import dataclass
from pathlib import Path

from bollhav.model.source import Source


@dataclass
class SourceFile(Source):
    path: Path
    encoding: str | None = None
    separator: str | None = None
    infer_schema_length: int | None = None
    remove_top_rows: int = 0
    archive_folder: Path | None = None
    dateformat: str | None = None
    file_ending: str | None = None
