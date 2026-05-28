from dataclasses import dataclass, field


@dataclass
class SourceTable:
    name: str
    catalog: str | None = None
    schema: str | None = None
    partitioned_by: str | None = None
    dsn_env_var: str | None = None
    query: str | None = None
    infer_schema_length: int | None = None
    """Passed to polars as infer_schema_length. The maximum number of rows to scan
    for schema inference. If set to None, the full data may be scanned (this can be
    slow). This parameter only applies if the input data is a sequence or generator
    of rows; other input is read as-is."""
    extra: dict = field(default_factory=dict)
