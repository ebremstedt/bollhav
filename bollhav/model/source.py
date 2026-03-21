from dataclasses import dataclass


@dataclass
class Source:
    name: str
    catalog: str | None = None
    schema: str | None = None
    partitioned_by: str | None = None
    dsn_env_var: str | None = None
    query: str | None = None
    extra: dict | None = None
