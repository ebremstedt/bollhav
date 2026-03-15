from dataclasses import dataclass


@dataclass
class Source:
    name: str
    schema: str | None = None
    dsn_env_var: str | None = None
    query: str | None = None
