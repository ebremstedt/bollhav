from enum import Enum


class Writer(Enum):
    """What executes a model's DDL and writes. Only Iceberg has a choice:
    pyiceberg through the catalog for tables, or Trino for views, which
    pyiceberg's SQL catalog cannot create. Postgres and MSSQL have one client
    each, so they leave it unset."""

    PYICEBERG = "PYICEBERG"
    TRINO = "TRINO"
