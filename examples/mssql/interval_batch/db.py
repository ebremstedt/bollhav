"""Connection helpers for the local Dockerized MSSQL.

The full ODBC DSN lives in the `BOLLHAV_MSSQL_DSN` env var (see README).
`connect(database=...)` swaps the DATABASE token so we can bootstrap the
`bollhav` database from a `master` connection before it exists.
"""

from __future__ import annotations

import os
import re

import pyodbc

DSN_ENV_VAR = "BOLLHAV_MSSQL_DSN"


def _dsn(database: str | None = None) -> str:
    try:
        dsn = os.environ[DSN_ENV_VAR]
    except KeyError:
        raise SystemExit(
            f"{DSN_ENV_VAR} is not set. See README.md — export the ODBC DSN "
            f"for the local Docker MSSQL first."
        )
    if database is not None:
        dsn = re.sub(r"DATABASE=[^;]*", f"DATABASE={database}", dsn)
    return dsn


def connect(database: str | None = None, autocommit: bool = False) -> pyodbc.Connection:
    return pyodbc.connect(_dsn(database), autocommit=autocommit)


def ensure_database(name: str) -> None:
    """CREATE DATABASE if missing — run against `master` with autocommit
    (CREATE DATABASE can't run inside a multi-statement transaction)."""
    with connect(database="master", autocommit=True) as conn:
        conn.cursor().execute(f"IF DB_ID(N'{name}') IS NULL CREATE DATABASE [{name}]")
