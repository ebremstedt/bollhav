"""Connection helpers — MSSQL for data, Postgres for state.

The model's *data* lives in MSSQL (`BOLLHAV_MSSQL_DSN`) and its *state* in
Postgres (`STATE_DSN`); the run opens one of each and threads them as
`data_conn` (pyodbc) and `state_conn` (psycopg).

  * `connect(database=...)` — the MSSQL data connection. Defaults to
    `autocommit=False`: the staging apply (DELETE/INSERT/MERGE + DROP) runs
    as one transaction and commits explicitly, so the per-interval apply
    stays atomic. The asset-DDL helpers commit themselves too.
  * `state_connect()` — the Postgres state connection, `autocommit=True`
    (the state machine brackets each transition in its own transaction).
"""

from __future__ import annotations

import os
import re

import psycopg
import pyodbc

DSN_ENV_VAR = "BOLLHAV_MSSQL_DSN"
STATE_DSN_ENV_VAR = "STATE_DSN"


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


def state_connect() -> psycopg.Connection:
    """The Postgres connection the state machine runs on. Must be autocommit —
    each state transition commits in its own `conn.transaction()`."""
    try:
        dsn = os.environ[STATE_DSN_ENV_VAR]
    except KeyError:
        raise SystemExit(
            f"{STATE_DSN_ENV_VAR} is not set. This model is state-tracked, and "
            f"state lives in Postgres — export a Postgres DSN (see README.md)."
        )
    return psycopg.connect(dsn, autocommit=True)
