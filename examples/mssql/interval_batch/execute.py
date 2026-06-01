"""Per-interval execute.

One connection per interval; `bollhav.mssql.write` dispatches on the
model's write mode (APPEND here). No `@state` decorator — MSSQL state
coordination isn't implemented, so the model carries no `State()`.
"""

from __future__ import annotations

from datetime import datetime

from bollhav.model import Model
from bollhav.mssql import write

from db import connect
from read import read


def execute(model: Model, since: datetime, until: datetime) -> None:
    df_gen = read(model, since, until)
    with connect() as conn:
        write(conn=conn, model=model, df_gen=df_gen, since=since, until=until)
