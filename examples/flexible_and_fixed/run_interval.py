"""One unit of work, wrapped by `@execute_lifecycle`.

The hook brackets this: gate on applied → take the lock → check the model's
upstream **contracts** → mark running → run this body → mark applied (or
record failure). For staging models it also creates the staging table, then
merges it into the target and drops it around the body — so this body just
`read()`s and `write()`s.

This is where the contract semantics show up: `daily_report` gates
`ENCAPSULATE` on the FLEXIBLE `clean_events`, and `audit` gates `EXACT` on the
FIXED `raw_events`. A gate the upstream can't satisfy marks the interval
`blocked`.
"""

from __future__ import annotations

import os
import time

from bollhav.model import ModelRun, execute_lifecycle
from bollhav.postgres import write
from mock_read import read


@execute_lifecycle
def run_interval(run: ModelRun, interval, data_conn, state_conn=None) -> None:
    delay = float(os.environ.get("DEMO_DELAY", "0"))
    if delay:
        time.sleep(delay)
    df_gen = read(run, interval)
    write(conn=data_conn, run=run, df_gen=df_gen)
