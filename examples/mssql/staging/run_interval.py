"""One unit of work, wrapped by `@execute_lifecycle`.

Because the model stages, the hook brackets this body with the staging
lifecycle: create the per-interval staging table → run this body (which
bulk-inserts each chunk into it via `bollhav.mssql.write`) → MERGE
staging into the target and drop it, atomically. So this body just
`read()`s and `write()`s — the table lifecycle is the hook's job.

`bollhav.mssql.write` in staged mode only lands chunks; it keys the
staging table on `model.run_id`, the same id the hook used to create it.
"""

from __future__ import annotations

from bollhav.model import ModelRun, execute_lifecycle
from bollhav.mssql import write
from mock_read import read


@execute_lifecycle
def run_interval(run: ModelRun, interval, data_conn, state_conn=None) -> None:
    df_gen = read(run, interval)
    write(conn=data_conn, run=run, df_gen=df_gen)
