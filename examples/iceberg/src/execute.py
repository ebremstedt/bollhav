"""The execute functions — the same two-hook shape as intelligence-src-entity-raw.

    execute_model     @model_lifecycle     one model: assets (namespace + table)
      execute_interval  @execute_lifecycle   one interval: read -> transform -> write

`data_conn` is the pyiceberg Catalog. `@model_lifecycle` sees
`Database.ICEBERG` and uses `IcebergData` to create the namespace and table
before the first interval runs, so the writer only ever appends.
"""

from __future__ import annotations

import logging
from datetime import datetime

from bollhav.iceberg import write
from bollhav.model import (
    ModelRun,
    Source,
    SourceModel,
    execute_lifecycle,
    model_lifecycle,
)
from bollhav.model.intervals import TZInterval
from pyiceberg.catalog import Catalog

from read import read_source
from transform import transform

logger = logging.getLogger(__name__)


@model_lifecycle
def execute_model(
    run: ModelRun,
    data_conn: Catalog,
    metadata_modified: datetime,
    state_conn=None,
) -> None:
    (source,) = run.model.upstream
    assert isinstance(source.type, SourceModel)

    for interval in run.intervals:
        execute_interval(
            run=run,
            interval=interval,
            source=source,
            data_conn=data_conn,
            metadata_modified=metadata_modified,
            state_conn=state_conn,
        )


@execute_lifecycle
def execute_interval(
    *,
    run: ModelRun,
    interval: TZInterval | None,
    source: Source,
    data_conn: Catalog,
    metadata_modified: datetime,
    state_conn=None,
) -> None:
    since = interval.since if interval else None
    until = interval.until if interval else None

    assert isinstance(source.type, SourceModel)
    assert run.model.batching is not None
    assert run.model.target.extra is not None

    df_gen = read_source(
        table=run.model.target.name,
        hospital=run.model.target.extra["hospital"],
        since=since,
        until=until,
        fetch_size=run.model.batching.size,
    )

    df_gen = transform(
        df_gen=df_gen,
        data_modified_column=source.type.partitioned_by,
        metadata_modified=metadata_modified,
    )

    write(
        conn=data_conn,
        run=run,
        df_gen=df_gen,
        since=since,
        until=until,
    )
