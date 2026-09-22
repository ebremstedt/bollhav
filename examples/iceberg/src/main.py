"""Entry point — discovery + the per-model loop.

    @load_models  ->  main(runs)                    # match models by TAGS
        for run in runs:
            execute_model(run, catalog, ...)        # @model_lifecycle
                for interval in run.intervals:
                    execute_interval(run, interval, ...)  # @execute_lifecycle

The only thing that differs from intelligence-src-entity-raw's main.py is the
connection: a pyiceberg Catalog instead of a psycopg connection. Each model's
`Target.catalog` names which catalog it lives in, so one is connected per
distinct name.
"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from bollhav.model import ModelRun, load_models
from pyiceberg.catalog import Catalog

from catalog import connect_catalog
from execute import execute_model

STOCKHOLM = ZoneInfo("Europe/Stockholm")


class StockholmFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, tz=STOCKHOLM)
        return dt.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]


def setup_logging(*, debug: bool) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(
        StockholmFormatter(fmt="%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if debug else logging.INFO)
    root.handlers.clear()
    root.addHandler(handler)


@load_models
def main(runs: list[ModelRun], debug: bool) -> None:
    setup_logging(debug=debug)
    metadata_modified = datetime.now(tz=timezone.utc)

    catalogs: dict[str, Catalog] = {}
    for run in runs:
        name = run.model.target.catalog
        assert name is not None
        catalog = catalogs.setdefault(name, connect_catalog(name=name))
        execute_model(
            run=run,
            data_conn=catalog,
            metadata_modified=metadata_modified,
        )


if __name__ == "__main__":
    # Run from the example root so `@load_models` finds `src/models` and the
    # sqlite warehouse lands in `examples/iceberg/warehouse/`.
    os.chdir(Path(__file__).resolve().parent.parent)
    main()
