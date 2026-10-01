"""Connect the pyiceberg Catalog — the Iceberg pipeline's `data_conn`.

Where a Postgres pipeline opens a psycopg connection from a DSN, an Iceberg
pipeline builds a `Catalog`. The catalog is what pyiceberg asks "where is
table X's metadata?"; the data itself is Parquet under `warehouse`.

Locally that is `SqlCatalog` over a sqlite file — zero infrastructure. Against
a Hive Metastore only this function changes, the models and the lifecycle do
not:

    from pyiceberg.catalog import load_catalog
    load_catalog(
        name,
        type="hive",
        uri="thrift://metastore:9083",
        warehouse="s3://lake/warehouse",
        # + s3.endpoint / s3.access-key-id / s3.secret-access-key
    )

and `pip install 'pyiceberg[hive]'` for the thrift client (or `hive-kerberos`).
"""

import os
from pathlib import Path

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog


def connect_catalog(name: str) -> Catalog:
    warehouse = Path(os.environ.get("ICEBERG_WAREHOUSE", "warehouse")).resolve()
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        name,
        uri=f"sqlite:///{warehouse / 'catalog.db'}",
        warehouse=f"file://{warehouse}",
    )
