"""raw — schemaless ingestion into Iceberg, one model per (hospital, view).

The same shape as `models/raw.py` in intelligence-src-entity-raw, with the
target swapped from Postgres to Iceberg:

  * `Database.ICEBERG` picks the Iceberg adapter (`IcebergData` for assets,
    `write_dataframes` for rows). Which *catalog backend* holds the table
    pointers — sqlite here, Hive Metastore in production — is a connection
    detail decided in `catalog.py`, not in the model.
  * `Target.catalog` is the pyiceberg catalog NAME. `main.py` connects one
    catalog per distinct name and hands it to the lifecycle as `data_conn`.
  * The columns are `IcebergColumn`s. `payload` is a JSON string (Iceberg has
    no JSONB), and `_data_modified` carries `partition_on=True` so a
    RECREATE_PARTITION run would know which window to overwrite.
  * No `staging=` — Iceberg commits are atomic snapshot swaps, so the adapter
    rejects a staging table as pointless.
  * No `state=` — bollhav state lives in Postgres, and this example runs with
    no database at all. In production keep `state=State()` and pass a
    separate Postgres `state_conn` next to the catalog `data_conn`.
"""

from datetime import datetime, timezone

from bollhav.iceberg import IcebergColumn, IcebergType
from bollhav.model import (
    Batch,
    ChunkFlex,
    Contract,
    Database,
    Materialization,
    Model,
    Source,
    SourceModel,
    Target,
    Temporality,
    TimeChunking,
    WriteMode,
)

from blueprints import source_hospitals, source_tables


# fmt: off
def create_model(*, table: str, hospital: str) -> Model:
    target_schema = f"intelligence_raw_{hospital.lower()}"
    source_full_name = f"viewreader.{table}"

    return Model(
        target=Target(
            name=table,
            schema=target_schema,
            catalog="local",
            database=Database.ICEBERG,
            write_mode=WriteMode.APPEND,
            columns=[
                IcebergColumn(name="_data_modified", data_type=IcebergType.TIMESTAMPTZ, partition_on=True, nullable=False),
                IcebergColumn(name="_metadata_modified", data_type=IcebergType.TIMESTAMPTZ, nullable=False),
                IcebergColumn(name="payload", data_type=IcebergType.STRING, nullable=True),
            ],
            # Where the read function finds the hospital — the target schema
            # name encodes it too, but a plain field beats parsing.
            extra={"hospital": hospital},
        ),
        upstream=[
            Source(
                name=source_full_name,
                type=SourceModel(
                    schema="viewreader",
                    partitioned_by="TimestampRead",
                ),
            ),
        ],
        contract=Contract(begin=datetime(2024, 1, 1, tzinfo=timezone.utc)),
        batching=Batch(
            time=TimeChunking(
                chunk="@monthly",
                latest_window="@daily",
                flexibility=ChunkFlex(floor_chunk="@daily"),
            ),
            size=25,
        ),
        temporality=Temporality.TEMPORAL,
        materialization=Materialization.TABLE,
    )
# fmt: on


models: list[Model] = [
    create_model(table=table, hospital=hospital)
    for hospital in source_hospitals
    for table in source_tables
]
