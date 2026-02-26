import polars as pl
from bollhav.database import Database
from bollhav.implementations.postgres import PostgresColumn
from bollhav.implementations.parquet import ParquetColumn
from bollhav.modes import WriteMode, ModelType
from bollhav.batching import infer_batch_size


class Model:
    def __init__(
        self,
        name: str,
        source_entity: str,
        table: str = "",
        schema: str = "",
        database: Database | None = None,
        columns: list[PostgresColumn | ParquetColumn] | None = None,
        model_type: ModelType = ModelType.TABLE,
        write_mode: WriteMode = WriteMode.APPEND,
        tags: list[str] | None = None,
        cron: str | None = None,
        enabled: bool = True,
        debug: bool = False,
        description: str | None = None,
        source_dsn: str | None = None,
        source_query: str | None = None,
        partitioned_by: list[str] | None = None,
        **kwargs,
    ):
        if model_type == ModelType.VIEW and write_mode != WriteMode.VIEW:
            raise ValueError("ModelType.VIEW must use WriteMode.VIEW")
        if model_type == ModelType.TABLE and write_mode == WriteMode.VIEW:
            raise ValueError("ModelType.TABLE cannot use WriteMode.VIEW")
        if database is not None and columns is None:
            raise ValueError("columns must be set when database is provided")
        if columns is not None and database is None:
            raise ValueError("database must be set when columns is provided")
        if partitioned_by and columns:
            col_names = {c.name for c in columns}
            invalid = [p for p in partitioned_by if p not in col_names]
            if invalid:
                raise ValueError(
                    f"partitioned_by references unknown columns: {invalid}"
                )

        self.name = name
        self.source_entity = source_entity
        self.table = table
        self.schema = schema
        self.database = database
        self.columns = columns
        self.model_type = model_type
        self.write_mode = write_mode
        self.tags = tags
        self.cron = cron
        self.enabled = enabled
        self.debug = debug
        self.description = description
        self.source_dsn = source_dsn
        self.source_query = source_query
        self.partitioned_by = partitioned_by
        self.batch_size = infer_batch_size(cron) if cron else None
        self.sensitive = (
            any(getattr(c, "sensitive", False) for c in columns) if columns else False
        )

        for key, val in kwargs.items():
            if callable(val):
                kwargs[key] = val(
                    **{k: v for k, v in kwargs.items() if not callable(v)}
                )
        self.extra = kwargs

    def __repr__(self) -> str:
        return (
            f"Model("
            f"name={self.name!r}, "
            f"source_entity={self.source_entity!r}, "
            f"table={self.table!r}, "
            f"schema={self.schema!r}, "
            f"database={self.database}, "
            f"columns={self.columns!r}, "
            f"model_type={self.model_type}, "
            f"write_mode={self.write_mode}, "
            f"tags={self.tags!r}, "
            f"cron={self.cron!r}, "
            f"enabled={self.enabled}, "
            f"debug={self.debug}, "
            f"description={self.description!r}, "
            f"source_dsn={self.source_dsn!r}, "
            f"source_query={self.source_query!r}, "
            f"partitioned_by={self.partitioned_by!r}, "
            f"sensitive={self.sensitive}, "
            f"extra={self.extra!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Model):
            return NotImplemented
        return self.__dict__ == other.__dict__
