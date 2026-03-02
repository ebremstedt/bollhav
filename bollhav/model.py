from datetime import datetime
from typing import Callable
from bollhav.database import Database
from bollhav.implementations.postgres import PostgresColumn
from bollhav.implementations.parquet import ParquetColumn
from bollhav.modes import WriteMode, ModelType
from bollhav.batching import infer_batch_size
from bollhav.sorting import sort_columns


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
        column_sorting: Callable | None = sort_columns,
        partitioned_by: list[str] | None = None,
        begin: datetime | None = None,
        end: datetime | None = None,
        retries: int | None = None,
        lookback: int | None = None,
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
        if begin is not None and begin.tzinfo is None:
            raise ValueError("begin must be timezone-aware")
        if end is not None and end.tzinfo is None:
            raise ValueError("end must be timezone-aware")

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
        self.column_sorting = column_sorting
        self.partitioned_by = partitioned_by
        self.begin = begin
        self.end = end
        self.retries = retries
        self.lookback = lookback
        self.batch_size = infer_batch_size(cron) if cron else None
        self.sensitive = (
            any(getattr(c, "sensitive", False) for c in columns) if columns else False
        )

        if self.columns and self.column_sorting:
            col_names = [c.name for c in self.columns]
            sorted_names = self.column_sorting(col_names)
            name_to_col = {c.name: c for c in self.columns}
            self.columns = [name_to_col[n] for n in sorted_names]

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
            f"begin={self.begin.isoformat() if self.begin else None!r}, "
            f"end={self.end.isoformat() if self.end else None!r}, "
            f"retries={self.retries!r}, "
            f"lookback={self.lookback!r}, "
            f"sensitive={self.sensitive}, "
            f"extra={self.extra!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Model):
            return NotImplemented
        return self.__dict__ == other.__dict__
