import polars as pl
from bollhav.database import Database
from bollhav.postgres import PostgresColumn
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
        columns: list[PostgresColumn] | None = None,
        model_type: ModelType = ModelType.TABLE,
        write_mode: WriteMode = WriteMode.APPEND,
        tags: list[str] | None = None,
        cron: str | None = None,
        enabled: bool = True,
        debug: bool = False,
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
        self.batch_size = infer_batch_size(cron) if cron else None

        for key, val in kwargs.items():
            if callable(val):
                kwargs[key] = val(
                    **{k: v for k, v in kwargs.items() if not callable(v)}
                )
        self.extra = kwargs

    def __repr__(self) -> str:
        return (
            f"Model(name={self.name!r}, source_entity={self.source_entity!r}, "
            f"table={self.table!r}, schema={self.schema!r}, "
            f"database={self.database}, columns={self.columns!r}, "
            f"model_type={self.model_type}, write_mode={self.write_mode}, "
            f"tags={self.tags!r}, cron={self.cron!r}, "
            f"batch_size={self.batch_size}, enabled={self.enabled}, "
            f"debug={self.debug}, extra={self.extra!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Model):
            return NotImplemented
        return self.__dict__ == other.__dict__
