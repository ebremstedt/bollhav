import polars as pl
from bollhav.database import Database
from bollhav.postgres import PostgresColumn
from bollhav.modes import WriteMode, ModelType
from bollhav.batching import infer_batch_size


class Model:
    """
    Represents a data model definition for a pipeline target.

    Args:
        name:           Unique identifier for the model.
        source_entity:  Source table or view to read from.
        table:          Destination table name.
        schema:         Destination schema name.
        database:       Target database type. Required if columns is set.
        columns:        Column definitions. Required if database is set.
        model_type:     TABLE or VIEW. Defaults to TABLE.
        write_mode:     How to write data. VIEW requires ModelType.VIEW.
        tags:           Optional labels for filtering.
        cron:           Cron expression. Infers batch_size automatically.
        enabled:        Whether the model is active. Defaults to True.
        debug:          Enables debug mode. Defaults to False.
        description:    Optional human-readable description of the model.
        **kwargs:       Extra metadata. Callable values are resolved with
                        non-callable kwargs as arguments.

    Raises:
        ValueError: If model_type and write_mode are incompatible.
        ValueError: If database is set without columns or vice versa.
    """

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
        description: str | None = None,
        source_dsn: str | None = None,
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
        self.description = description
        self.source_dsn = source_dsn
        self.batch_size = infer_batch_size(cron) if cron else None

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
            f"extra={self.extra!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Model):
            return NotImplemented
        return self.__dict__ == other.__dict__
