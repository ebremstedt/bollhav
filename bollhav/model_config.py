from datetime import datetime, timedelta
from typing import Callable
from bollhav.database import Database
from bollhav.postgres.columns import PostgresColumn
from bollhav.parquet.columns import ParquetColumn
from bollhav.modes import WriteMode, ModelType
from bollhav.batching import infer_batch_size
from bollhav.sorting import sort_columns


def snake_to_tags(s: str) -> set[str]:
    return set(s.split("_"))


class ModelConfig:
    """
    Configuration for a data model, defining source, target, write behavior, and metadata.

    Parameters
    ----------
    name : str
        Unique model identifier. Added to tags by default.
    source_entity : str
        Source table/view name to read from.
    table : str
        Target table name.
    schema : str
        Target schema name. Added to tags by default.
    schema_suffix : str
        Suffix appended to schema when use_suffix=True.
    database : Database | None
        Target database. Required if columns is provided.
    columns : list[PostgresColumn | ParquetColumn]
        Column definitions. Required if database is provided.
    model_type : ModelType
        TABLE or VIEW. VIEW requires WriteMode.VIEW.
    write_mode : WriteMode
        How data is written (APPEND, UPDATE_INSERT, VIEW, etc).
        UPDATE_INSERT requires at least one column with unique=True.
    tags : set[str] | None
        Additional tags. name, schema, and "all" are added automatically.
    cron : str | None
        Cron expression used to infer batch_size.
    enabled : bool
        Whether the model is active.
    debug : bool
        Enables debug mode.
    description : str | None
        Human-readable description.
    target_dsn_env_var : str | None
        Env var name for the target DSN.
    source_dsn_env_var : str | None
        Env var name for the source DSN.
    source_query : str | None
        Optional override query for reading from source.
    column_sorting : Callable | None
        Function to sort column order. Defaults to sort_columns.
    partitioned_by : str | None
        Column name used for batch partitioning. Must exist in columns if set.
    partitioned_by_index : bool
        Auto-set to True when partitioned_by is provided.
    begin : datetime | None
        Start of the valid time range. Must be UTC-aware if tz_aware=True.
    end : datetime | None
        End of the valid time range. Must be UTC-aware if tz_aware=True.
    retries : int | None
        Number of retry attempts on failure.
    lookback : int | None
        Lookback window in batch units.
    name_add_to_tags : bool
        Whether to add name to tags.
    schema_add_to_tags : bool
        Whether to add schema to tags.
    model_gets_all_tag : bool
        Whether to add "all" to tags.
    unkebab_schema_for_tags : bool
        Whether to split schema on underscores and add parts as tags.
    use_schema_suffix : bool
        Whether to append schema_suffix to schema.
    tz_aware : bool
        Enforces UTC-awareness on begin/end if True.
    **kwargs
        Extra fields stored in self.extra. Callable values are resolved at init time.

    Raises
    ------
    ValueError
        On invalid combinations of model_type/write_mode, missing columns/database,
        unknown partitioned_by column, non-UTC begin/end, or UPDATE_INSERT without unique columns.
    """

    def __init__(
        self,
        name: str,
        source_entity: str,
        table: str = "",
        schema: str = "",
        schema_suffix: str = "",
        database: Database | None = None,
        columns: list[PostgresColumn | ParquetColumn] = [],
        model_type: ModelType = ModelType.TABLE,
        write_mode: WriteMode = WriteMode.APPEND,
        tags: set[str] | None = None,
        cron: str | None = None,
        enabled: bool = True,
        debug: bool = False,
        description: str | None = None,
        target_dsn_env_var: str | None = None,
        source_dsn_env_var: str | None = None,
        source_query: str | None = None,
        column_sorting: Callable | None = sort_columns,
        partitioned_by: str | None = None,
        partitioned_by_index: bool = False,
        begin: datetime | None = None,
        end: datetime | None = None,
        retries: int | None = None,
        lookback: int | None = None,
        name_add_to_tags: bool = True,
        schema_add_to_tags: bool = True,
        model_gets_all_tag: bool = True,
        unkebab_schema_for_tags: bool = True,
        use_schema_suffix: bool = True,
        tz_aware: bool = True,
        **kwargs,
    ):
        if model_type == ModelType.VIEW and write_mode != WriteMode.VIEW:
            raise ValueError("ModelType.VIEW must use WriteMode.VIEW")
        if model_type == ModelType.TABLE and write_mode == WriteMode.VIEW:
            raise ValueError("ModelType.TABLE cannot use WriteMode.VIEW")
        if database is not None and len(columns) == 0:
            raise ValueError("columns must be set when database is provided")
        if len(columns) > 0 and database is None:
            raise ValueError("database must be set when columns is provided")
        if partitioned_by and columns:
            col_names = {c.name for c in columns}
            if partitioned_by not in col_names:
                raise ValueError(
                    f"Partitioned_by references unknown column: {partitioned_by!r}"
                )
        if tz_aware:
            if begin is not None and (
                begin.tzinfo is None or begin.utcoffset() != timedelta(0)
            ):
                raise ValueError("begin must be UTC-aware")
            if end is not None and (
                end.tzinfo is None or end.utcoffset() != timedelta(0)
            ):
                raise ValueError("end must be UTC-aware")

        self.name = name

        self.schema = schema
        self.schema_suffix = schema_suffix
        self.use_schema_suffix = use_schema_suffix
        if self.use_schema_suffix:
            self.schema_with_suffix = self.schema + self.schema_suffix

        self.source_entity = source_entity
        self.table = table
        self.database = database
        self.columns = columns
        self.model_type = model_type
        self.write_mode = write_mode

        self.unkebab_schema_for_tags = unkebab_schema_for_tags
        self.tags = set(tags) if tags else set()
        self.name_add_to_tags = name_add_to_tags
        if self.name_add_to_tags:
            self.tags.add(self.name)
        self.schema_add_to_tags = schema_add_to_tags
        if self.schema_add_to_tags:
            self.tags.add(self.schema)
        self.every_model_gets_all_tag = model_gets_all_tag
        if self.every_model_gets_all_tag:
            self.tags.add("all")
        if self.unkebab_schema_for_tags:
            self.tags.update(snake_to_tags(s=self.schema))

        self.cron = cron
        self.enabled = enabled
        self.debug = debug
        self.description = description
        self.target_dsn_env_var = target_dsn_env_var
        self.source_dsn_env_var = source_dsn_env_var
        self.source_query = source_query
        self.column_sorting = column_sorting
        self.partitioned_by = partitioned_by
        self.partitioned_by_index = partitioned_by is not None
        self.begin = begin
        self.end = end
        self.retries = retries
        self.lookback = lookback
        self.tz_aware = tz_aware
        self.batch_size = infer_batch_size(cron) if cron else None
        self.sensitive = (
            any(getattr(c, "sensitive", False) for c in columns) if columns else False
        )
        self.unique_columns = (
            [c for c in columns if getattr(c, "unique", False)] if columns else []
        )

        if write_mode == WriteMode.UPDATE_INSERT and not self.unique_columns:
            raise ValueError(
                "WriteMode.UPDATE_INSERT requires at least one column with unique=True"
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
            f"source_dsn={self.source_dsn_env_var!r}, "
            f"source_query={self.source_query!r}, "
            f"partitioned_by={self.partitioned_by!r}, "
            f"begin={self.begin.isoformat() if self.begin else None!r}, "
            f"end={self.end.isoformat() if self.end else None!r}, "
            f"retries={self.retries!r}, "
            f"lookback={self.lookback!r}, "
            f"sensitive={self.sensitive}, "
            f"unique_columns={self.unique_columns!r}, "
            f"tz_aware={self.tz_aware}, "
            f"extra={self.extra!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ModelConfig):
            return NotImplemented
        return self.__dict__ == other.__dict__
