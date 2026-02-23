import polars as pl
from enum import Enum
from croniter import croniter


class WriteMode(Enum):
    APPEND = "APPEND"
    TRUNCATE_INSERT = "TRUNCATE_INSERT"
    OVERWRITE_INSERT = "OVERWRITE_INSERT"
    MERGE = "MERGE"
    VIEW = "VIEW"


class ModelType(Enum):
    TABLE = "TABLE"
    VIEW = "VIEW"


class BatchSize(Enum):
    YEARLY = "YEARLY"
    MONTHLY = "MONTHLY"
    WEEKLY = "WEEKLY"
    DAILY = "DAILY"
    HOURLY = "HOURLY"
    SUBHOURLY = "SUBHOURLY"


def _infer_batch_size(cron: str) -> BatchSize | None:
    if not croniter.is_valid(cron):
        raise ValueError(f"Invalid cron expression: {cron!r}")

    minute, hour, day, month, weekday = cron.split()

    if month != "*":
        return BatchSize.YEARLY
    if day != "*" and weekday == "*":
        return BatchSize.MONTHLY
    if weekday != "*":
        return BatchSize.WEEKLY
    if hour != "*":
        return BatchSize.DAILY
    if minute == "0":
        return BatchSize.HOURLY
    return None


class Model:
    def __init__(
        self,
        name: str,
        source_table: str,
        columns: dict[str, pl.DataType],
        destination_table: str = "",
        destination_schema: str = "",
        model_type: ModelType = ModelType.TABLE,
        write_mode: WriteMode = WriteMode.APPEND,
        tags: list[str] | None = None,
        cron: str | None = None,
        **kwargs,
    ):
        if model_type == ModelType.VIEW and write_mode != WriteMode.VIEW:
            raise ValueError(f"ModelType.VIEW must use WriteMode.VIEW")
        if model_type == ModelType.TABLE and write_mode == WriteMode.VIEW:
            raise ValueError(f"ModelType.TABLE cannot use WriteMode.VIEW")

        self.name = name
        self.source_table = source_table
        self.destination_table = destination_table
        self.destination_schema = destination_schema
        self.model_type = model_type
        self.write_mode = write_mode
        self.tags = tags
        self.columns = columns
        self.cron = cron
        self.batch_size = _infer_batch_size(cron) if cron else None

        for key, val in kwargs.items():
            if callable(val):
                kwargs[key] = val(
                    **{k: v for k, v in kwargs.items() if not callable(v)}
                )
        self.extra = kwargs

    def __repr__(self) -> str:
        return (
            f"Model(name={self.name!r}, source_table={self.source_table!r}, "
            f"destination_table={self.destination_table!r}, destination_schema={self.destination_schema!r}, "
            f"model_type={self.model_type}, write_mode={self.write_mode}, "
            f"tags={self.tags!r}, columns={self.columns!r}, cron={self.cron!r}, "
            f"batch_size={self.batch_size}, extra={self.extra!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Model):
            return NotImplemented
        return self.__dict__ == other.__dict__
