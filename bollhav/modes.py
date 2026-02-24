from enum import Enum


class WriteMode(Enum):
    APPEND = "APPEND"
    TRUNCATE_INSERT = "TRUNCATE_INSERT"
    OVERWRITE_INSERT = "OVERWRITE_INSERT"
    MERGE = "MERGE"
    VIEW = "VIEW"


class ModelType(Enum):
    TABLE = "TABLE"
    VIEW = "VIEW"
