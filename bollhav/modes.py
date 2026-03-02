from enum import Enum


class WriteMode(Enum):
    APPEND = "APPEND" # Simply append rows each time
    TRUNCATE_INSERT = "TRUNCATE_INSERT" # Fully load a table each time
    UPDATE_INSERT = "UPDATE_INSERT" # Update and/or insert, but do not delete
    OVERWRITE_INSERT = "OVERWRITE_INSERT" # Update and/or insert, but _do_ delete first
    MERGE = "MERGE" # Wait until postgres 15
    VIEW = "VIEW" # Simply update view in place


class ModelType(Enum):
    TABLE = "TABLE"
    VIEW = "VIEW"
