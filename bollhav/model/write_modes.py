from enum import Enum


class WriteMode(Enum):
    APPEND = "APPEND"  # Simply append rows each time
    RECREATE_TABLE_INSERT = "RECREATE_TABLE_INSERT"  # Drop and create table before fully loading it
    TRUNCATE_TABLE_INSERT = "TRUNCATE_TABLE_INSERT"  # Fully load a table each time
    UPDATE_INSERT = "UPDATE_INSERT"  # Update and/or insert, but do not delete
    RECREATE_PARTITION = "RECREATE_PARTITION"  # Update and/or insert, but _do_ delete first
    VIEW = "VIEW"  # Simply update view in place
