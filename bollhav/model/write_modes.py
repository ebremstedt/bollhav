from enum import Enum


class WriteMode(Enum):
    APPEND = "APPEND"  # Simply append rows each time
    UPSERT_NO_DELETE = "UPSERT_NO_DELETE"  # Update and/or insert, but do not delete
    RECREATE_PARTITION = (
        "RECREATE_PARTITION"  # Update and/or insert, but _do_ delete first
    )
    VIEW = "VIEW"  # Simply update view in place
