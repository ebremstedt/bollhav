from enum import Enum


class WriteMode(Enum):
    """How *data* is written to a table target. Views have no write mode —
    a view is identified by `ModelType.VIEW` and created by the lifecycle
    (`PostgresData.create_or_replace_view`), not written here."""

    APPEND = "APPEND"  # Simply append rows each time
    UPSERT_NO_DELETE = "UPSERT_NO_DELETE"  # Update and/or insert, but do not delete
    RECREATE_PARTITION = (
        "RECREATE_PARTITION"  # Update and/or insert, but _do_ delete first
    )
