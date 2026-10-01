"""Iceberg backend.

`IcebergColumn` / `IcebergType` (the model-config side) import without
pyiceberg installed. Everything that touches a catalog or table is resolved
on first access, so defining models never needs the optional `iceberg` extra
(the same way Postgres users never install pyodbc).
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from bollhav.iceberg.columns import (
    IcebergColumn,
    IcebergPrimaryKeyNotNullableError,
    IcebergType,
)

if TYPE_CHECKING:  # real types for the checker; at runtime these go via __getattr__
    from bollhav.iceberg.data import (
        IcebergData,
        IcebergStagingNotSupportedError,
        IcebergViewWithoutBodyError,
        IcebergViewsNotSupportedError,
        IcebergWriterConnectionError,
        IcebergWrongWriterError,
    )
    from bollhav.iceberg.modes import (
        OverwriteRequiresPartitionColumnError,
        UpsertDuplicateKeysError,
        UpsertRequiresMergeKeysError,
        append,
        overwrite,
        upsert,
    )
    from bollhav.iceberg.schema import (
        NotIcebergColumnsError,
        arrow_schema,
        iceberg_schema,
    )
    from bollhav.iceberg.write_modes import (
        MissingDataFrameError,
        RecreatePartitionRequiresWindowError,
        UnhandledWriteModeError,
        write,
        write_dataframes,
    )

# name -> submodule that defines it. Each of these imports pyiceberg / pyarrow.
_LAZY = {
    "IcebergData": "data",
    "IcebergStagingNotSupportedError": "data",
    "IcebergViewWithoutBodyError": "data",
    "IcebergViewsNotSupportedError": "data",
    "IcebergWriterConnectionError": "data",
    "IcebergWrongWriterError": "data",
    "NotIcebergColumnsError": "schema",
    "arrow_schema": "schema",
    "iceberg_schema": "schema",
    "OverwriteRequiresPartitionColumnError": "modes",
    "UpsertDuplicateKeysError": "modes",
    "UpsertRequiresMergeKeysError": "modes",
    "append": "modes",
    "overwrite": "modes",
    "upsert": "modes",
    "MissingDataFrameError": "write_modes",
    "RecreatePartitionRequiresWindowError": "write_modes",
    "UnhandledWriteModeError": "write_modes",
    "write": "write_modes",
    "write_dataframes": "write_modes",
}

__all__ = [
    "IcebergColumn",
    "IcebergData",
    "IcebergPrimaryKeyNotNullableError",
    "IcebergStagingNotSupportedError",
    "IcebergType",
    "IcebergViewWithoutBodyError",
    "IcebergViewsNotSupportedError",
    "IcebergWriterConnectionError",
    "IcebergWrongWriterError",
    "MissingDataFrameError",
    "NotIcebergColumnsError",
    "OverwriteRequiresPartitionColumnError",
    "RecreatePartitionRequiresWindowError",
    "UnhandledWriteModeError",
    "UpsertDuplicateKeysError",
    "UpsertRequiresMergeKeysError",
    "append",
    "arrow_schema",
    "iceberg_schema",
    "overwrite",
    "upsert",
    "write",
    "write_dataframes",
]


def __getattr__(name: str) -> Any:
    try:
        submodule = _LAZY[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    value = getattr(import_module(f"{__name__}.{submodule}"), name)
    globals()[name] = value  # cache so later lookups bypass __getattr__
    return value


def __dir__() -> list[str]:
    return sorted(__all__)
