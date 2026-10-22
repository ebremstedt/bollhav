# Backends

A model's `Target` names a `Database`, and bollhav writes to it with that
backend's columns and write path. Three are supported.

| Backend | Install | Columns | What it does |
|---|---|---|---|
| **Postgres** | `pip install bollhav` | `PostgresColumn` from `bollhav.postgres` | The full set: data writes, staging tables, and [state](../model/state.md), which also makes it the home of the library and the GUI |
| **MSSQL** | `pip install "bollhav[mssql]"` | `MssqlColumn` from `bollhav.mssql` | Data writes with staging and indexes. No state, so an external orchestrator or your own loop drives every run; see [Orchestration](../articles/orchestration.md) |
| **Iceberg** | `pip install "bollhav[iceberg]"` | `IcebergColumn` from `bollhav.iceberg` | Data writes through pyiceberg to any catalog pyiceberg can open; views through Trino. No staging or indexes, since Iceberg has neither |

Each backend has its own column dataclass with the database's own types,
and the same `Target` and [write modes](../model/modes.md) on top. The column
classes are documented in their modules:
[postgres/columns.py](../../bollhav/postgres/columns.py),
[mssql/columns.py](../../bollhav/mssql/columns.py),
[iceberg/columns.py](../../bollhav/iceberg/columns.py).
