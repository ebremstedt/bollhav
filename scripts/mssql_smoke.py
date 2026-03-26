"""
Smoke test for the bollhav MSSQL implementation.

Requirements:
  pip install pyodbc polars
  brew install unixodbc
  brew tap microsoft/mssql-release && brew install msodbcsql18

Start server:
  docker compose up -d

Run:
  python scripts/mssql_smoke.py
"""

import pyodbc
import polars as pl

from bollhav.model.database import Database
from bollhav.model.model import Model
from bollhav.model.schema import Schema
from bollhav.model.target import Target
from bollhav.model.write_modes import WriteMode
from bollhav.mssql import MssqlColumn, MssqlType, write

DSN = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=localhost,1433;"
    "Database=master;"
    "UID=sa;"
    "PWD=Bollhav123!;"
    "TrustServerCertificate=yes;"
)

COLUMNS = [
    MssqlColumn(name="id", data_type=MssqlType.INT, nullable=False, unique=True),
    MssqlColumn(name="name", data_type=MssqlType.NVARCHAR, length=100),
    MssqlColumn(name="score", data_type=MssqlType.FLOAT),
]


def make_model(write_mode: WriteMode) -> Model:
    return Model(
        name="smoke_test",
        target=Target(
            name="smoke_test",
            schema=Schema(name="dbo"),
            database=Database.MSSQL,
            columns=COLUMNS,
            write_mode=write_mode,
        ),
    )


def df_gen(df: pl.DataFrame):
    yield df


def main() -> None:
    conn = pyodbc.connect(DSN, autocommit=False)
    print("Connected to MSSQL.")

    # --- truncate_write ---
    df1 = pl.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "score": [9.5, 7.2, 8.8]}
    )
    model_tw = make_model(WriteMode.TRUNCATE_TABLE_INSERT)
    print("\n[truncate_write] Writing initial 3 rows...")
    write(conn=conn, model=model_tw, df_gen=df_gen(df1))

    cursor = conn.cursor()
    rows = cursor.execute("SELECT * FROM [dbo].[smoke_test] ORDER BY id").fetchall()
    print(f"  Rows after truncate_write: {[tuple(r) for r in rows]}")

    print("\n[truncate_write] Overwriting with 2 rows...")
    df2 = pl.DataFrame({"id": [10, 20], "name": ["X", "Y"], "score": [1.0, 2.0]})
    write(conn=conn, model=model_tw, df_gen=df_gen(df2), create_if_missing=False)
    rows = cursor.execute("SELECT * FROM [dbo].[smoke_test] ORDER BY id").fetchall()
    print(f"  Rows after second truncate_write: {[tuple(r) for r in rows]}")

    # --- merge ---
    df3 = pl.DataFrame(
        {"id": [10, 30], "name": ["X_updated", "Z"], "score": [99.0, 3.0]}
    )
    model_m = make_model(WriteMode.UPSERT_NO_DELETE)
    print("\n[merge] Upserting (id=10 updated, id=30 inserted)...")
    write(conn=conn, model=model_m, df_gen=df_gen(df3), create_if_missing=False)
    rows = cursor.execute("SELECT * FROM [dbo].[smoke_test] ORDER BY id").fetchall()
    print(f"  Rows after merge: {[tuple(r) for r in rows]}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
