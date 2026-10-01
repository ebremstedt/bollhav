"""Build `warehouse/lake.duckdb` — one DuckDB view per Iceberg table — so a SQL
client that speaks DuckDB (DataGrip, the duckdb CLI, DBeaver) can browse the
warehouse like a database.

Each view is `SELECT * FROM iceberg_scan('<table dir>', version='?')`. The
`version='?'` makes DuckDB resolve the LATEST metadata file at query time, so
the views stay current across pipeline runs; rerun this only when tables are
added. Any session that queries them must allow that lookup first:

    SET unsafe_enable_version_guessing = true;

(DataGrip: Data Source Properties -> Options -> Startup script.)

The file is built next to the real one and swapped in with a rename, so it can
be rebuilt while DataGrip still holds the old one open — reconnect to see the
new views.

    python src/duckdb_views.py
"""

import os
from pathlib import Path

import duckdb

from catalog import connect_catalog


def main() -> None:
    catalog = connect_catalog(name="local")
    warehouse = Path(os.environ.get("ICEBERG_WAREHOUSE", "warehouse")).resolve()
    target = warehouse / "lake.duckdb"
    staging = warehouse / "lake.duckdb.tmp"
    staging.unlink(missing_ok=True)

    con = duckdb.connect(str(staging))
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("SET unsafe_enable_version_guessing = true")

    views = 0
    for namespace in sorted(catalog.list_namespaces()):
        schema = ".".join(namespace)
        con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        for identifier in sorted(catalog.list_tables(namespace)):
            table = catalog.load_table(identifier)
            location = table.location().removeprefix("file://")
            con.execute(
                f'CREATE VIEW "{schema}"."{identifier[-1]}" AS '
                f"SELECT * FROM iceberg_scan('{location}', version='?')"
            )
            views += 1
    con.close()

    os.replace(staging, target)
    print(f"{target}: {views} views")


if __name__ == "__main__":
    os.chdir(Path(__file__).resolve().parent.parent)
    main()
