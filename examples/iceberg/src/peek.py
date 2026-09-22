"""Read back what the pipeline wrote — the Iceberg-side `SELECT count(*)`.

Lists every table in the local catalog with its row count and snapshot count
(one snapshot per chunk written), then the three oldest rows.

    python src/peek.py
"""

import os
from pathlib import Path

import polars as pl

from catalog import connect_catalog


def main() -> None:
    catalog = connect_catalog(name="local")
    for namespace in sorted(catalog.list_namespaces()):
        for identifier in sorted(catalog.list_tables(namespace)):
            table = catalog.load_table(identifier)
            df = pl.from_arrow(table.scan().to_arrow())
            assert isinstance(df, pl.DataFrame)
            snapshots = len(table.metadata.snapshots)
            print(f"\n{'.'.join(identifier)}: {df.height} rows, {snapshots} snapshots")
            if df.height:
                with pl.Config(fmt_str_lengths=90, tbl_width_chars=160):
                    print(df.sort("_data_modified").head(3))


if __name__ == "__main__":
    os.chdir(Path(__file__).resolve().parent.parent)
    main()
