from datetime import datetime
import polars as pl
from bollhav.model import Model


def write(model: Model, df: pl.DataFrame, since: datetime, until: datetime) -> None:
    # Uncomment to inspect the data being written:
    # print(f"\n  [{model.target.write_mode.value}] {model.target.full_name} ({since.date()} → {until.date()})")
    # print(df)
    pass
