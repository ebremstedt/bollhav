"""Source rows -> the raw layer's three columns. Lifted unchanged from
intelligence-src-entity-raw: the transform has no idea whether the target is
Postgres or Iceberg. The only difference downstream is that `payload` lands
in a STRING column instead of JSONB."""

from collections.abc import Generator
from datetime import datetime

import polars as pl

UTC_DATETIME = pl.Datetime(time_unit="us", time_zone="UTC")


def _json_safe(*, df: pl.DataFrame) -> pl.DataFrame:
    """Make every column encodable by ``struct.json_encode``. Only Binary needs
    handling; non-finite floats are already written as null."""
    binary_cols = [name for name, dtype in df.schema.items() if dtype == pl.Binary]
    if not binary_cols:
        return df
    return df.with_columns(
        [pl.col(name).bin.encode("hex").alias(name) for name in binary_cols]
    )


def _to_utc(*, col: pl.Series) -> pl.Series:
    if col.dtype == pl.Utf8:
        col = col.str.to_datetime(time_unit="us")

    if isinstance(col.dtype, pl.Datetime) and col.dtype.time_zone is None:
        col = col.dt.replace_time_zone(
            time_zone="Europe/Stockholm",
            ambiguous="latest",
            non_existent="null",
        )

    return col.dt.convert_time_zone(time_zone="UTC")


def to_payload_df(
    *,
    df: pl.DataFrame,
    metadata_modified: datetime,
    data_modified_column: str | None,
) -> pl.DataFrame:
    n: int = len(df)

    if data_modified_column is not None and data_modified_column in df.columns:
        data_modified = _to_utc(col=df[data_modified_column]).cast(UTC_DATETIME)
    else:
        data_modified = pl.Series(values=[None] * n, dtype=UTC_DATETIME)

    payloads = (
        _json_safe(df=df)
        .select(pl.struct(pl.all()).struct.json_encode().alias("payload"))
        .get_column("payload")
    )

    return pl.DataFrame(
        data={
            "_data_modified": data_modified,
            "_metadata_modified": pl.Series(
                values=[metadata_modified] * n, dtype=UTC_DATETIME
            ),
            "payload": payloads,
        }
    )


def transform(
    *,
    df_gen: Generator[pl.DataFrame, None, None],
    data_modified_column: str | None,
    metadata_modified: datetime,
) -> Generator[pl.DataFrame, None, None]:
    for df in df_gen:
        yield to_payload_df(
            df=df,
            metadata_modified=metadata_modified,
            data_modified_column=data_modified_column,
        )
