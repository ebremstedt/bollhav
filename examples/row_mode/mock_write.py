"""Mock sink — prints what would have been written instead of touching
a real database. Sleeps a little per chunk so the progress bar is
visible on screen for the demo. Chunk-level prints only show at
PROGRESS_BAR=batch — at lower verbosity levels the per-model line from
the @progress_bar decorator is enough."""

import time

import polars as pl
from bollhav.model.progress_bar import ProgressLevel, get_progress_level

_PER_CHUNK_DELAY_SECONDS = 0.5


def write_chunk(model, df: pl.DataFrame, chunk_index: int, total_chunks: int) -> None:
    time.sleep(_PER_CHUNK_DELAY_SECONDS)
    if get_progress_level() is ProgressLevel.BATCH:
        print(
            f"  wrote chunk {chunk_index:>2}/{total_chunks} "
            f"({len(df):>4} rows) to {model.target.full_name}"
        )
