"""Per-chunk write handler. The @progress_bar decorator counts each call
as one chunk and renders the line based on `model.batching.mode` — so a
row-mode model shows "(rows)" next to its name, an interval model shows
"(interval)". Tag overrides like `r_row_<N>:` flip the label too, because
`apply_pipe` bakes tag-driven reload overrides into `batching` before the
progress bar reads it."""

import polars as pl
from bollhav.model import Model, progress_bar
from mock_write import write_chunk


@progress_bar
def execute(
    model: Model, df_chunk: pl.DataFrame, chunk_index: int, total_chunks: int
) -> None:
    write_chunk(model, df_chunk, chunk_index, total_chunks)
