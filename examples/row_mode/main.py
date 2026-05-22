"""Entry point for the row_mode example.

Demonstrates a model configured with `Batch(mode=ChunkMode.ROW)` — work
is chunked by row count instead of time interval. The dispatcher below
branches on `model.batching.mode` and uses the row-batching code path
for ROW-mode models. INTERVAL models fall through to the standard
infer_intervals() path.

Run from the repo root:

  TAGS="[r:events]" USE_SCHEMA_SUFFIX=false python examples/row_mode/main.py

The `r:` tag prefix is required: ROW-mode models can only be reloaded.
"""

import os

from bollhav.model import ChunkMode, Model, load_models, name_width_for
from execute import execute
from mock_read import read_all


@load_models
def main(models: list[Model], debug: bool) -> None:
    if models:
        execute.set_name_width(name_width_for(models))

    for model in models:
        if model.batching.mode is ChunkMode.ROW:
            _run_row_mode(model)
        else:
            _run_interval_mode(model)

    execute.finish()


def _run_row_mode(model) -> None:
    if not model.directives.reload:
        raise ValueError(
            f"Model {model.target.full_name!r} is ROW-mode — it can only be "
            f'reloaded. Re-run with the `r:` tag prefix, e.g. TAGS="[r:events]"'
        )
    df = read_all(model)
    size = model.batching.row.batch_size
    total = (len(df) + size - 1) // size
    execute.set_total(total)
    for i, start in enumerate(range(0, len(df), size), start=1):
        execute(
            model=model,
            df_chunk=df.slice(start, size),
            chunk_index=i,
            total_chunks=total,
        )


def _run_interval_mode(model) -> None:
    intervals = model.infer_intervals()
    execute.set_total(len(intervals))
    df = read_all(model)
    for i, _ in enumerate(intervals, start=1):
        execute(model=model, df_chunk=df, chunk_index=i, total_chunks=len(intervals))


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
