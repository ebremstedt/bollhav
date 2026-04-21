"""Entry point for the progress_bar example.

Three INTERVAL models with backfill windows of 4, 12, and 24 hourly
chunks respectively. Each chunk sleeps 150ms so the spinner is visible
when running with PROGRESS_BAR=batch.

Run from the repo root:

  PROGRESS_BAR=batch TAGS="[facts]" USE_SCHEMA_SUFFIX=false \\
    BACKFILL_ENABLED=true python examples/progress_bar/main.py

Try also PROGRESS_BAR=model and PROGRESS_BAR=minimal for quieter output.
"""

import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from bollhav.pipe import with_pipe_config, PipeConfig
from bollhav.model import match_models
from execute import execute


@with_pipe_config
def main(pipe: PipeConfig) -> None:
    matched = match_models(folder="src/models", tags=pipe.tags)
    if matched:
        execute.set_name_width(max(len(m.target.full_name) for m in matched) + 12)

    for model in matched:
        model.apply_pipe(pipe)
        intervals = model.infer_intervals()
        execute.set_total(len(intervals))
        for interval in intervals:
            execute(model=model, since=interval.since, until=interval.until)

    execute.finish()


if __name__ == "__main__":
    main()
