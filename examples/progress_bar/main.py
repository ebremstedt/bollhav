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

from bollhav.model import Model, load_models, name_width_for
from execute import execute


@load_models
def main(models: list[Model], debug: bool) -> None:
    if models:
        execute.set_name_width(name_width_for(models))

    for model in models:
        intervals = model.infer_intervals()
        execute.set_total(len(intervals))
        for interval in intervals:
            execute(model=model, since=interval.since, until=interval.until)

    execute.finish()


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
