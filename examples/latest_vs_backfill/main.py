"""Entry point for the latest_vs_backfill example.

This example does not read or write data. Its purpose is to show how the
pipe's mode (latest vs backfill) changes which intervals get resolved for
the same model.

For each matched model we:
  1. Apply the pipe config (this is what sets runtime_override from env).
  2. Call infer_intervals() — this is where the mode matters.
  3. Print the first/last chunk and total count.

Run the same folder twice — once with LATEST_ENABLED=true, once with
BACKFILL_ENABLED=true — and compare the output. See README.md.
"""

import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from bollhav.pipe import with_pipe_config, PipeConfig
from bollhav.model import match_models


@with_pipe_config
def main(pipe: PipeConfig) -> None:
    for model in match_models(folder="src/models", tags=pipe.tags):
        model.apply_pipe(pipe)
        intervals = model.infer_intervals()

        print(f"\n{model.target.full_name}")
        print(f"  batch_expression : {model.batching.interval.expression}")
        print(f"  bounds           : {model.bounds.begin} → {model.bounds.end}")
        print(f"  chunks returned  : {len(intervals)}")
        if intervals and intervals[0] is not None:
            first, last = intervals[0], intervals[-1]
            print(f"  first chunk      : {first.since} → {first.until}")
            print(f"  last chunk       : {last.since} → {last.until}")
    print()


if __name__ == "__main__":
    main()
