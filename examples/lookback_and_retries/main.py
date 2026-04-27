"""Entry point for the lookback_and_retries example.

Demonstrates two separate Batch features working together:

  - `lookback=2` extends every interval two hours backwards, picking up
    late-arriving rows that landed after the previous run.
  - `retries=3` is honoured by our execute() function, which retries
    each chunk on failure up to that many times.

The mock read deliberately fails the first two times each chunk is
attempted. retries=3 gives us 4 attempts total, so every chunk
eventually succeeds on attempt 3. Watch the console output.
"""

import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from bollhav.model import Model, load_models
from execute import execute


@load_models
def main(models: list[Model], debug: bool) -> None:
    for model in models:
        intervals = model.infer_intervals()

        print(f"\n{model.target.full_name}")
        print(f"  interval_expression : {model.batching.interval.expression}")
        print(f"  lookback            : {model.batching.interval.lookback}")
        print(f"  retries             : {model.batching.retries}")
        print(f"  bounds              : {model.bounds.begin} → {model.bounds.end}")
        print(f"  chunks returned     : {len(intervals)}")
        if intervals and intervals[0] is not None:
            print(
                f"  first chunk         : {intervals[0].since} → {intervals[0].until}"
            )
            print(
                f"  last chunk          : {intervals[-1].since} → {intervals[-1].until}"
            )
        print()

        for iv in intervals:
            if iv is None:
                continue
            print(f"  chunk {iv.since:%H:%M} → {iv.until:%H:%M}")
            execute(model=model, since=iv.since, until=iv.until)
    print()


if __name__ == "__main__":
    main()
