"""Entry point for the reload_flag example.

Shows how the `r:` prefix on a tag expression flips a matched model into
reload mode. Reload uses the model's `bounds.begin` / `bounds.end` as the
interval, overriding both latest and backfill.

The match_models call sets `model.runtime_override.reload = True` when the
tag expression matched with `r:`. apply_pipe() then honours that flag —
latest is forced off and the bounds are the source of truth.

Run it with different TAGS values (and optionally LATEST_ENABLED) to see
how reload behaves.
"""

import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from bollhav.pipe import with_pipe_config, PipeConfig
from bollhav.model import match_models


@with_pipe_config
def main(pipe: PipeConfig) -> None:
    for model in match_models(folder="src/models", tags=pipe.tags):
        reload = model.runtime_override.reload
        model.apply_pipe(pipe)
        intervals = model.infer_intervals()

        print(f"\n{model.target.full_name}")
        print(f"  reload flag     : {reload}")
        print(f"  bounds          : {model.bounds.begin} → {model.bounds.end}")
        print(f"  chunks returned : {len(intervals)}")
        if intervals and intervals[0] is not None:
            first, last = intervals[0], intervals[-1]
            print(f"  first chunk     : {first.since} → {first.until}")
            print(f"  last chunk      : {last.since} → {last.until}")
    print()


if __name__ == "__main__":
    main()
