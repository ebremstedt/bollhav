"""Entry point for the reload_flag example.

Shows how the `r:` prefix on a tag expression flips a matched model into
reload mode. Reload uses the model's `bounds.begin` / `bounds.end` as the
interval, overriding both latest and backfill.

`@load_models` runs `match_models` and bakes runtime overrides in. When the
tag expression matches with `r:`, the resulting `model.directives.reload`
is True; latest is forced off and the bounds are the source of truth.

Run it with different TAGS values (and optionally LATEST_ENABLED) to see
how reload behaves.
"""

import os

from bollhav.model import Model, load_models


@load_models
def main(models: list[Model], debug: bool) -> None:
    for model in models:
        reload = model.directives.reload
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
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
