"""Entry point for the tag_matching example.

The purpose of this example is to demonstrate how tag expressions select
models. Instead of actually running anything against data, we just print
the matched models and their tags — the important output is *which* models
match for a given TAGS expression.

Run with different TAGS values to see different selections. See README.md
in this folder for a table of expressions to try.
"""

import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from bollhav.pipe import with_pipe_config, PipeConfig
from bollhav.model import match_models


@with_pipe_config
def main(pipe: PipeConfig) -> None:
    matched = match_models(folder="src/models", tags=pipe.tags)

    print(f"\nTAGS = {pipe.tags!r}")
    print(f"Matched {len(matched)} model(s):\n")
    for model in matched:
        reload_marker = " (reload)" if model.directives.reload else ""
        tags = ", ".join(sorted(model.tags))
        print(f"  - {model.target.full_name}{reload_marker}")
        print(f"      auto-tags: {tags}")
    print()


if __name__ == "__main__":
    main()
