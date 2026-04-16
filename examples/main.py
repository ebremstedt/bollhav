import logging
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from bollhav.pipe import with_pipe_config, PipeConfig
from bollhav.model import match_models
from execute import execute

MODELS_DIR = os.path.join(os.path.dirname(__file__), "models")


def setup_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@with_pipe_config
def main(pipe: PipeConfig) -> None:
    setup_logging(debug=pipe.debug)
    matches = match_models(folder=MODELS_DIR, tags=pipe.tags)

    execute.set_name_width(max(len(m.target.full_name) for m in matches))

    for model in matches:
        model.runtime_override.apply_pipe(pipe)
        model.target.schema.suffix = model.runtime_override.schema_suffix

        intervals = model.infer_intervals()

        execute.set_total(len(intervals))
        for interval in intervals:
            execute(
                model=model,
                since=interval.since,
                until=interval.until,
            )

    execute.finish()


if __name__ == "__main__":
    main()
