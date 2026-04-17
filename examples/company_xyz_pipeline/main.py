import logging
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from bollhav.pipe import with_pipe_config, PipeConfig
from bollhav.model import match_models
from execute import execute


def setup_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@with_pipe_config
def main(pipe: PipeConfig) -> None:
    setup_logging(debug=pipe.debug)
    matched_models = match_models(
        folder="src/models", tags=pipe.tags, upstream_mode=pipe.upstream_mode
    )

    execute.set_name_width(max(len(m.target.full_name) for m in matched_models))

    for model in matched_models:
        model.apply_pipe(pipe)
        intervals = model.infer_intervals()

        execute.set_total(len(intervals))
        for interval in intervals:
            since, until = (
                (interval.since, interval.until) if interval else (None, None)
            )
            execute(model=model, since=since, until=until)

    execute.finish()


if __name__ == "__main__":
    main()
