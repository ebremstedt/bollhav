[back to README](../../README.md)

# Match models in your pipe folder

`apply_pipe_to_models(pipe, folder)` discovers every `Model` under `folder`, filters by `pipe.tags`, topologically sorts them, and returns copies with pipe/tag overrides baked into `batching`, `target.schema`, and `directives`. The input models are not mutated.

```python
from bollhav.pipe import PipeConfig, with_pipe_config
from bollhav.model import apply_pipe_to_models

@with_pipe_config
def main(pipe: PipeConfig) -> None:
    for model in apply_pipe_to_models(pipe, folder="src/models"):
        for interval in model.infer_intervals():
            execute(model, interval, ...)
```

If you only need raw matching (no pipe application — e.g. for diagnostics or tooling), use `match_models(folder, tags, upstream_mode)` directly.
