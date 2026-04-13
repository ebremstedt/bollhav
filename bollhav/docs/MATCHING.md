[back to README](../../README.md)

# Match models in your pie folder

## Using just an environment variable

```sh
export TAGS=[schema_xyz|schema_xyz]
```

```python
from bollhav.model import match_models

tags = env_var(name="TAGS", required=True)
for model, reload in match_models(tags=tags, folder="src/models"):
    if reload:
        interval = (model.bounds.begin, model.bounds.end)
    else:
        interval = ...  # use your default incremental interval
    execute(model, interval, ...)
```

## Using the decorator

Load pipe level config with the decorator:

```python
from bollhav.pipe import PipeConfig, with_pipe_config
from bollhav.model import match_models

@with_pipe_config
def main(pipe: PipeConfig):
    for model, reload in match_models(tags=pipe.tags, folder="src/models"):
        if reload:
            interval = (model.bounds.begin, model.bounds.end)
        else:
            interval = model.batching.infer_intervals(
                since=None, until=None,
                batch_expression=pipe.latest.batch_expression or model.batching.batch_expression,
                latest=True,
            )
        execute(model, interval, ...)
```
