[README.md](..README.md)

# Match models in your pipelines folder

## Using just an environment variable

```sh
export TAGS=[schema_xyz|schema_xyz]
```

```python
from bollhav.model.matching import match_models

tags = env_var(name="TAGS", required=True)
for model in match_models(tags=tags, folder="src/models"):
    execute(model, ...)
```

## Using the decorator

Load pipe level stuff with the decorator:

```python
from bollhav.pipe import PipeConfig, with_pipe_config
from bollhav.model.matching import match_models

@with_pipe_config
def main(pipe: PipeConfig):
    for model in match_models(tags=pipe.tags, folder="src/models"):
        execute(
            model=model,
            since=pipe.backfill.since,
            until=pipe.backfill.until,
        )
```
