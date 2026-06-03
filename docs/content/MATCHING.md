[Home](index.md) › [Tags](TAGS.md) › **Matching**

# Matching models

The standard entry point is `@load_models`. It reads runtime overrides from env vars (see [RUNTIME_OVERRIDES.md](RUNTIME_OVERRIDES.md)), discovers models under `folder`, filters by `TAGS`, topologically sorts them, and bakes the overrides into `batching` / `target.schema` / `directives`. The discovered source models are not mutated.

```python
from bollhav.model import Model, load_models

@load_models
def main(models: list[Model], debug: bool) -> None:
    for model in models:
```

