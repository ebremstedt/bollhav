# bollhav

Model definition framework that standardizes data-pipeline code.

This library
- is permissive by design
- is designed with ✨developer experience✨ in mind

Concepts
- [Model](docs/content/MODEL.md)
  - [Upstream](docs/content/MODEL.md#upstream-dependencies)
- [Runtime overrides](docs/content/RUNTIME_OVERRIDES.md) — env vars that override model settings at run time, plus the `@load_models` decorator
- [Tags](docs/content/TAGS.md)
    - [matching](docs/content/MATCHING.md)
- [Modes](docs/content/MODES.md)
- [Progress Bar](docs/content/PROGRESS_BAR.md)
- [State](docs/content/STATE.md)

Implementations:
- [Postgres](docs/content/POSTGRES.md)
- [MSSQL](docs/content/MSSQL.md)

# Demo

![demo](docs/content/batch_recording.gif)

## Explore the features

See [examples/](examples/) for self-contained, runnable pipelines that isolate each feature of bollhav.

## Installation
```bash
pip install bollhav
```

## Testing
Tests use `pytest`. Run the full suite:
```bash
pytest tests/
```

## Build + publish example

```sh
git tag 1.2.3 && git push --tags
```
