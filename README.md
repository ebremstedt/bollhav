# bollhav

Model definition framework that standardizes code at pipe and model level using the model and pipe [abstractions](bollhav/docs/ABSTRACTIONS.md)

This library
- is permissive by design
- is designed with ✨developer experience✨ in mind

Concepts
- [Model](bollhav/docs/MODEL.md)
  - [Upstream](bollhav/docs/MODEL.md#upstream-dependencies)
- [Pipe](bollhav/docs/PIPE.md)
- [Tags](bollhav/docs/TAGS.md)
    - [examples](bollhav/docs/MATCHING.md)
- [Modes](bollhav/docs/MODES.md)
- [Progress Bar](bollhav/docs/PROGRESS_BAR.md)

Implementations:
- [Postgres](bollhav/docs/POSTGRES.md)
- [MSSQL](bollhav/docs/MSSQL.md)

# Demo

![demo](bollhav/docs/batch_recording.gif)

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
