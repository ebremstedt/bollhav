# bollhav

Model definition framework that standardizes code at pipe and model level using the [model and pipe abstractions](ABSTRACTIONS.md)

This library is very permissive by design. Use the model level without using pipe level. Or use both with a database implemetation, or make your own.

- [Model](bollhav/DOCS/MODEL.md)
- [Pipe](bollhav/DOCS/PIPE.md)
- [Modes](bollhav/DOCS/MODES.MD)
    - [Postgres implementation](bollhav/DOCS/POSTGRES.md)

Match which models to run using [tags](bollhav/DOCS/TAGS.md) with examples [here](bollhav/DOCS/MATCHING.md).

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
