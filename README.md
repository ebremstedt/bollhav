# bollhav

Model definition framework that standardizes code at pipe and model level using the [model and pipe abstractions](ABSTRACTIONS.md).

This library is very permissive by design. Use the model level without using pipe level. Or use both with a database implemetation, or make your own.

- [Model](DOCS/MODEL.md)
- [Pipe](DOCS/PIPE.md)
- [Modes](DOCS/MODES.MD)
    - [Postgres implementation](DOCS/POSTGRES.md)

Match which models to run using [tags](DOCS/TAGS.md) with examples [here](DOCS/MATCHING.md).

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
