# arcus-py

Python Driver for [Arcus](https://github.com/naver/arcus) Cache.

> Status: pre-alpha. The public API is not implemented yet.

## Requirements

- Python 3.11 or newer

## Development

This project uses [uv](https://docs.astral.sh/uv/).

```sh
uv sync    # create .venv with runtime and dev dependencies

uv run ruff check .
uv run ruff format --check .
uv run ty check
uv run pytest
uv build
```

All functions and methods must be fully type-annotated (enforced by ruff `ANN` and `ty`).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
