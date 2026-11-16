# Contributing

Thanks for helping. detloop is a small project; focused, well-tested changes
are easiest to review.

## Setup

```console
git clone https://github.com/raunaksahahere/detloop
cd detloop
uv sync --group dev --extra compat
uv run pytest
```

## Before opening a pull request

```console
uv run ruff format src tests && uv run ruff check src tests
uv run mypy
uv run python scripts/check_docs.py
uv run pytest
```

- Add a test that fails without your change. For simulator behaviour, prefer a
  small simulation in the relevant layer (see [docs/TESTING.md](docs/TESTING.md)).
- Behaviour that changes existing seeds' executions must be called out in the
  changelog (see [docs/MAINTENANCE.md](docs/MAINTENANCE.md)).
- Keep the runtime free of third-party dependencies.
- Update the documentation in the same pull request; every documented command
  and example must work.
- Do not weaken checks (broad `# noqa`, `type: ignore`, skipped tests) to get
  green; fix the cause or explain the exception in the code.

## Reporting bugs

Include the seed, the manifest (`manifest.json`) and trace if you can share
them, `detloop --version`, the Python version, the OS, and the replay command.
A failing seed that reproduces with `detloop replay --expect-reproduce` is the
best bug report.

## Licensing

Contributions are accepted under the MIT license. Do not copy code from
projects with incompatible licenses.
