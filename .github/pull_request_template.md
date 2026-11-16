## What and why

<!-- What does this change and why? Link issues. -->

## Reproduction / evidence

- Seed(s) and configuration used:
- Replay command (if relevant):
- Behaviour change for existing seeds? <!-- yes/no; if yes, explain and update CHANGELOG -->

## Environment

- Python version(s) and OS tested:

## Validation

- [ ] `uv run ruff format --check src tests && uv run ruff check src tests`
- [ ] `uv run mypy`
- [ ] `uv run python scripts/check_docs.py`
- [ ] `uv run pytest` (and `tests/compat` if networking changed)
- [ ] Documentation updated
- [ ] A test fails without this change
