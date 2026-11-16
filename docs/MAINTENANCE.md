# Maintenance

## Adding a new Python version

1. Add it to the CI matrix (`.github/workflows/ci.yml`, `release.yml`) and to
   the classifiers in `pyproject.toml`.
2. Read the asyncio changelog for the new version, focusing on `Handle`,
   `TimerHandle`, `Task` (step/wakeup scheduling, eager tasks), `Runner`,
   `Server.wait_closed`/`serve_forever`, streams and `create_task` signatures.
3. Re-check every item in `src/detloop/_compat.py` and the
   [private-internals table](COMPATIBILITY.md#private-cpython-internals-used).
4. Run the full suite, including `tests/compat`, and compare the portable
   fixture hashes with the other versions (`scripts/compare_fixture_hashes.py`).
5. Determine pinned regression seeds for the new runtime family if behaviour
   differs (`tests/test_kv_corpus.py`), and update
   [COMPATIBILITY.md](COMPATIBILITY.md).
6. Only then add the version to `SUPPORTED_VERSIONS` and the docs.

Dropping a version: remove it from the matrix and classifiers, bump
`requires-python`, and note it in the changelog.

## Trace and manifest compatibility

- The canonical trace, manifest, results database and PRNG derivation are
  versioned (`src/detloop/_version.py`). Read
  [TRACE_FORMAT.md](TRACE_FORMAT.md#compatibility-policy) before changing any
  of them.
- Any change that alters execution for existing seeds (scheduler rules,
  network model, fault generation, decision consumption) changes traces and
  pinned seeds. Document it in the changelog as behaviour-changing, re-pin
  regression seeds, and regenerate benchmark results.
- Test-visible constants in `tests/test_rng.py` (stream derivation values)
  must never change without a new `PRNG_VERSION`.

## Regression triage

1. Reproduce: `detloop replay <manifest> --expect-reproduce` on the same Python
   minor version; check the envelope warnings.
2. If replay diverges, determine whether the code under test, the runtime or
   detloop changed (`--allow-mismatch` to compare).
3. Shrink (`detloop shrink`) and render a report (`--trace-level full`).
4. Classify: product bug (keep the seed as a regression test and keep an
   exploration test), detloop bug (add a focused test in the relevant layer),
   or unsupported feature (document it and make it fail loudly).

## Dependency maintenance

- Runtime: none. Keep it that way unless there is a compelling reason.
- `compat` extra: when aiohttp or httpx release, run `tests/compat` against the
  new version, update the tested-version table in
  [COMPATIBILITY.md](COMPATIBILITY.md), and widen the range in
  `pyproject.toml` only after the tests pass.
- Dev tools: update `uv.lock` (`uv lock --upgrade`) periodically; fix new lint
  or typing findings instead of adding broad ignores.
- GitHub Actions are pinned to commit SHAs; update them deliberately (check
  the release notes) rather than floating tags.

## Validating changes

Every change must keep green: `ruff format --check`, `ruff check`,
`mypy --strict`, `scripts/check_docs.py`, the full test suite on 3.11–3.14,
and the CI fixture comparison. Behaviour changes need a test that fails
without the change. Performance work should be measured with
`benchmarks/bench.py` and recorded, never asserted as a CI threshold.
