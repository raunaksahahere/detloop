# Tech stack

## Runtime

| Item | Choice | Why |
|---|---|---|
| Language | Python, pure (no compiled extensions) | Debuggable, installs everywhere; the brief favours a pure-Python core |
| Supported interpreters | CPython 3.11–3.14 | `asyncio.Runner(loop_factory=...)` exists from 3.11; see [ADR 0007](DESIGN_DECISIONS/0007-version-support.md) |
| Runtime dependencies | **None** (standard library only) | Keeps installation trivial and the trust surface small |
| Event loop | Custom `asyncio.AbstractEventLoop` | [ADR 0001](DESIGN_DECISIONS/0001-custom-event-loop.md) |
| PRNG | SplitMix64 over SHA-256-derived stream seeds | Fully specified and portable; [DETERMINISM.md](DETERMINISM.md) |
| Storage | JSON Lines (traces), JSON (manifests), SQLite (`sqlite3` stdlib) | No server; easy to diff and inspect |
| Report | Static HTML with inline vanilla JavaScript and CSS | Works offline; no CDN; hash-pinned CSP |
| Parallelism | `multiprocessing` with the `spawn` start method and custom supervision | Cross-platform; lets the parent hard-kill hung workers |

## Optional dependencies

| Extra | Contents | Purpose |
|---|---|---|
| `compat` | `aiohttp>=3.10,<4`, `httpx>=0.27,<0.29` | Library compatibility tests (tested versions in [COMPATIBILITY.md](COMPATIBILITY.md)) |

pytest is not a dependency: the plugin module is imported by pytest through
the `pytest11` entry point only when pytest is installed.

## Development tooling (dependency groups, PEP 735)

| Group | Tools | Purpose |
|---|---|---|
| `test` | pytest, hypothesis, pytest-asyncio | Test runner; property-based checker validation; coexistence test |
| `lint` | ruff, mypy | Formatting and linting (ruff), strict typing (`mypy --strict` on `src/detloop`) |
| `release` | build, twine, pip-licenses | Building, metadata validation (`twine check`), dependency license audit |
| `dev` | all of the above | Local development |

Tested tool versions at release: pytest 9.2.0, hypothesis 6.170.15,
pytest-asyncio 1.5.0, ruff 0.17.5, mypy 2.4.1.

## Packaging

| Item | Choice | Why |
|---|---|---|
| Build backend | hatchling | Standards-based (PEP 517/621/639), simple `src/` layout, includes package data (report assets) without configuration |
| Layout | `src/detloop` | Tests run against the installed package, not the working tree |
| Version | single source in `src/detloop/_version.py`, read by hatchling | CLI, metadata and tag must agree (checked in the release workflow) |
| Environment manager | uv with a committed `uv.lock` | Reproducible development environments across Python versions |
| License metadata | SPDX `MIT` with `license-files` (PEP 639) | Machine-readable licensing |

## CI and release

| Item | Choice |
|---|---|
| CI | GitHub Actions: lint/type/docs/licenses; test matrix 3.11–3.14 × Linux/macOS/Windows; cross-job trace comparison; build and installed-package smoke tests |
| Actions | Pinned to full commit SHAs |
| Publishing | PyPI Trusted Publishing (OIDC) from a protected `pypi` environment; no long-lived tokens |
| Release assets | wheel, sdist, `SHA256SUMS` attached to the GitHub release; the same files are uploaded to PyPI |

## Licensing considerations

- detloop is MIT-licensed. It contains no third-party source code.
- CPython's asyncio source (PSF license) was read to mirror its semantics; no
  code was copied. Published designs that informed detloop at the concept
  level only: FoundationDB/TigerBeetle-style deterministic simulation, turmoil
  and madsim (Rust), Porcupine/Knossos (Wing & Gong search with memoization),
  Jepsen histories. No source from these projects, nor from async-solipsism
  (GPL-3.0), was used.
- Optional/dev dependencies keep their own licenses; `pip-licenses` runs in
  CI and fails if any package of the `compat` extra is GPL-family licensed.
  At release: aiohttp Apache-2.0 AND MIT, httpx/httpcore/idna BSD, anyio/h11/
  attrs MIT, multidict/yarl/frozenlist/propcache/aiosignal Apache-2.0,
  aiohappyeyeballs PSF, certifi MPL-2.0 (file-level copyleft, unmodified, test
  dependency only).
