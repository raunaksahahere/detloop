# Releasing

## Ownership

| Destination | Owner | Mechanism |
|---|---|---|
| GitHub repository `raunaksahahere/detloop` | raunaksahahere | Tags and releases created by `.github/workflows/release.yml` |
| PyPI project `detloop` | raunaksahahere | Trusted Publishing (OIDC) from `release.yml`, job `publish-pypi`, environment `pypi` |

No API tokens are stored anywhere. Never paste tokens into issues, chats or
workflow files.

## One-time setup

1. **PyPI trusted publisher.** On pypi.org (logged in as the owner): *Your
   projects → Publishing* (or *Add a new pending publisher* before the first
   upload) with:
   - PyPI project name: `detloop`
   - Owner: `raunaksahahere`
   - Repository name: `detloop`
   - Workflow name: `release.yml`
   - Environment name: `pypi`
2. **GitHub environment.** Repository *Settings → Environments → `pypi`*,
   restricted to tags matching `v*` (deployment branch/tag policy). Optional:
   add a required reviewer to approve each publication.
3. Tag protection: protect `v*` tags so they cannot be moved or deleted.

## Procedure

1. Update the version in `src/detloop/_version.py`, `CHANGELOG.md`, and add
   release notes at `docs/releases/vX.Y.Z.md`. Keep docs consistent.
2. Run the release gates locally:
   ```console
   uv run ruff format --check src tests && uv run ruff check src tests && uv run mypy
   uv run python scripts/check_docs.py
   uv run pytest                                   # also on 3.11/3.13/3.14
   uv build && uv run twine check --strict dist/*
   ```
3. Commit, push to `main`, and wait for **CI on that exact commit** to pass
   (all OS × Python jobs, fixture comparison, build/smoke).
4. Audit what will be published: `git ls-files`, the sdist and wheel contents
   (`tar tzf dist/*.tar.gz`, `unzip -l dist/*.whl`), commit authors, and that
   no PDFs, research material, `.detloop/` artifacts or secrets are included.
5. Create and push the tag from the verified commit:
   ```console
   git tag -a vX.Y.Z -m "detloop X.Y.Z" <commit>
   git push origin vX.Y.Z
   ```
6. The release workflow then:
   - `build`: checks the tag equals the package version, builds the sdist and
     wheel **once**, runs `twine check --strict`, writes `SHA256SUMS`, uploads
     them as the `dist` artifact;
   - `verify`: installs exactly those files into clean environments on
     Linux/macOS/Windows × 3.11–3.14 and runs `scripts/smoke_installed.py`
     plus the test suite against the installed wheel;
   - `github-release`: creates the GitHub release for the tag with the same
     files and `SHA256SUMS`, using `docs/releases/vX.Y.Z.md` as notes. It
     refuses to overwrite an existing release;
   - `publish-pypi`: uploads the same files to PyPI via Trusted Publishing.
   The workflow uses a concurrency group per tag, so manual re-runs cannot race.
7. Verify after publication:
   ```console
   git ls-remote origin refs/tags/vX.Y.Z           # resolves to the intended commit
   gh release download vX.Y.Z -D /tmp/rel && (cd /tmp/rel && sha256sum -c SHA256SUMS)
   python -m venv /tmp/v && /tmp/v/bin/pip install detloop==X.Y.Z pytest
   (cd /tmp && /tmp/v/bin/python /path/to/scripts/smoke_installed.py X.Y.Z)
   ```

## Release artifact inventory

| File | Source |
|---|---|
| `detloop-X.Y.Z-py3-none-any.whl` | `uv build` in the `build` job |
| `detloop-X.Y.Z.tar.gz` | same |
| `SHA256SUMS` | `sha256sum` of the two files above |

## Partial failures, rollback and yanking

- **PyPI publish failed** (e.g. trusted publisher not configured): fix the
  configuration and *re-run the failed `publish-pypi` job* of the same
  workflow run. It reuses the already-verified artifacts. Do not rebuild.
- **GitHub release exists but PyPI does not**: same as above; report the two
  destinations' status separately.
- **A bad release reached PyPI**: never delete or overwrite a version (PyPI
  versions are immutable). *Yank* it on pypi.org with a reason, fix the
  problem, and release a new patch version.
- **Never force-move a published tag.** If a tag points to the wrong commit
  before anything was published, delete the tag and the draft release, then
  re-tag; after publication, release a new version instead.
