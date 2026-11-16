# Command-line interface

```
detloop [--version] COMMAND ...
python -m detloop COMMAND ...
```

## Exit codes (stable)

| Code | Meaning |
|---|---|
| 0 | Success: every seed passed, the replay passed, or shrinking finished |
| 1 | At least one seed failed (or the replayed run failed) |
| 2 | Usage, configuration or target error |
| 3 | No failures, but some runs were incomplete (budget exhausted, checker unknown, wall timeout) |
| 4 | Replay mismatch or divergence, or `--expect-reproduce` not satisfied |
| 5 | Internal detloop error |
| 130 | Interrupted |

For `explore`, the precedence is 130 > 5 > 1 > 3 > 0.

## Targets

`path/to/file.py`, `path/to/file.py::test_name`, `package.module`, or
`package.module::test_name`. Files are imported like pytest's `prepend`
mode: the first ancestor directory without `__init__.py` goes on `sys.path`.

## `detloop explore TARGET`

| Option | Default | Meaning |
|---|---|---|
| `--seeds N` | decorator `seeds` | Number of seeds |
| `--seed-start S` | decorator `seed_start` | First seed |
| `-j, --jobs J` | 1 | Worker processes; 1 runs in-process |
| `--max-failures K` | none | Stop dispatching above the K-th lowest failing seed; all lower seeds still run, so the reported set is independent of `--jobs` |
| `--faults SPEC` | decorator | Override fault kinds (`none` disables) |
| `--scheduler MODE` | decorator | `interleave`, `fifo`, `relaxed` |
| `--set KEY=VALUE` | | Override any config field; JSON values (`--set max_virtual_time=60 --set network.crash_mode='"host"'`) or plain strings |
| `--wall-timeout S` | 60 | Supervisory wall-clock seconds per seed; workers that overrun are killed (outcome `incomplete`/`wall_timeout`) |
| `--artifacts DIR` | `.detloop/artifacts` | Failure traces and manifests |
| `--db PATH` / `--no-db` | `.detloop/results.sqlite` | Results database |
| `--shrink` | off | Shrink the canonical first failure |
| `--hash-seed H` | `0` | `PYTHONHASHSEED` for worker processes (recorded) |
| `--fresh-process` | off | New worker process per seed (isolates module globals; slower) |
| `--verify-determinism N` | 0 | Re-run the first N seeds in fresh processes with `PYTHONHASHSEED` 1 and 2 and compare trace hashes |
| `--json` | off | JSON summary on stdout |
| `--summary-file PATH` | | Also write the JSON summary |
| `-q, --quiet` | off | Only the final summary |

JSON summary keys: `target`, `test`, `seeds_requested`, `seeds_completed`,
`passed`, `failed`, `incomplete`, `errors`, `first_failure` (canonical),
`first_failure_discovered`, `signatures` (signature → seeds), `artifacts`
(seed → manifest), `interrupted`, `discarded_beyond_cutoff`, `jobs`,
`wall_seconds`, `seeds_per_second`, `db`, `exploration_id`, `exit_code`, and
optionally `shrink` and `determinism`.

## `detloop replay MANIFEST`

Reloads the test named in the manifest, checks the replay envelope (test
module fingerprint must match unless `--allow-mismatch`; detloop version,
Python version and hash seed differences are reported as warnings), and
reruns with the manifest's configuration. Manifests with decision tapes
replay in strict mode.

| Option | Meaning |
|---|---|
| `--allow-mismatch` | Replay although the test module changed |
| `--trace-level L` | Record at another level (trace hash then not compared) |
| `--out DIR` | Write the replayed trace and manifest |
| `--expect-reproduce` | Exit 0 only if the signature *and* trace hash match, else 4 |
| `--json` | Machine-readable result |

## `detloop shrink SOURCE`

`SOURCE` is a manifest, or a target with `--seed N`. Writes
`<artifacts>/<test>/seed-N-shrunk/manifest.json` with explicit faults and
decision tapes, and prints before/after metrics (faults, non-zero decisions,
total decisions, steps, trace events, virtual time) and the status
`fixed_point` or `budget_exhausted`.

| Option | Default |
|---|---|
| `--max-runs` | 300 candidate runs |
| `--max-seconds` | 300 |
| `--artifacts` | `.detloop/artifacts` |
| `--json`, `-q` | |

## `detloop report INPUT [-o OUT]`

`INPUT` is a `manifest.json`, a `trace.jsonl` or a results database
(`.sqlite`). Writes one self-contained HTML file (default
`detloop-report.html`).

## pytest options

| Option | Meaning |
|---|---|
| `--detloop-seed=N` | Run only seed N of every simulation test |
| `--detloop-seeds=N`, `--detloop-seed-start=N` | Override the seed range |
| `--detloop-replay=MANIFEST` | Replay a saved (possibly shrunk) failure; skips other simulation tests |
| `--detloop-artifacts=DIR`, `--detloop-no-artifacts` | Artifact location |
| `--detloop-shrink` | Shrink a failure and save the minimized manifest |
| `--detloop-trace=off\|basic\|full` | Override the trace level |
