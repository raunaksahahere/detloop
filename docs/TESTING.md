# Testing

## Commands

```console
uv sync --group dev --extra compat     # dev tools + aiohttp/httpx for compat tests
uv run pytest                          # full suite (~40-70 s)
uv run pytest tests/compat             # library compatibility only
DETLOOP_SLOW=1 uv run pytest -m slow   # 1,000-seed corrected-baseline run
uv run ruff format --check src tests && uv run ruff check src tests
uv run mypy                            # strict typing of src/detloop
uv run python scripts/check_docs.py    # links, anchors, embedded examples
```

Run on another Python with a separate environment:
`UV_PROJECT_ENVIRONMENT=.venv-3.11 uv run --python 3.11 pytest`.

## Test layers

| Layer | Files | What it proves |
|---|---|---|
| PRNG and decisions | `test_rng.py` | Published SplitMix64 vectors, stable stream derivation constants, stream independence, strict-replay divergence detection, guided normalization |
| Loop semantics | `test_loop.py` | `fifo` ordering equals real asyncio on the same workload; per-owner FIFO under `interleave`; `relaxed` can break FIFO (opt-in); virtual time; equal-time timers; asyncio feature subset; deadlock, starvation, step/time budgets; watchdog interrupts a non-yielding coroutine; unsupported APIs and executors fail loudly |
| Network | `test_network.py` | Ordered large transfers; `drain()` backpressure; backpressure deadlock detection; buffer budget; half-close; refused/timeouts; SYN retransmission; partitions stall then deliver in order after heal; retransmission timeout; one-way partitions; reference-counted partitions; both crash modes; late packets never reach a new incarnation; resets; server lifecycle and errors; bandwidth; message loss/dup/reorder |
| World | `test_world.py` | Crashed tasks stay frozen even when suppressing cancellation; ownership of descendants and fresh contexts; disk vs volatile; restarts; node errors; clock skew/drift isolated from `loop.time()`; sticky invariants; fault generation and application; framework errors surface as internal errors |
| Guards | `test_guards.py` | Virtual time inside, real outside; every blocked API fails even if swallowed; seeded entropy; `random` restored; restoration after failure and `KeyboardInterrupt`; other threads unaffected; nested simulations rejected; simulated sockets support the happy-eyeballs path |
| Checker | `test_checker.py`, `oracle.py` | Hand-written legal/illegal/pending/timeout/CAS cases; budget exhaustion is `unknown`; 1,500 Hypothesis histories and all 675 two-operation histories agree with an independent brute-force oracle |
| Determinism | `test_determinism.py` | Identical hashes in-process and in fresh processes under `PYTHONHASHSEED` 0/1/4242 (including the KV example); trace level does not change execution; strict replay reproduces; no host data in traces; seeds independent of prior runs |
| Exploration and shrinking | `test_explore_shrink.py` | Serial and parallel agree per seed (status, signature, hash); deterministic `max_failures` cutoff; hard kill of a CPU-bound worker; SQLite schema and versioning; artifacts replay; shrinking preserves the signature, removes faults, replays strictly; budget exhaustion reported |
| Bug corpus | `test_kv_corpus.py` | Each planted bug is found by exploration within its declared budget, replayed with an identical trace, shrunk and strictly replayed; pinned regression seeds; corrected baseline passes 40 seeds (1,000 with `DETLOOP_SLOW=1`) |
| CLI | `test_cli.py` | Exit codes, JSON summaries, explore→replay→shrink→report round trip, replay rejects changed code and foreign manifests, `--verify-determinism` |
| Report | `test_report.py` | Adversarial strings stay inert; CSP hashes match the inline script/style; no external resources |
| pytest plugin | `test_pytest_plugin.py` | Discovery, parametrization, fixtures, failure messages, `--detloop-seed`, `--detloop-replay`, `--detloop-shrink`, coexistence with pytest-asyncio auto mode |
| Examples | `test_examples.py` | The README example finds and replays its race; the fixed version passes 200 seeds; both run under pytest |
| Compatibility | `compat/test_http_libraries.py` | aiohttp server/client and httpx over simulated transports with timeouts, partitions, crashes, determinism; TLS rejected |

## Independent oracles and negative controls

- The linearizability checker is validated against `tests/oracle.py`, a
  brute-force enumerator that shares no code with it.
- `fifo` scheduling is validated against the real asyncio loop.
- Negative controls: stale reads, lost updates, backpressure deadlocks,
  busy loops, swallowed violations and broken predicates must *fail*; the
  corrected KV baseline and the fixed quickstart must *pass*.

## Seeded regression strategy

Exploration is the evidence; fixed seeds pin known reproductions. Pinned seeds
are per runtime family because 3.11 schedules different internal callbacks
than 3.12+ (`tests/test_kv_corpus.py`). When a bug is fixed, keep its seed as a
regression test *and* keep an exploration test with a budget.

Expected findings (planted bugs) are asserted, so the suite stays green.

## Determinism checks in CI

Every CI job records the trace hashes (and uploads the traces) of the
declared portable fixture (`scripts/fixture_hashes.py --dump`). A follow-up job
compares them (`scripts/compare_fixture_hashes.py`): every pair of operating
systems with the same Python minor version must agree; differences across
minor versions are reported. This gate caught a real leak before release —
errno numbers in exception text differ between Linux, macOS and Windows — which
is now canonicalized. At release the fixture is byte-identical across all 12
jobs (3 operating systems × 4 Python versions).

## Reproducing a CI failure

1. Download the job's `.detloop/artifacts` (uploaded on failure) or read the
   seed from the log.
2. Use the same Python minor version (`uv run --python 3.X`).
3. `detloop replay <manifest> --expect-reproduce`, or
   `pytest <nodeid> --detloop-seed=N`.
4. Add `--trace-level full` and `detloop report` for a timeline.
