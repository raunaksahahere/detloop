# Implementation status (v0.1.0)

Requirement → implementation → evidence. "CI" means the GitHub Actions
workflow `ci.yml`; results are linked from the release notes.

## Core contracts

| Requirement | Implementation | Evidence |
|---|---|---|
| Virtual monotonic clock, timer heap with stable tie-breakers, time advances only when idle | `loop.py` (`_advance_time`, `_pop_due`, integer ns) | `test_loop.py::test_virtual_time_*`, `test_equal_time_timers_*` |
| Preserve `call_soon` FIFO; explore independent choices | Per-owner iterations, owner decision (ADR 0002) | `test_fifo_scheduler_matches_real_asyncio_order`, `test_call_soon_order_preserved_per_owner_under_interleave`, `test_interleave_explores_cross_node_orders`, `test_racy_counter_has_both_outcomes` |
| Opt-in relaxed ready-queue permutation, labelled | `scheduler="relaxed"` | `test_relaxed_scheduler_can_break_fifo_but_is_opt_in` |
| Cancellation, TaskGroup, timeouts, wait_for, gather, shield, primitives, async generators, contextvars, exceptions, runner cleanup | Stdlib Task/Future on `SimLoop` | `test_asyncio_primitives_supported_subset`, `test_exceptions_propagate_and_runner_cleans_up`, `test_eager_task_factory_when_available` |
| Budgets; distinguish assertion, deadlock, starvation, budget, unsupported, internal | `runner.classify`, `BudgetExceeded`, `DeadlockError`, `StarvationError` | `test_deadlock_*`, `test_busy_loop_is_starvation_not_a_pass`, `test_step_budget_is_incomplete`, `test_virtual_time_budget_is_incomplete`, `test_write_buffer_hard_cap_is_a_budget_failure`, `test_framework_errors_in_fault_callbacks_are_internal_errors` |
| Seeded independent streams; stable derivation; no `hash()` | `rng.py` | `test_rng.py` (reference vectors, derivation constants, independence) |
| Replay metadata, envelope, divergence detection | `artifacts.build_manifest`, `replay.check_envelope`, strict `Decisions` | `test_strict_tape_replay_reproduces_and_detects_divergence`, `test_replay_rejects_changed_code`, `test_replay_rejects_foreign_manifests` |
| Canonical JSONL; metadata separated | `trace.py`, manifest `metadata` | `test_canonical_trace_contains_no_host_specific_data` |
| Identical hashes in fresh processes and different `PYTHONHASHSEED` | | `test_fresh_processes_with_different_hash_seeds_agree` (0, 1, 4242; fixture and KV) |
| Cross-version/platform equivalence on declared fixtures | `scripts/fixture_hashes.py`, `compare_fixture_hashes.py` | CI `determinism` job; locally byte-identical on 3.11/3.12/3.13 |
| Ownership of tasks/timers/callbacks/transports incl. descendants | `_owner.py`, contextvar ownership (ADR 0004) | `test_descendant_tasks_and_callbacks_inherit_ownership` |
| Block real networking, threads, subprocesses, DNS, entropy, real time; restore patches | `guards.py`, `SimSocket`, loop rejections | `test_guards.py` (all blocked APIs, restoration after failure/interrupt, other threads unaffected, nested rejected) |
| Supervisory wall-clock timeouts outside virtual time; kill hung workers | `runner._Watchdog`, explorer hard deadline | `test_wall_clock_watchdog_interrupts_cpu_bound_code`, `test_hard_kill_of_cpu_bound_worker` |
| create_connection/create_server, streams | `net/network.py`, `net/stream.py`, `net/server.py` | `test_network.py` |
| Addressing, DNS, ports, refusal, connection IDs, latency/jitter, bandwidth, partitions, healing | `Network` | `test_addressing_and_resolution`, `test_connection_refused_and_timeouts`, `test_bandwidth_limits_transfer_time`, `test_oneway_partition_*`, `test_overlapping_partitions_are_reference_counted` |
| Flow control: buffers, pause/resume, drain, reader backpressure, EOF/half-close, close vs abort, wait_closed, failure propagation | `SimTransport`, receiver window | `test_drain_blocks_until_receiver_reads`, `test_backpressure_deadlock_is_detected`, `test_half_close_eof_and_close_vs_abort`, `test_echo_and_ordered_large_transfer_with_backpressure` |
| Reliable ordered streams; faults below the stream; separate message model | Stall/retransmit/timeout; `MessageEndpoint` (ADR 0003) | `test_partition_stalls_stream_then_delivers_in_order_after_heal`, `test_long_partition_times_out_connection`, `test_message_bus_*` |
| In-flight data at partition start/end, resets, crashes | | `test_partition_stalls_*` (data in flight at partition start), `test_crash_semantics_for_peers[process|host]`, `test_host_crash_then_restart_resets_old_connections`, `test_late_packets_never_reach_a_new_incarnation`, `test_reset_fault_and_reset_connections` |
| World/node API; deterministic fault schedules; clock skew independent of monotonic clock | `world.py`, `faults.py` | `test_scheduled_faults_are_applied_and_recovered`, `test_fault_generation_is_deterministic_and_bounded`, `test_local_clock_skew_and_drift_do_not_affect_loop_time` |
| Crash invalidates activity; generations; restart via factory; suppression-proof | Incarnations; dead-owner handles dropped | `test_crash_freezes_tasks_even_if_they_suppress_cancellation`, `test_disk_survives_crash_volatile_does_not` |
| Persistent state defined | `node.disk` (no fsync model) | `docs/FAULT_MODEL.md`, `test_disk_survives_crash_volatile_does_not` |
| Invariants, histories (pending, failures) | `World.check/invariant`, `History` | `test_invariants_are_sticky_and_periodic`, `test_history_api_records_logical_order` |
| Bounded linearizability checker, extension interface, valid/invalid/unknown, ambiguous outcomes | `checker/linearizability.py` (ADR 0005) | `test_checker.py` incl. oracle agreement (1,500 generated + 675 exhaustive) and budget → unknown |
| Deterministic seed ranges, serial/parallel, cancellation, failure collection, deterministic aggregation | `explore.py` | `test_serial_and_parallel_agree_per_seed`, `test_max_failures_cutoff_is_deterministic` |
| Bounded ddmin shrinker, signature preserved, revalidated, metrics, fixed point vs budget | `shrink.py` (ADR 0006) | `test_shrink_*`, `test_planted_bug_found_replayed_and_shrunk` |
| `@simulate`, `world.partition/crash/restart/heal` | `api.py`, `world.py` | README/GETTING_STARTED examples, `test_examples.py` |
| CLI explore/replay/shrink/report/--version, exit codes, JSON | `cli.py` | `test_cli.py` |
| pytest plugin, `--detloop-seed`, replay of shrunk runs, no pytest-asyncio collision | `pytest_plugin.py`, `pytest_support.py` | `test_pytest_plugin.py`, fresh-install smoke test |
| JSONL traces, versioned SQLite, safe bounded writes | `artifacts.py`, `trace.py` | `test_sqlite_schema_and_artifacts` |
| Offline HTML report, escaping, no CDN | `report.py`, `report_assets/` | `test_report.py` |
| aiohttp client/server and httpx over simulated transports | | `tests/compat/test_http_libraries.py` |

## Demonstration system

Toy Raft-style KV (`src/detloop/examples/kvstore.py`), simulation tests
(`kv_sim.py`). Each planted bug is found by exploration within its declared
budget, saved, replayed with an identical trace, and shrunk; the corrected
baseline passes. Evidence: `tests/test_kv_corpus.py`, 300-seed corpus numbers
in [BENCHMARKS.md](BENCHMARKS.md).

## Release gates

| # | Gate | Status |
|---|---|---|
| 1 | Unit/integration tests with negative controls | Passing locally on 3.11–3.14 (Linux); CI matrix: see release notes |
| 2 | Fresh-process determinism under multiple hash seeds; serial/parallel agreement | Passing (tests) |
| 3 | Adversarial crash/partition/in-flight/backpressure/cancellation/deadlock tests | Passing |
| 4 | Planted bugs detected and replayed; corrected baseline passes its budget | Passing (40 seeds in suite; 300 in benchmarks; 1,000 with `DETLOOP_SLOW=1`) |
| 5 | Checker agrees with independent oracle; unknown exercised | Passing |
| 6 | Shrinking preserves signature; replayable artifact | Passing |
| 7 | aiohttp/httpx integration | Passing (httpx keep-alive limitation documented) |
| 8 | Wheel and sdist installed outside the tree; CLI, imports, pytest discovery, examples, resources | `scripts/smoke_installed.py` locally and in CI/release |
| 9 | CI on 3.11–3.14 × Linux/macOS/Windows; cross-job trace comparison | See release notes for run links |
| 10 | Lint, format, typing, docs, licenses, metadata | Passing (`ruff`, `mypy --strict`, `check_docs.py`, `pip-licenses`, `twine check --strict`) |
| 11 | Benchmarks with raw results and baseline | [BENCHMARKS.md](BENCHMARKS.md) |
| 12 | No leaks, junk, secrets or source material in artifacts | Release workflow package audit; manual audit before tagging |

## Known limitations (by design in 0.1.0)

- Interleaving within one node is explored only through timing, not by
  reordering ready callbacks (unless `relaxed`).
- No TLS, IPv6, UNIX sockets, datagram endpoints or disk model.
- httpx does not reuse keep-alive connections over simulated sockets.
- Decision-tape shrinking often stops on its budget for network-heavy
  workloads; fault shrinking is effective.
- Traces are byte-identical across Python minor versions only for workloads
  that do not depend on version-specific internal callback counts (verified
  for the declared fixture); pinned seeds are per runtime family.
- Module-level mutable state in test code persists between seeds in a worker
  (use `--fresh-process` to isolate).

## Unresolved issues

None known at release time. Report problems via the issue tracker.
