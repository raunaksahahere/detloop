# Changelog

All notable changes are documented here. The project follows
[Semantic Versioning](https://semver.org/); while the version is 0.x, minor
releases may change behaviour and seed-to-execution mappings, and the
changelog says so explicitly.

## [0.1.0] - 2026-11-16

First public release.

### Added

- `SimLoop`: a deterministic `asyncio.AbstractEventLoop` with integer-nanosecond
  virtual time, per-node FIFO scheduling with seeded interleaving
  (`interleave`), single-loop `fifo` mode, opt-in `relaxed` mode, step /
  virtual-time / buffer budgets, deadlock and starvation detection, and a
  supervisory wall-clock watchdog.
- Seeded decision streams (SplitMix64, SHA-256 stream derivation) with
  recorded tapes, strict replay with divergence detection, and guided replay.
- `World` and `Node`: multi-node worlds, crash generations, restart factories,
  persistent `disk`, per-node clock skew and drift, partitions (directional,
  reference-counted), healing, resets, latency faults, invariants.
- Simulated network: TCP-like streams with flow control, half-close, close vs
  abort, partition stalls with retransmission and timeouts, process vs host
  crash modes; servers; simulated sockets for connection setup; a lossy,
  reordering message bus.
- Determinism guards for time, `random`, entropy, sockets, threads,
  subprocesses and TLS, restored on every exit path.
- Operation histories and a bounded linearizability checker (valid / invalid /
  unknown) with register and key-value models and an extension interface.
- Exploration across worker processes with deterministic aggregation, SQLite
  results, JSONL traces and replay manifests.
- Delta-debugging shrinker over faults and decision tapes with strict-replay
  verification.
- `@detloop.simulate` pytest integration (`--detloop-seed`,
  `--detloop-replay`, `--detloop-shrink`, ...).
- `detloop explore | replay | shrink | report` CLI with stable exit codes and
  JSON summaries.
- Self-contained offline HTML report with a hash-pinned CSP.
- Toy Raft-style replicated KV store with three planted bugs (lost
  acknowledged writes, stale reads, split brain) and simulation tests.
- Compatibility tests for aiohttp (client and server) and httpx over the
  simulated network.

[0.1.0]: https://github.com/raunaksahahere/detloop/releases/tag/v0.1.0
