# Determinism

This document is the replay contract: what is guaranteed, what is detected on
a best-effort basis, and what is outside detloop's control.

## The guarantee

For a fixed **seed**, **configuration** (fingerprint), **code under test**
(workload fingerprint), **detloop version**, **Python minor version** and
**hash seed**, a run executes the same sequence of callbacks and produces the
same canonical trace bytes, in any process, on any worker, regardless of
`--jobs`, provided the code stays inside the simulation boundary described
below.

Evidence (see [TESTING.md](TESTING.md)): fresh-process runs under
`PYTHONHASHSEED` 0, 1 and 4242 produce identical trace hashes; serial and
parallel exploration agree on every seed's status, signature and hash; trace
level never changes execution; strict replay of recorded decisions reproduces
the trace hash.

## Time

- Virtual time is an **integer number of nanoseconds**, starting at 0.
  `loop.time()` returns `ns / 10**9` (exact for runs shorter than ~104 days).
- `call_later(delay)` schedules at `now + round(delay * 1e9)`;
  `call_at(when)` at `round(when * 1e9)`. Infinite delays never fire.
- Virtual time advances **only when no handle is runnable**, jumping to the
  earliest live timer. Zero-delay sleeps and `call_soon` never advance it.
- `time.monotonic()`/`perf_counter()` return virtual monotonic time inside a
  simulation; `time.time()` returns the *current node's* wall clock:
  `epoch + virtual time + offset + drift`. Local clock skew never affects
  `loop.time()` or scheduling.

## Scheduling and ordering rules

asyncio guarantees that callbacks registered with `call_soon` run in
registration order. detloop preserves that guarantee **per owner** (the
control context or one node incarnation):

1. Every handle has an owner, taken from a context variable that asyncio
   itself propagates into tasks, callbacks and future done-callbacks.
2. Each owner behaves like its own event loop. Its *iteration* starts by
   appending its due timers after its already-queued handles; its *batch* is
   everything queued at that moment; handles scheduled during the batch run
   in its next iteration. This is exactly `BaseEventLoop._run_once`'s order,
   per owner.
3. Timers that become due at the same instant enter their owners' queues in
   registration order.
4. **The decision**: when more than one owner has runnable work, a recorded
   choice on the `sched` stream picks which owner runs the next handle.
   Candidates are ordered by the sequence number of their oldest pending
   handle, so choice `0` is "global FIFO".

### Schedulers

| Mode | Semantics | Use |
|---|---|---|
| `interleave` (default) | Per-owner iterations; seeded choice of owner | Explores cross-node interleavings without violating asyncio guarantees within a node |
| `fifo` | One global iteration structure, identical to `BaseEventLoop._run_once` | Single-loop fidelity; cross-node order fixed |
| `relaxed` (opt-in) | `interleave` plus a seeded choice of *any* handle in the chosen owner's current batch | Violates asyncio's FIFO guarantee. Failures found only in this mode may be impossible on real asyncio; report the mode with any bug |

## Decision streams and PRNG (`detloop-prng/1`)

- Streams: `sched` (scheduling), `net` (latency, message loss/duplication,
  jitter), `workload` (`world.rng`), `faults` (fault-schedule generation),
  `entropy` (seed for the patched `random` module and seeded `os.urandom`).
- Stream seed: first 8 bytes, big-endian, of
  `SHA-256("detloop/prng/v1/<seed>/<stream>")`. Python's `hash()` is never used.
- Generator: SplitMix64. Bounded choice in `[0, n)`: rejection sampling on
  64-bit outputs with `limit = 2**64 - 2**64 mod n`, result `x mod n`.
  `n == 1` consumes nothing and is not recorded.
- Every taped decision is recorded as `(n, value)`. Value `0` is always the
  least eventful option (minimum latency, no loss, first candidate,
  `random() == 0.0`).
- Streams are independent, so removing a fault does not shift workload or
  network draws positionally.

## Replay modes

| Mode | Input | Behaviour |
|---|---|---|
| Seed replay | seed + config | Regenerates everything; compares the trace hash with the manifest |
| Strict decision replay | recorded tapes | Every decision must offer the recorded number of options; otherwise `ReplayDivergence` (outcome `error`/`replay_divergence`). Running past the tape is also divergence. Used for shrunk failures |
| Guided | candidate tapes | Shrinker only: out-of-range values are reduced modulo `n`, missing values are `0` |

## The replay envelope

`detloop replay` and `--detloop-replay` check the manifest before running:

| Mismatch | Handling |
|---|---|
| Manifest schema | Rejected |
| Test module source (SHA-256, newlines normalized) | Rejected unless `--allow-mismatch` |
| Config fingerprint vs. recorded config | Rejected (edited manifest or incompatible version) |
| detloop version | Warning |
| Python minor version | Warning: asyncio internals differ, so identical traces are not guaranteed |
| `PYTHONHASHSEED` | Warning: code that iterates sets of strings may diverge |

A seed alone is meaningful only inside this envelope.

## Canonical traces and hash randomization

- Canonical JSONL: `sort_keys`, compact separators, ASCII, integer times.
- Object addresses (`at 0x...`) and host path prefixes (stdlib,
  site-packages, prefix, cwd, home) are replaced in trace text.
- Messages of exceptions raised by user code are **not** canonical (pytest's
  assertion rewriting changes them); types and signatures are. Messages are
  kept in the manifest.
- Wall-clock durations, PIDs, Python version, platform, hash seed and
  creation time live in the manifest's `metadata`, never in the trace.
- detloop itself never iterates hash-ordered containers in a way that affects
  execution or traces (insertion-ordered dicts only). Workers use a fixed
  `PYTHONHASHSEED` (default `0`, recorded); `--verify-determinism N` re-runs
  seeds under other hash seeds to detect code that depends on string hashing.
- Garbage collection is disabled during a run (after a full collection) so
  cyclic-GC finalizers cannot fire at allocation-dependent moments.

## Cross-runtime equivalence

detloop does **not** promise byte-identical traces across Python minor
versions: CPython 3.11 schedules a different number of internal callbacks than
3.12+ (for example in `TaskGroup`, `wait_for` and server shutdown), which
changes interleaving decisions. CI therefore:

- runs the full suite on 3.11–3.14 × Linux/macOS/Windows;
- computes trace hashes of the **declared portable fixture**
  (`tests/sim_targets.py::test_portable_fixture`, fifo scheduler, seeds 0–19)
  in every job and asserts they are identical across operating systems for the
  same Python version, and reports (without failing) differences across
  Python versions.

Pinned regression seeds are declared per runtime family (3.11 vs 3.12+).

## Inside vs. outside the boundary

| Inside (deterministic) | Rejected with an error | Not detected (your responsibility) |
|---|---|---|
| Coroutines, tasks, futures, timers, `asyncio` synchronization primitives, streams, protocols/transports through the simulated network, `random` module, `time.*` clocks, `socket.socket` for TCP connection setup, `getaddrinfo` | `run_in_executor`/`to_thread` (unless `executor="inline"`), `threading.Thread.start`, subprocesses, `time.sleep`, `os.urandom`/`secrets`/`uuid4`/`SystemRandom` (strict mode), `socket.create_connection`/`socketpair`, non-TCP sockets, TLS, UNIX sockets, datagram endpoints, signal handlers, `add_reader` | `datetime.now()`, aliases cached before the run (`from time import monotonic`), C extensions doing I/O or reading clocks, real files and databases, shared mutable module globals across seeds, iteration over `set`s of strings when the hash seed differs, `id()`-based ordering |

Rejections are **sticky**: the violation fails the run even if the code under
test catches the exception. Python monkeypatching is not a sandbox; strict
mode protects documented paths, not hostile code.

## Supervisory timeouts

The wall-clock watchdog runs outside virtual time. On the main thread it
interrupts even a coroutine that never yields; the explorer additionally kills
a worker process that exceeds `1.5 × wall_timeout + 10 s`. Both outcomes are
`incomplete`/`wall_timeout`, never a pass.
