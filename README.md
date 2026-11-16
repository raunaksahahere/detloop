# detloop

**Deterministic simulation testing for Python asyncio services.** Run your
cooperating asyncio services inside a simulated world with virtual time,
seeded scheduling, a simulated network, partitions, crashes and restarts;
check invariants and linearizability; explore thousands of seeds; shrink a
failure and replay it exactly from its seed.

detloop runs ordinary asyncio code — `asyncio.start_server`,
`asyncio.open_connection`, tasks, timeouts, aiohttp and httpx — on a custom
event loop where every source of nondeterminism is either controlled by a
seed or rejected loudly.

> Status: **0.1.0, alpha.** The core is complete and tested on CPython
> 3.11–3.14 (Linux, macOS, Windows in CI), but the API may still change and
> the simulation boundary is deliberately narrow. Read
> [the determinism boundary](#what-detloop-can-and-cannot-prove) before
> relying on it.

## Install

```console
pip install detloop
```

No runtime dependencies beyond the standard library. The pytest plugin is
registered automatically when pytest is installed.

## A 30-line example that finds a race

A counter server and two clients that each read the value and write it back
plus one. Under real asyncio this usually "works"; detloop explores message
timings and finds the lost update:

<!-- example: examples/quickstart/test_counter.py -->
```python
import asyncio

import detloop


async def counter_server(node):
    value = 0

    async def handle(reader, writer):
        nonlocal value
        while line := await reader.readline():
            cmd, *arg = line.split()
            if cmd == b"SET":
                value = int(arg[0])
            writer.write(b"%d\n" % value)
            await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, port=7000)
    await server.serve_forever()


async def increment(host):
    reader, writer = await asyncio.open_connection(host, 7000)
    writer.write(b"GET\n")
    current = int(await reader.readline())
    writer.write(b"SET %d\n" % (current + 1))  # read-modify-write race
    await reader.readline()
    writer.close()


@detloop.simulate(nodes=1, seeds=200)
async def test_concurrent_increments(world):
    world.start(counter_server)
    await asyncio.sleep(0.1)
    await asyncio.gather(increment("n1"), increment("n1"))
    reader, writer = await asyncio.open_connection("n1", 7000)
    writer.write(b"GET\n")
    assert int(await reader.readline()) == 2, "lost update"
    writer.close()
```

```console
$ pytest test_counter.py
...
E   detloop.errors.SimulationFailure: detloop: seed 0 fail (assertion:test_counter.py:39) after 1 of 200 seed(s)
E     AssertionError: lost update
E   assert 1 == 2
E    +  where 1 = int(b'1\n')
E     virtual time 0.149417s, 78 steps, 0 scheduled fault(s)
E     manifest: <rootdir>/.detloop/artifacts/test_concurrent_increments/seed-0/manifest.json
E     trace:    <rootdir>/.detloop/artifacts/test_concurrent_increments/seed-0/trace.jsonl
E     replay:   pytest test_counter.py::test_concurrent_increments --detloop-seed=0
E               detloop replay <rootdir>/.detloop/artifacts/test_concurrent_increments/seed-0/manifest.json
E     report:   detloop report <rootdir>/.detloop/artifacts/test_concurrent_increments/seed-0/manifest.json -o detloop-report.html
```

(Output from CPython 3.12; the failing seed number can differ on another
Python minor version, see [determinism](docs/DETERMINISM.md).) The fixed
version, [`test_counter_fixed.py`](examples/quickstart/test_counter_fixed.py),
increments atomically on the server and passes all 200 seeds even with
latency faults.

The decorated test is a normal (synchronous) pytest test; the function body
receives a [`World`](docs/API.md#world). The node `n1` gets address
`10.0.0.1` and a resolvable name; the test body runs on a `control` node.

## Find, replay, shrink, report

```console
# explore 10,000 seeds on 8 worker processes
detloop explore tests/sim_test.py --seeds 10000 --jobs 8

# replay the canonical first failure exactly (same trace hash)
detloop replay .detloop/artifacts/test_x/seed-17/manifest.json --expect-reproduce

# minimize it (fewer faults, simpler decisions) and save a strict-replay manifest
detloop shrink .detloop/artifacts/test_x/seed-17/manifest.json

# self-contained offline HTML: node timeline, partitions, crashes, operations
detloop report .detloop/artifacts/test_x/seed-17-shrunk/manifest.json -o report.html
```

The same failure replays inside pytest with `--detloop-seed=17`, or with
`--detloop-replay=<manifest>` for a shrunk run whose decisions are recorded.

The report is one HTML file with no external assets: a per-node timeline
(up/crash intervals, partitions, faults, custom notes), an operation history
chart, checker results, and a filterable, linked event table. Trace text is
embedded as inert JSON under a hash-pinned Content-Security-Policy.

## What it simulates

| Area | Behaviour |
|---|---|
| Time | Integer-nanosecond virtual clock; jumps to the next timer only when nothing is runnable |
| Scheduling | Each node behaves like its own event loop (asyncio FIFO order preserved per node); a seeded decision picks which node runs next |
| Network | TCP-like ordered streams with latency/jitter, bandwidth, receiver windows, `drain()` backpressure, half-close, resets |
| Faults | Directed/bidirectional partitions, crashes with restart, connection resets, latency spikes, local clock skew/drift, lossy message bus |
| Crashes | Node generations: a crashed node's tasks never run again, even if they suppress cancellation |
| Checking | `world.check`/`world.invariant`, operation histories, bounded linearizability checker (valid / invalid / unknown) |
| Exploration | Serial or multi-process seed ranges with deterministic aggregation, SQLite results, JSONL traces |
| Replay | Seed replay with environment checks; strict decision-tape replay that detects divergence |

See [docs/FAULT_MODEL.md](docs/FAULT_MODEL.md) and [docs/DETERMINISM.md](docs/DETERMINISM.md).

## The bundled bug corpus

`detloop.examples.kvstore` is a small Raft-style replicated KV store (a
teaching system, not production Raft) with three independently selectable
planted bugs. Exploration finds each one; the corrected baseline passes:

```console
detloop explore detloop.examples.kv_sim::test_kv_split_brain --seeds 300 --jobs 8
```

| Variant | Detected by | Failing seeds of 300 (CPython 3.12) | First failing seed |
|---|---|---|---|
| `stale-read` | linearizability checker | 199 (66%) | 1 |
| `lost-ack` | linearizability checker | 44 (15%) | 0 |
| `split-brain` | election-safety invariant | 16 (5%) | 17 |
| corrected | — | 0 | — |

Each failure replays with an identical trace and can be shrunk. The same
workload on real asyncio with random sleeps finds these bugs at similar rates
per trial but costs ~6.5 s per trial (vs ~0.11 s per simulated seed) and
cannot replay what it finds; see [benchmarks](docs/BENCHMARKS.md).

## Supported environments

| | |
|---|---|
| Python | CPython 3.11, 3.12, 3.13, 3.14 |
| OS | Linux, macOS, Windows (CI) |
| Libraries tested over simulated transports | aiohttp 3.14 (client and server), httpx 0.28 (AsyncClient) |
| Not supported | TLS, UNIX sockets, datagram endpoints, subprocesses, threads/executors (rejected), uvloop, PyPy |

Details: [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md).

## What detloop can and cannot prove

- **Code inside the simulated loop is deterministic** for a given seed,
  configuration, code and runtime. A replay that cannot be trusted is rejected
  or flagged, never silently "fixed".
- **It does not prove correctness.** It finds failures among the schedules and
  faults it explores. A passing run means "no failure found in these seeds".
- **Threads, executors, subprocesses, real sockets, TLS and external services
  are outside the boundary.** detloop rejects the routes it knows about
  (`run_in_executor`, `threading.Thread.start`, `socket.create_connection`,
  `os.urandom`, `time.sleep`, ...). Python monkeypatching is not a sandbox:
  C extensions, cached aliases (`from time import monotonic`) and
  `datetime.now()` are not intercepted.
- **Shared module globals** persist between seeds in one process; keep test
  state inside the test function or in `node.disk` / `node.volatile`.
- **Byte-identical traces across Python versions are not promised.** Different
  asyncio versions schedule different internal callbacks. CI compares trace
  hashes of declared portable fixtures across jobs; see
  [docs/DETERMINISM.md](docs/DETERMINISM.md).

## Documentation

- [Getting started](docs/GETTING_STARTED.md) · [API](docs/API.md) · [CLI](docs/CLI.md)
- [Architecture](docs/ARCHITECTURE.md) · [Determinism](docs/DETERMINISM.md) · [Fault model](docs/FAULT_MODEL.md)
- [Trace format](docs/TRACE_FORMAT.md) · [Compatibility](docs/COMPATIBILITY.md) · [Troubleshooting](docs/TROUBLESHOOTING.md)
- [Testing](docs/TESTING.md) · [Benchmarks](docs/BENCHMARKS.md) · [Design decisions](docs/DESIGN_DECISIONS/README.md)
- [Roadmap](docs/ROADMAP.md) · [Implementation status](docs/IMPLEMENTATION_STATUS.md) · [Tech stack](docs/TECH_STACK.md)
- [Releasing](docs/RELEASING.md) · [Maintenance](docs/MAINTENANCE.md) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md) · [Changelog](CHANGELOG.md)

## License

MIT. See [LICENSE](LICENSE).
