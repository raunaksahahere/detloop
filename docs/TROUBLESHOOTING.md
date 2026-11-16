# Troubleshooting

## The run "hangs"

Simulated runs cannot hang on virtual time: idle periods are skipped. Real
wall-clock hangs come from code that never yields (a CPU-bound loop or a
blocking C call).

- The supervisory watchdog (`wall_timeout`, default 60 s) interrupts it and
  reports `incomplete` / `wall_timeout`. Lower it with
  `--wall-timeout 10` or `@detloop.simulate(wall_timeout=10)`.
- Under `detloop explore --jobs N` a worker that ignores the interrupt is
  killed after `1.5 × wall_timeout + 10 s`.
- If you see `budget:virtual_time`, something kept scheduling timers until the
  virtual-time budget (600 s) ran out — typically the test waits for a
  condition that never happens while heartbeats keep ticking. Raise
  `max_virtual_time` only if the test genuinely needs more simulated time.

## Deadlock

`DeadlockError` means no callback is runnable, no timer is pending and the test
has not finished. The message lists every live blocked task with its await
chain and the file:line where it is suspended. Common causes:

- Waiting for a reply from a crashed or partitioned node without a timeout.
  Real systems need timeouts; add `asyncio.timeout(...)`.
- End-to-end backpressure: writing a large payload and awaiting `drain()`
  before reading the reply (both sides block, exactly like real TCP).
- Awaiting a future that only a crashed node would have resolved.

## Starvation / step budget

- `starvation`: more than `max_steps_without_progress` (200,000) steps ran
  without virtual time advancing — a `while True: await asyncio.sleep(0)` loop
  or a callback that reschedules itself forever.
- `budget:steps`: the total step budget (2,000,000) ran out while time was
  advancing. The run is *incomplete*, never a pass. Shorten the workload or
  raise `max_steps`.

## `NondeterminismError` / unsupported I/O

The message names the API. Typical fixes:

| Message mentions | Fix |
|---|---|
| `run_in_executor`, `to_thread`, starting a thread | Make the call synchronous, or `executor="inline"` (runs it on the simulation thread) |
| `time.sleep` | `await asyncio.sleep(...)` |
| `os.urandom`, `uuid4`, `secrets` | Use `world.rng`, or `entropy="seeded"` |
| `socket.create_connection`, blocking `connect` | Use `asyncio.open_connection` |
| TLS | Use plaintext between simulated nodes |
| subprocess | Not simulated; replace with a simulated node |

Violations are sticky on purpose: catching the exception does not make the run
pass.

## Replay drift

`detloop replay` prints why a replay may not match:

- **"the test module changed"** — the source fingerprint differs; replay is
  refused unless `--allow-mismatch`. Re-run exploration on the new code.
- **"recorded on Python X, replaying on Y"** — asyncio internals differ
  between minor versions; signature and trace may differ.
- **"PYTHONHASHSEED"** — workers use `PYTHONHASHSEED=0` by default. If your code
  iterates sets of strings, run the replay with the same value
  (`PYTHONHASHSEED=0 detloop replay ...`) or remove the dependency on set
  order. `detloop explore --verify-determinism 20` finds such code.
- **Trace differs but signature matches** — usually module-level state that
  carried over from a previous seed in the same process, or reading
  something outside the boundary (`datetime.now()`, a file). Try
  `--fresh-process` to confirm.
- **`ReplayDivergence`** (strict replays of shrunk runs) — the program asked
  for a different decision than recorded. The code or the runtime changed.

## Incomplete checker results

`CheckIncomplete` / `checker_unknown` means the linearizability search hit its
budget (`max_steps`, `max_cache`). Options: check fewer operations per key,
use more keys (the KV model checks keys independently), avoid long runs of
concurrent indeterminate writes, or raise the budget. An unknown result is
never treated as a pass.

## pytest plugin conflicts

- The plugin registers through the `pytest11` entry point. Do not also pass
  `-p detloop.pytest_plugin` (pytest reports a double registration).
- `@detloop.simulate` tests are synchronous functions, so pytest-asyncio
  (strict or auto mode) leaves them alone. Async fixtures from other plugins
  run outside the simulation and cannot share its loop.
- `detloop.run()` cannot be called from inside a running event loop (for
  example from an `async def` test); call it from a synchronous test.

## httpx opens a connection per request

Documented limitation: httpcore polls a real file descriptor to check pooled
connections; simulated sockets have none. See
[COMPATIBILITY.md](COMPATIBILITY.md).

## Different results with `--jobs`

They should not happen: per-seed results are independent of the worker count
(tested). If they differ, the test module keeps mutable global state across
runs. Move state into the test function, `node.volatile` or `node.disk`, or use
`--fresh-process`.
