# API reference

Everything listed here is importable from `detloop` unless noted. Names not
listed are internal and may change without notice.

## `@detloop.simulate`

```python
detloop.simulate(fn=None, *, nodes=3, seeds=100, seed_start=0, faults=None,
                 scheduler="interleave", **config)
```

Turns `async def test(world, *extra)` into a synchronous test function.

| Argument | Default | Meaning |
|---|---|---|
| `nodes` | `3` | Number of nodes (`n1..nN`) or a list of names (letters, digits, `-`, `_`; `control` and `localhost` are reserved; at most 250) |
| `seeds` | `100` | Seeds a pytest run executes: `seed_start .. seed_start+seeds-1` |
| `seed_start` | `0` | First seed |
| `faults` | `None` | Comma-separated kinds: `partitions`, `crashes`, `resets`, `latency`, `clock`, `messages`; `"all"`; `None`/`"none"` |
| `scheduler` | `"interleave"` | `interleave`, `fifo`, or opt-in `relaxed` (see [DETERMINISM.md](DETERMINISM.md#schedulers)) |
| `**config` | | Any [`SimConfig`](#simconfig) field, `section.field`, or a field name unique across sections (e.g. `max_virtual_time=60`, `latency_max=0.05`, `crash_mode="host"`) |

Extra parameters after `world` are passed through, so pytest fixtures and
`@pytest.mark.parametrize` work. Fixtures are evaluated outside the
simulation (async fixtures of other plugins do not run inside it). Tests with
extra parameters are skipped by `detloop explore`.

**Registration and discovery.** The wrapper carries a `Spec` in
`__detloop_spec__` (see `detloop.get_spec(obj)`). pytest collects the wrapper
like any `test_*` function; `detloop explore path.py` collects every
module-level object with a spec, in definition order.

**Configuration precedence** (highest first): `--detloop-*` pytest options
or CLI flags → decorator arguments → `SimConfig` defaults.

## `detloop.run`

```python
detloop.run(fn, *, seed, config=None, args=(), kwargs=None, faults=None,
            tapes=None, decision_mode=None, keep_trace=False, trace_level=None) -> RunResult
```

Runs `await fn(world, *args, **kwargs)` once in a fresh world. `faults`
replaces the generated fault schedule. `tapes` with `decision_mode="strict"`
replays recorded decisions exactly (divergence is an error); `"guided"` uses
them as hints (shrinking). Cannot be called from inside a running event loop.

### `RunResult`

| Field | Meaning |
|---|---|
| `status` | `pass`, `fail`, `incomplete` (budget exhausted, checker unknown, wall timeout), `error` (internal error, replay divergence) |
| `kind` | `pass`, `assertion`, `invariant`, `linearizability`, `exception`, `node_error`, `deadlock`, `starvation`, `nondeterminism`, `unsupported`, `budget`, `checker_unknown`, `wall_timeout`, `internal_error`, `replay_divergence` |
| `signature` | Stable failure identity used by replay and shrinking, e.g. `invariant:election-safety`, `assertion:test_x.py:39`, `budget:steps` |
| `message`, `traceback`, `details` | Human-readable failure information (`details` lists blocked tasks for deadlocks) |
| `steps`, `virtual_ns` | Handles executed and final virtual time |
| `trace_sha256`, `trace_events`, `trace_lines` | Canonical trace hash, event count, and lines (with `keep_trace=True`) |
| `faults`, `tapes`, `decisions` | The fault schedule and recorded decision tapes |
| `loop_errors`, `warnings` | Exceptions that reached the loop's exception handler; teardown/leak notes |
| `wall_seconds` | Wall-clock duration (metadata, not canonical) |

## `World`

Passed to the test function. Created fresh per run.

### Nodes

| Member | Description |
|---|---|
| `world.nodes` | `dict[str, Node]` in configuration order; `world["n1"]`, `world.node("n1")` |
| `world.control` | The control node (`10.0.0.254`), which runs the test body; cannot crash |
| `world.node_names` | Configured node names |
| `world.add_node(name)` | Add a node at runtime (e.g. a client host); not targeted by random faults |
| `world.start(factory, *args, nodes=None)` | `await factory(node, *args)` on every node or the listed ones |
| `world.crash(node, restart_after=None)` | Crash now; optionally restart after a virtual delay |
| `world.restart(node)` | New incarnation with the stored factory (crashes first if running) |
| `world.spawn(coro)` | Run a control-owned task |
| `world.current_node()` | Node owning the currently running code |

### Network faults

| Member | Description |
|---|---|
| `world.partition(a, b=None, *, oneway=False)` | Block `a↔b` (`a→b` only with `oneway`). `a`/`b` are nodes, names or lists. `b=None` isolates `a` from everyone, including `control` |
| `world.heal(a=None, b=None, *, oneway=False)` | Undo a matching `partition`; `heal()` heals everything. Partitions are reference-counted |
| `world.reset_connections(a, b)` | Reset live connections between two nodes |
| `world.slow(a, b, factor)` | Multiply latency both ways; returns an undo function |

### Invariants and records

| Member | Description |
|---|---|
| `world.check(cond, name, message="")` | Record a violation and fail the run as soon as the current callback returns. Sticky: swallowing exceptions in node code does not hide it |
| `world.fail(name, message="")` | Unconditional `check(False, ...)` |
| `world.invariant(name, predicate, every=0.05)` | Check periodically and at the end of the run |
| `world.history` | The [`History`](#history) of operations |
| `world.rng` | [`SimRandom`](#simrandom): workload randomness on a recorded, shrinkable stream |
| `world.note(kind, **fields)` | Custom trace event (shown in the report) |
| `world.time()`, `world.now_ns` | Virtual monotonic time (same as `loop.time()`) |
| `world.seed`, `world.config`, `world.loop`, `world.network`, `world.trace` | Run context |

## `Node`

| Member | Description |
|---|---|
| `name`, `address`, `generation`, `state` | `state` is `new`, `up`, or `down` |
| `up` | Whether a live incarnation exists |
| `disk` | `dict` that **survives crashes** (no fsync/torn-write model: assignments are durable immediately) |
| `volatile` | Fresh `dict` per incarnation |
| `start(factory, *args)`, `crash()`, `restart()` | Lifecycle |
| `spawn(coro)` | Task owned by the current incarnation |
| `messages(port)` | Bind a [`MessageEndpoint`](#message-bus) |
| `set_clock(offset=None, drift_ppm=None)` | Skew this node's wall clock (`time.time()`); `loop.time()` is unaffected |
| `wall_time()`, `local_time_ns()` | The node's local clock |

Inside node code use ordinary asyncio: `asyncio.start_server(handler,
port=...)` binds the node's address (and `127.0.0.1`), and
`asyncio.open_connection("n2", port)` resolves node names.

## Message bus

`ep = node.messages(port)`; `ep.send(dst_name_or_address, port, payload_bytes)`;
`src, payload = await ep.recv()`; `ep.recv_nowait()`; `ep.close()`.
Messages draw independent latencies (may reorder); with the `messages` fault
kind they can be lost or duplicated (`msg_loss`, `msg_duplicate`,
`msg_reorder_jitter`). This is a detloop API, not `create_datagram_endpoint`
(which is unsupported in 0.1.0).

## History

```python
op = world.history.invoke(client, f, key=None, value=None)
op.ok(output=None) | op.fail(error=None) | op.info(error=None)
async with world.history.op(client, f, key, value) as op: ...   # info on exception
```

`fail` means the operation definitely did not take effect; `info` means it may
have. Operations never completed are `pending`. Real-time order uses a
strictly increasing logical index. `History()` also works standalone.

## Checkers (`detloop.checker`)

```python
check_linearizable(history_or_ops, model, *, max_steps=2_000_000, max_cache=1_000_000) -> CheckResult
assert_linearizable(...)  # raises LinearizabilityViolation / CheckIncomplete
```

- `CheckResult.status` is `valid`, `invalid` or `unknown`; `partitions`
  contain per-key results with the longest linearized prefix and the operation
  that could not be linearized.
- `RegisterModel(initial=None)`: `read`/`get`, `write`/`put`, `cas` with
  `value=(expected, new)` and boolean output.
- `KVModel(initial=None)`: independent registers per `op.key` (checked per key).
- **Extension interface:** subclass `Model` and implement `init()` and
  `step(state, op)`, returning *every* possible successor state for the
  observed `op.output` (`UNKNOWN` for indeterminate operations; never return
  an empty list then). Optionally implement `partition(ops)` (only for truly
  independent sub-histories) and `is_readonly(op)`.

## SimConfig

Frozen dataclasses: `SimConfig(nodes, scheduler, network, faults, budgets,
trace, entropy, executor, fail_on_node_error, fail_on_loop_errors, epoch)`
with `NetworkConfig`, `FaultConfig` and `Budgets`. Defaults are listed in
`src/detloop/config.py` and summarized in [FAULT_MODEL.md](FAULT_MODEL.md).
Use `config.with_overrides(**{"network.rto": 0.1, "max_steps": 10_000})`.
`config.fingerprint()` hashes every semantic field (the wall timeout is
excluded).

Notable fields: `trace` (`off`/`basic`/`full`; never changes execution),
`entropy` (`strict` rejects `os.urandom`/`uuid4`/`secrets`; `seeded` makes them
deterministic), `executor` (`reject`, or opt-in `inline` running executor
functions synchronously), `crash_mode` (`process` or `host`).

## SimRandom

`world.rng.randrange/randint/choice/random/uniform/chance/getrandbits/shuffle/sample`.
Every draw is a recorded decision on the `workload` stream; zero is the
"simplest" value (the shrinker pushes decisions toward it).

## Errors

`DetloopError` ⊃ `ConfigError`, `TargetError`, `InternalError`,
`ReplayError` (⊃ `ReplayDivergence`, `ReplayMismatch`), and `SimulationError`
(⊃ `NondeterminismError`, `UnsupportedOperation`, `DeadlockError`,
`StarvationError`, `BudgetExceeded`, `InvariantViolation`, `NodeFailure`,
`LinearizabilityViolation`, `CheckIncomplete`, `WallClockTimeout`).
`SimulationFailure` (an `AssertionError`) is what pytest reports.

## Lower-level pieces

- `detloop.SimLoop` can drive plain coroutines through
  `asyncio.Runner(loop_factory=lambda: SimLoop())` (no network attached).
- `detloop.explore.explore`, `detloop.shrink.shrink`,
  `detloop.replay.replay_manifest` and `detloop.report.render_report` back the
  CLI and are usable from Python.
