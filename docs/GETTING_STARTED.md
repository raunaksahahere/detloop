# Getting started

This walkthrough uses only commands and files that ship with detloop. Every
snippet here is exercised by the test suite (`tests/test_examples.py`,
`tests/test_cli.py`, `tests/test_pytest_plugin.py`).

## 1. Install

```console
pip install detloop
pip install pytest          # for the pytest integration
```

## 2. Write a simulation test

A simulation test is an `async def` function decorated with
`@detloop.simulate`. Its first parameter is the [`World`](API.md#world):

<!-- example: examples/getting_started/test_echo.py -->
```python
import asyncio
import detloop

async def echo_server(node):
    async def handle(reader, writer):
        while data := await reader.read(1024):
            writer.write(data)
            await writer.drain()
        writer.close()
    server = await asyncio.start_server(handle, port=9000)
    await server.serve_forever()

@detloop.simulate(nodes=2, seeds=50, faults="partitions")
async def test_echo(world):
    world.start(echo_server, nodes=["n1"])     # run echo_server(node) on n1
    await asyncio.sleep(0.1)                   # virtual time: instant
    reader, writer = await asyncio.open_connection("n1", 9000)
    writer.write(b"ping")
    assert await reader.readexactly(4) == b"ping"
    writer.close()
```

Things to notice:

- `world.start(factory, nodes=[...])` runs `await factory(node)` as the node's
  main task. On restart the same factory runs again with fresh volatile state.
- Nodes are `n1`, `n2`, ... with addresses `10.0.0.1`, `10.0.0.2`, ... The test
  body itself runs on a non-crashable `control` node (`10.0.0.254`).
- `asyncio.sleep(0.1)` costs no wall-clock time; virtual time jumps forward
  when nothing is runnable.
- The test is deterministic: seed 7 always produces the same execution.

## 3. Run it with pytest

```console
pytest test_echo.py                       # seeds 0..49
pytest test_echo.py --detloop-seeds=500   # more seeds
pytest test_echo.py --detloop-seed=7      # exactly one seed
```

A failing seed raises `detloop.SimulationFailure` with the outcome, the
artifact paths and the exact replay command. Artifacts go to
`<rootdir>/.detloop/artifacts/<test>/seed-<N>/` (`--detloop-artifacts=DIR` to
change, `--detloop-no-artifacts` to disable).

## 4. Explore at scale

```console
detloop explore test_echo.py --seeds 10000 --jobs 8
```

`explore` imports the file, finds every `@detloop.simulate` test (or the one
named with `file.py::test_name`), and runs the seed range in worker
processes. Results are aggregated per seed into `.detloop/results.sqlite`;
failing seeds get a `trace.jsonl` and a `manifest.json`. The summary reports
the **canonical first failure** (lowest failing seed), which does not depend
on `--jobs`.

## 5. Replay, shrink, report

```console
detloop replay .detloop/artifacts/test_echo/seed-17/manifest.json
detloop shrink .detloop/artifacts/test_echo/seed-17/manifest.json
detloop report .detloop/artifacts/test_echo/seed-17-shrunk/manifest.json -o report.html
```

- `replay` checks that the code and configuration still match the manifest,
  reruns the seed, and reports whether the failure signature and the trace
  hash were reproduced.
- `shrink` removes faults and simplifies recorded decisions while preserving
  the failure signature, then saves a manifest with the decision tapes so the
  shrunk run replays in *strict* mode.
- `report` writes a self-contained HTML timeline.

## 6. Faults and invariants

```python
@detloop.simulate(nodes=3, seeds=200, faults="partitions,crashes")
async def test_cluster(world):
    world.start(my_node_main)
    world.invariant("one-leader", lambda: count_leaders() <= 1)   # checked every 50 ms
    ...
    world.partition("n1", ["n2", "n3"])     # explicit faults are fine too
    await asyncio.sleep(2)
    world.heal()
    world.crash("n2", restart_after=1.0)
```

Random faults are generated from the seed before the run starts (see
[FAULT_MODEL.md](FAULT_MODEL.md)); the schedule is printed in the manifest
and shown in the report.

## 7. Check linearizability

```python
from detloop.checker import KVModel, assert_linearizable

op = world.history.invoke("client-1", "put", key="x", value=1)
...
op.ok()            # completed
op.fail("error")   # definitely did not take effect
op.info("timeout") # may or may not have taken effect
...
assert_linearizable(world.history, KVModel())
```

`assert_linearizable` raises `LinearizabilityViolation` for a definite
violation and `CheckIncomplete` when its search budget runs out; an
incomplete check is reported as *incomplete*, never as a pass.

## 8. Next steps

- The full API: [API.md](API.md). Command reference: [CLI.md](CLI.md).
- What is and is not deterministic: [DETERMINISM.md](DETERMINISM.md).
- A complete worked example with planted bugs:
  [`src/detloop/examples/kv_sim.py`](../src/detloop/examples/kv_sim.py).
