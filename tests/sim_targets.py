"""Simulation targets used by the CLI/explorer/determinism tests (importable by workers)."""

from __future__ import annotations

import asyncio

import detloop

PORT = 7000


async def _echo(node: detloop.Node) -> None:
    async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        try:
            while data := await r.read(4096):
                w.write(data)
                await w.drain()
        except ConnectionError:
            pass
        finally:
            w.close()

    await (await asyncio.start_server(handle, port=PORT)).serve_forever()


@detloop.simulate(nodes=2, seeds=20, faults="partitions,crashes,resets,latency", scheduler="fifo")
async def test_portable_fixture(world: detloop.World) -> None:
    """Declared portable fixture: traffic, faults and history, with the fifo
    scheduler; its trace hashes are compared across CI jobs."""
    world.start(_echo)
    await asyncio.sleep(0.05)
    for i in range(6):
        op = world.history.invoke("c", "echo", value=i)
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection(world.rng.choice(world.node_names), PORT), 1
            )
            w.write(str(i).encode() * 100)
            await asyncio.wait_for(r.readexactly(len(str(i)) * 100), 2)
            op.ok(i)
            w.close()
        except (OSError, TimeoutError) as exc:
            op.info(exc)
        await asyncio.sleep(world.rng.uniform(0.1, 1.0))


@detloop.simulate(nodes=3, seeds=30, faults="partitions")
async def test_racy_counter(world: detloop.World) -> None:
    """A lost-update race between nodes: fails on some interleavings only."""
    shared = {"value": 0}

    async def incr(node: detloop.Node) -> None:
        v = shared["value"]
        await asyncio.sleep(0)  # yield between read and write
        shared["value"] = v + 1

    world.start(incr)
    await asyncio.sleep(1)
    assert shared["value"] == 3, f"lost update: {shared['value']}"


@detloop.simulate(nodes=1, seeds=5)
async def test_always_passes(world: detloop.World) -> None:
    await asyncio.sleep(1)


@detloop.simulate(nodes=1, seeds=3, wall_timeout=1.0)
async def test_cpu_hog(world: detloop.World) -> None:
    if world.seed == 1:
        while True:  # never yields: needs the supervisory timeout
            pass
    await asyncio.sleep(0.1)


@detloop.simulate(nodes=1, seeds=2)
async def test_with_fixture(world: detloop.World, tmp_path: object) -> None:
    await asyncio.sleep(0)
