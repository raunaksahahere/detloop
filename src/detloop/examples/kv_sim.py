"""detloop simulation tests for the toy replicated KV store.

Each test runs three KV nodes plus three concurrent clients for a few virtual
seconds under random partitions and crashes, then heals the network, performs
final reads and checks the history for linearizability. The election-safety
invariant ("at most one leader per term") is checked live.

Run them with pytest (``pytest --pyargs detloop.examples.kv_sim``) or explore:

    detloop explore detloop.examples.kv_sim::test_kv_stale_read --seeds 200 --jobs 4

The ``test_kv_<bug>`` variants contain a planted bug and are *expected to fail*;
``test_kv_correct`` is the corrected baseline.
"""

from __future__ import annotations

import asyncio
import detloop
from detloop.checker import KVModel, assert_linearizable
from detloop.examples.kvstore import KVClient, KVConfig, Observer, run_node

PORT = 7000
KEYS = ("a", "b", "c", "d")
CLIENTS = 3
WRITE_RATIO = 0.3
DURATION = 4.0


async def scenario(world: detloop.World, bugs: tuple[str, ...]) -> None:
    """Run the KV workload against nodes with the given planted ``bugs``."""
    if True:  # noqa: SIM108 - keeps the scenario body indented as one block
        peers = {name: (name, PORT) for name in world.node_names}
        cfg = KVConfig(peers=peers, bugs=frozenset(bugs))
        leaders: dict[int, str] = {}

        def on_leader(node: str, term: int) -> None:
            first = leaders.setdefault(term, node)
            world.note("leader", node=node, term=term)
            world.check(first == node, "election-safety", f"term {term} has two leaders: {first} and {node}")

        observer = Observer(on_leader=on_leader)

        async def node_main(node: detloop.Node) -> None:
            await run_node(node.name, cfg, node.disk, observer)

        world.start(node_main)
        counter = 0

        async def client_loop(client: KVClient) -> None:
            nonlocal counter
            end = world.time() + DURATION
            while world.time() < end:
                key = world.rng.choice(KEYS)
                if world.rng.random() < WRITE_RATIO:
                    counter += 1
                    await client.op("put", key, f"{client.name}-{counter}")
                else:
                    await client.op("get", key)
                await asyncio.sleep(world.rng.uniform(0.0, 0.05))

        clients = [KVClient(f"c{i}", peers, world.history, world.rng) for i in range(CLIENTS)]
        await asyncio.gather(*(client_loop(c) for c in clients))

        # Heal everything, restart crashed nodes, let the cluster settle, and
        # read every key through the leader.
        world.heal()
        for n in world.nodes.values():
            if not n.up:
                n.restart()
        await asyncio.sleep(2.0)
        final = KVClient("final", peers, world.history, world.rng)
        for key in KEYS:
            for _ in range(10):
                before = len(world.history.ops)
                await final.op("get", key)
                if world.history.ops[before].status == "ok":
                    break
                await asyncio.sleep(0.5)
        assert_linearizable(world.history, KVModel())


FAULTS = "partitions,crashes"
#: Faults are injected while clients are active (the network is healed afterwards).
SIM = {"nodes": 3, "faults": FAULTS, "window_start": 0.2, "window_end": DURATION, "max_faults": 4}


@detloop.simulate(seeds=50, **SIM)
async def test_kv_correct(world: detloop.World) -> None:
    """Corrected baseline: must pass every seed."""
    await scenario(world, ())


@detloop.simulate(seeds=50, **SIM)
async def test_kv_lost_ack(world: detloop.World) -> None:
    """Planted bug: acknowledge writes before majority replication."""
    await scenario(world, ("lost-ack",))


@detloop.simulate(seeds=50, **SIM)
async def test_kv_stale_read(world: detloop.World) -> None:
    """Planted bug: serve reads from local state on any node."""
    await scenario(world, ("stale-read",))


@detloop.simulate(seeds=50, **SIM)
async def test_kv_split_brain(world: detloop.World) -> None:
    """Planted bug: grant votes to several candidates in one term."""
    await scenario(world, ("split-brain",))
