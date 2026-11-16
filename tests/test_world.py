"""Nodes, ownership, crash/restart, clocks, invariants, fault schedules."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from typing import Any

import pytest

import detloop
from detloop.config import FaultConfig
from detloop.faults import Fault, generate
from detloop.rng import SplitMix64

from .conftest import expect_pass, run_sim


def test_crash_freezes_tasks_even_if_they_suppress_cancellation() -> None:
    progress: list[tuple[int, float]] = []

    async def body(world: detloop.World) -> None:
        async def stubborn(node: detloop.Node) -> None:
            async def child() -> None:
                while True:
                    try:
                        progress.append((node.generation, world.time()))
                        await asyncio.sleep(0.1)
                    except asyncio.CancelledError:
                        continue  # refuses to die

            node.spawn(child())
            asyncio.get_running_loop().call_later(0.05, progress.append, (node.generation, -1.0))
            await asyncio.Event().wait()

        world.start(stubborn, nodes=["n1"])
        await asyncio.sleep(1.0)
        world.crash("n1")
        crashed_at = world.time()
        await asyncio.sleep(2.0)
        assert all(t <= crashed_at for g, t in progress if g == 1)
        world.restart("n1")
        await asyncio.sleep(0.5)
        assert any(g == 2 for g, _ in progress)

    res = expect_pass(body)
    assert any('"k":"node_crash"' in line and '"frozen_tasks"' in line for line in res.trace_lines)


def test_descendant_tasks_and_callbacks_inherit_ownership() -> None:
    async def body(world: detloop.World) -> None:
        seen: list[str] = []

        async def node_main(node: detloop.Node) -> None:
            loop = asyncio.get_running_loop()

            async def grandchild() -> None:
                await asyncio.sleep(1)
                seen.append("grandchild")

            async def child() -> None:
                asyncio.create_task(grandchild())
                loop.call_later(1, seen.append, "timer")
                import contextvars

                loop.call_later(1, seen.append, "fresh-context", context=contextvars.Context())
                await asyncio.sleep(10)

            asyncio.ensure_future(child())
            await asyncio.sleep(10)

        world.start(node_main, nodes=["n1"])
        await asyncio.sleep(0.5)
        world.crash("n1")
        await asyncio.sleep(2)
        assert seen == []

    expect_pass(body)


def test_disk_survives_crash_volatile_does_not() -> None:
    async def body(world: detloop.World) -> None:
        async def node_main(node: detloop.Node) -> None:
            node.disk["boots"] = node.disk.get("boots", 0) + 1
            assert "token" not in node.volatile
            node.volatile["token"] = node.generation
            await asyncio.Event().wait()

        n1 = world["n1"]
        n1.start(node_main)
        await asyncio.sleep(0.1)
        for _ in range(3):
            world.crash(n1)
            with pytest.raises(RuntimeError):
                _ = n1.volatile
            world.restart(n1)
            await asyncio.sleep(0.1)
        assert n1.disk["boots"] == 4
        assert n1.volatile["token"] == n1.generation == 4

    expect_pass(body)


def test_crash_with_scheduled_restart_and_errors() -> None:
    async def body(world: detloop.World) -> None:
        world.start(lambda node: asyncio.Event().wait(), nodes=["n1"])
        await asyncio.sleep(0.1)
        world.crash("n1", restart_after=1.0)
        assert not world["n1"].up
        await asyncio.sleep(1.5)
        assert world["n1"].up and world["n1"].generation == 2
        with pytest.raises(RuntimeError):
            world["n2"].crash()  # never started
        with pytest.raises(detloop.ConfigError):
            world.control.crash()
        with pytest.raises(detloop.ConfigError):
            world.node("n9")

    expect_pass(body)


def test_node_errors_fail_the_run() -> None:
    async def body(world: detloop.World) -> None:
        async def bad(node: detloop.Node) -> None:
            await asyncio.sleep(0.1)
            raise ValueError("boom in node")

        world.start(bad, nodes=["n2"])
        await asyncio.sleep(1)

    res = run_sim(body)
    assert (res.status, res.kind) == ("fail", "node_error")
    assert res.signature.startswith("node_error:ValueError")
    assert run_sim(body, fail_on_node_error=False).status == "pass"


def test_local_clock_skew_and_drift_do_not_affect_loop_time() -> None:
    async def body(world: detloop.World) -> None:
        stamps: dict[str, tuple[float, float]] = {}

        async def node_main(node: detloop.Node) -> None:
            await asyncio.sleep(10)
            stamps[node.name] = (time.time(), asyncio.get_running_loop().time())

        world["n1"].set_clock(offset=2.5)
        world["n2"].set_clock(drift_ppm=1000)  # 1 ms per second fast
        world.start(node_main)
        await asyncio.sleep(11)
        epoch = world.config.epoch
        assert stamps["n1"][0] == pytest.approx(epoch + 12.5)
        assert stamps["n2"][0] == pytest.approx(epoch + 10.01)
        assert stamps["n3"][0] == pytest.approx(epoch + 10.0)
        assert {s[1] for s in stamps.values()} == {10.0}
        assert time.time() == pytest.approx(epoch + 11.0)  # control has no skew
        assert time.monotonic() == 11.0

    expect_pass(body)


def test_invariants_are_sticky_and_periodic() -> None:
    async def body(world: detloop.World) -> None:
        state = {"x": 0}
        world.invariant("x-small", lambda: state["x"] < 3, every=0.1)

        async def node_main(node: detloop.Node) -> None:
            while True:
                state["x"] += 1
                await asyncio.sleep(0.3)

        world.start(node_main, nodes=["n1"])
        await asyncio.sleep(5)

    res = run_sim(body)
    assert (res.status, res.signature) == ("fail", "invariant:x-small")

    async def swallow(world: detloop.World) -> None:
        with contextlib.suppress(Exception):
            world.check(False, "swallowed", "caught by user code")
        await asyncio.sleep(1)

    assert run_sim(swallow).signature == "invariant:swallowed"


def test_fault_generation_is_deterministic_and_bounded() -> None:
    cfg = FaultConfig(kinds=("partitions", "crashes", "resets", "latency", "clock"), max_faults=6)
    a = generate(cfg, ("n1", "n2", "n3"), SplitMix64(1))
    b = generate(cfg, ("n1", "n2", "n3"), SplitMix64(1))
    assert a == b and 1 <= len(a) <= 6
    assert [f.at for f in a] == sorted(f.at for f in a)
    for f in a:
        assert 0.1e9 <= f.at <= 10e9
        assert Fault.from_json(json.loads(json.dumps(f.to_json()))) == f
        assert f.describe()
    kinds = {f.kind for s in range(40) for f in generate(cfg, ("n1", "n2", "n3"), SplitMix64(s))}
    assert kinds == {"partition", "crash", "reset", "latency", "clock"}
    assert generate(FaultConfig(), ("n1", "n2"), SplitMix64(0)) == []


def test_scheduled_faults_are_applied_and_recovered() -> None:
    schedule = [
        Fault(
            "f0",
            "partition",
            int(1e9),
            int(1e9),
            {"groups": [["n1"], ["n2", "n3"]], "oneway": False},
        ),
        Fault("f1", "crash", int(1.5e9), int(0.5e9), {"node": "n2"}),
        Fault("f2", "latency", int(1e9), int(1e9), {"a": "n1", "b": "n3", "factor": 10.0}),
        Fault(
            "f3", "clock", int(1e9), int(1e9), {"node": "n3", "offset_ns": int(5e9), "drift_ppm": 0}
        ),
        Fault("f4", "crash", int(1.6e9), 0, {"node": "n2"}),  # already down -> skipped
    ]
    observed: dict[str, Any] = {}

    async def body(world: detloop.World) -> None:
        world.start(lambda node: asyncio.Event().wait())
        await asyncio.sleep(1.2)
        net = world.network
        observed["blocked"] = not net.link_up(world["n1"], world["n2"])
        observed["skew"] = world["n3"].wall_time() - world.config.epoch - world.time()
        await asyncio.sleep(0.5)
        observed["n2_down"] = not world["n2"].up
        await asyncio.sleep(1.0)
        observed["healed"] = net.link_up(world["n1"], world["n2"])
        observed["n2_back"] = world["n2"].up and world["n2"].generation == 2
        observed["skew_after"] = world["n3"].wall_time() - world.config.epoch - world.time()

    cfg = detloop.SimConfig()
    res = detloop.run(body, seed=0, config=cfg, faults=schedule, keep_trace=True)
    assert res.status == "pass", res.message
    assert observed == {
        "blocked": True,
        "skew": pytest.approx(5.0, abs=1e-6),
        "n2_down": True,
        "healed": True,
        "n2_back": True,
        "skew_after": pytest.approx(0.0, abs=1e-6),
    }
    assert sum('"k":"fault_skipped"' in line for line in res.trace_lines) == 1


def test_framework_errors_in_fault_callbacks_are_internal_errors() -> None:
    bad = [Fault("f0", "crash", int(1e9), 0, {"node": "nope"})]

    async def body(world: detloop.World) -> None:
        await asyncio.sleep(2)

    res = detloop.run(body, seed=0, config=detloop.SimConfig(), faults=bad)
    assert (res.status, res.kind) == ("error", "internal_error")


def test_world_partition_isolates_including_control() -> None:
    async def body(world: detloop.World) -> None:
        world.partition("n1")
        net = world.network
        assert not net.link_up(world.control, world["n1"])
        assert not net.link_up(world["n1"], world["n3"])
        assert net.link_up(world["n2"], world["n3"])
        world.heal("n1")
        assert net.link_up(world.control, world["n1"])
        undo = world.slow("n1", "n2", 5)
        undo()

    expect_pass(body)


def test_add_node_and_notes() -> None:
    async def body(world: detloop.World) -> None:
        client = world.add_node("client1")
        assert client.address == "10.0.0.4"
        with pytest.raises(detloop.ConfigError):
            world.add_node("n1")
        world.note("custom", i="user-i", k="user-k", detail=[1, 2])

    res = expect_pass(body)
    line = next(json.loads(line) for line in res.trace_lines if '"note":"custom"' in line)
    assert line["k"] == "note" and line["i_"] == "user-i" and line["k_"] == "user-k"
