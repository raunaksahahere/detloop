"""Determinism guards: virtualization, rejection, restoration, isolation."""

from __future__ import annotations

import asyncio
import os
import random
import socket
import subprocess
import threading
import time
import uuid

import pytest

import detloop
from detloop import guards

from .conftest import expect_pass, run_sim


def _snapshot() -> dict[str, object]:
    return {
        "time.time": time.time, "time.monotonic": time.monotonic, "time.sleep": time.sleep,
        "os.urandom": os.urandom, "socket.socket": socket.socket,
        "socket.getaddrinfo": socket.getaddrinfo, "Thread.start": threading.Thread.start,
        "Popen.__init__": subprocess.Popen.__init__,
    }


def test_time_is_virtual_inside_and_real_outside() -> None:
    async def body(world: detloop.World) -> None:
        assert time.monotonic() == 0.0
        await asyncio.sleep(42)
        assert time.monotonic() == 42.0
        assert time.perf_counter_ns() == 42 * 10**9
        assert time.time() == world.config.epoch + 42

    before = _snapshot()
    expect_pass(body)
    assert _snapshot() == before
    assert time.time() > 1.7e9


@pytest.mark.parametrize(
    ("snippet", "kind"),
    [
        (lambda: time.sleep(0.01), "nondeterminism"),
        (lambda: os.urandom(4), "nondeterminism"),
        (lambda: uuid.uuid4(), "nondeterminism"),
        (lambda: random.SystemRandom().random(), "nondeterminism"),
        (lambda: threading.Thread(target=lambda: None).start(), "nondeterminism"),
        (lambda: subprocess.run(["true"], check=False), "nondeterminism"),
        (lambda: socket.create_connection(("n1", 80)), "nondeterminism"),
        (lambda: socket.socket(socket.AF_UNIX, socket.SOCK_STREAM), "nondeterminism"),
        (lambda: socket.socket().connect(("n1", 80)), "nondeterminism"),
    ],
)
def test_blocked_apis_fail_loudly_even_if_swallowed(snippet: object, kind: str) -> None:
    async def body(world: detloop.World) -> None:
        try:
            snippet()  # type: ignore[operator]
        except Exception:
            pass  # sticky: still fails
        await asyncio.sleep(0.1)

    before = _snapshot()
    res = run_sim(body)
    assert (res.status, res.kind) == ("fail", kind), res.message
    assert _snapshot() == before


def test_seeded_entropy_mode_is_deterministic() -> None:
    async def body(world: detloop.World) -> None:
        world.note("entropy", a=os.urandom(8).hex(), u=str(uuid.uuid4()), r=random.random())

    a = expect_pass(body, entropy="seeded")
    b = expect_pass(body, entropy="seeded")
    c = expect_pass(body, seed=1, entropy="seeded")
    assert a.trace_sha256 == b.trace_sha256 != c.trace_sha256


def test_random_module_seeded_per_run_and_restored() -> None:
    random.seed(1234)
    expected_next = random.random()
    random.seed(1234)

    async def body(world: detloop.World) -> None:
        world.note("r", value=random.random())

    a = expect_pass(body)
    b = expect_pass(body)
    assert a.trace_sha256 == b.trace_sha256
    assert random.random() == expected_next  # host random state untouched


def test_guards_restored_after_failure_and_interrupt() -> None:
    before = _snapshot()

    async def boom(world: detloop.World) -> None:
        raise RuntimeError("x")

    assert run_sim(boom).status == "fail"
    assert _snapshot() == before

    async def interrupt(world: detloop.World) -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run_sim(interrupt)
    assert _snapshot() == before
    assert guards.active_world() is None


def test_other_threads_see_real_apis_during_a_simulation() -> None:
    results: dict[str, object] = {}
    go = threading.Event()
    done = threading.Event()

    def watcher() -> None:
        go.wait(5)
        results["monotonic"] = time.monotonic()
        results["urandom"] = len(os.urandom(4))
        done.set()

    t = threading.Thread(target=watcher)
    t.start()

    async def body(world: detloop.World) -> None:
        await asyncio.sleep(100)
        go.set()
        done.wait(5)  # blocking here is fine for the test; virtual time is frozen

    expect_pass(body)
    t.join()
    assert results["monotonic"] != 100.0 and results["urandom"] == 4


def test_nested_simulation_rejected() -> None:
    async def inner(world: detloop.World) -> None:
        pass

    async def outer(world: detloop.World) -> None:
        loop = asyncio.get_running_loop()
        assert loop is world.loop
        with pytest.raises(detloop.ConfigError):
            detloop.run(inner, seed=0)

    expect_pass(outer)


def test_sim_socket_path_used_by_happy_eyeballs_style_clients() -> None:
    async def body(world: detloop.World) -> None:
        async def server(node: detloop.Node) -> None:
            async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
                w.write(await r.readexactly(3))
                await w.drain()
                w.close()

            await (await asyncio.start_server(handle, port=80)).serve_forever()

        world.start(server, nodes=["n1"])
        await asyncio.sleep(0.05)
        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        await loop.sock_connect(sock, ("10.0.0.1", 80))
        assert sock.getpeername() == ("10.0.0.1", 80)
        r, w = await asyncio.open_connection(sock=sock)
        w.write(b"abc")
        assert await r.readexactly(3) == b"abc"
        w.close()
        with pytest.raises(detloop.UnsupportedOperation):
            sock.send(b"x")

    expect_pass(body)
