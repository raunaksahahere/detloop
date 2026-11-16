"""Simulated network: streams, flow control, partitions, resets, crashes, messages."""

from __future__ import annotations

import asyncio
import errno
import socket
from typing import Any

import pytest

import detloop

from .conftest import expect_pass, run_sim

PORT = 7000


async def echo_server(node: detloop.Node, port: int = PORT) -> None:
    async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        try:
            while data := await r.read(65536):
                w.write(data)
                await w.drain()
        except ConnectionError:
            pass
        finally:
            w.close()

    srv = await asyncio.start_server(handle, port=port)
    await srv.serve_forever()


async def start_echo(world: detloop.World, node: str = "n1") -> None:
    world.start(echo_server, nodes=[node])
    await asyncio.sleep(0.05)


def test_addressing_and_resolution() -> None:
    async def body(world: detloop.World) -> None:
        loop = asyncio.get_running_loop()
        info = await loop.getaddrinfo("n2", 80)
        assert info[0][4] == ("10.0.0.2", 80)
        assert (await loop.getaddrinfo("10.0.0.3", "http"))[0][4] == ("10.0.0.3", 80)
        with pytest.raises(socket.gaierror):
            await loop.getaddrinfo("nope.example", 80)
        with pytest.raises(socket.gaierror):
            await loop.getaddrinfo("n1", 80, family=socket.AF_INET6)
        assert world.node("n3").address == "10.0.0.3"
        assert socket.gethostbyname("n1") == "10.0.0.1"
        assert socket.gethostname() == "control"

    expect_pass(body)


def test_echo_and_ordered_large_transfer_with_backpressure() -> None:
    async def body(world: detloop.World) -> None:
        await start_echo(world)
        r, w = await asyncio.open_connection("n1", PORT)
        assert w.get_extra_info("peername") == ("10.0.0.1", PORT)
        assert w.get_extra_info("sockname")[0] == "10.0.0.254"
        payload = bytes(range(256)) * 4000  # 1 MiB, many segments

        async def send() -> None:
            w.write(payload)
            assert w.transport.get_write_buffer_size() > 0
            await w.drain()

        _, got = await asyncio.gather(send(), r.readexactly(len(payload)))
        assert got == payload
        w.close()
        await w.wait_closed()

    for seed in range(3):
        expect_pass(body, seed)


def test_backpressure_deadlock_is_detected() -> None:
    """Write-everything-then-read against an echo server deadlocks on real TCP
    once buffers fill; detloop reports it instead of hanging."""

    async def body(world: detloop.World) -> None:
        await start_echo(world)
        r, w = await asyncio.open_connection("n1", PORT)
        w.write(b"q" * 2_000_000)
        await w.drain()  # never completes: nobody reads the echoed data
        await r.read()

    res = run_sim(body)
    assert (res.status, res.kind) == ("fail", "deadlock")
    assert any("drain" in d.lower() for d in res.details)


def test_drain_blocks_until_receiver_reads() -> None:
    async def body(world: detloop.World) -> None:
        release = asyncio.Event()
        received: list[int] = []

        async def slow_server(node: detloop.Node) -> None:
            async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
                await release.wait()
                while data := await r.read(65536):
                    received.append(len(data))
                w.close()

            srv = await asyncio.start_server(handle, port=PORT)
            await srv.serve_forever()

        world.start(slow_server, nodes=["n1"])
        await asyncio.sleep(0.05)
        _, w = await asyncio.open_connection("n1", PORT)
        w.write(b"x" * 2_000_000)
        drained = asyncio.create_task(w.drain())
        await asyncio.sleep(5)
        # receiver window (64 KiB) + StreamReader pause threshold bound what can be in flight
        assert not drained.done(), "drain() must block while the receiver does not read"
        assert w.transport.get_write_buffer_size() > 1_000_000
        release.set()
        await asyncio.wait_for(drained, 30)
        w.close()
        await asyncio.sleep(1)
        assert sum(received) == 2_000_000

    expect_pass(body)


def test_write_buffer_hard_cap_is_a_budget_failure() -> None:
    async def body(world: detloop.World) -> None:
        async def sink(node: detloop.Node) -> None:
            async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
                await asyncio.sleep(1000)

            await (await asyncio.start_server(handle, port=PORT)).serve_forever()

        world.start(sink, nodes=["n1"])
        await asyncio.sleep(0.05)
        _, w = await asyncio.open_connection("n1", PORT)
        for _ in range(100):
            w.write(b"y" * 100_000)  # never drains
        await asyncio.sleep(1)

    res = run_sim(body, max_write_buffer=1_000_000)
    assert (res.status, res.signature) == ("incomplete", "budget:buffer")


def test_half_close_eof_and_close_vs_abort() -> None:
    async def body(world: detloop.World) -> None:
        events: list[str] = []

        async def server(node: detloop.Node) -> None:
            async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
                data = await r.read()  # until EOF
                events.append(f"server got {len(data)} then EOF")
                w.write(b"bye")
                await w.drain()
                w.close()

            await (await asyncio.start_server(handle, port=PORT)).serve_forever()

        world.start(server, nodes=["n1"])
        await asyncio.sleep(0.05)
        r, w = await asyncio.open_connection("n1", PORT)
        assert w.can_write_eof()
        w.write(b"hello")
        w.write_eof()
        assert await r.read() == b"bye"
        assert events == ["server got 5 then EOF"]
        with pytest.raises(RuntimeError):
            w.write(b"after eof")
        w.close()
        await w.wait_closed()
        # abort: the peer sees a reset
        _r2, w2 = await asyncio.open_connection("n1", PORT)
        w2.transport.abort()
        await asyncio.sleep(0.1)
        assert w2.is_closing()

    expect_pass(body)


def test_connection_refused_and_timeouts() -> None:
    async def body(world: detloop.World) -> None:
        # n2 booted but nothing listens -> refused
        with pytest.raises(ConnectionRefusedError) as ei:
            await asyncio.open_connection("n2", PORT)
        assert ei.value.errno == errno.ECONNREFUSED
        # unknown address -> nobody answers -> connect timeout
        t0 = world.time()
        with pytest.raises(TimeoutError):
            await asyncio.open_connection("10.0.0.99", PORT)
        assert world.time() - t0 == pytest.approx(10.0)
        # partitioned host -> SYNs lost -> timeout; asyncio.wait_for bounds it earlier
        await start_echo(world)
        world.partition("n1", "control")
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.open_connection("n1", PORT), 2)
        world.heal()
        _r, w = await asyncio.open_connection("n1", PORT)
        w.close()

    expect_pass(body)


def test_syn_retransmission_succeeds_after_short_partition() -> None:
    async def body(world: detloop.World) -> None:
        await start_echo(world)
        world.partition("n1", "control")
        world.loop.call_later(1.5, world.heal)
        t0 = world.time()
        r, w = await asyncio.open_connection("n1", PORT)
        assert 1.5 < world.time() - t0 < 4.0  # SYN retried at 1s, 3s
        w.write(b"ok")
        assert await r.readexactly(2) == b"ok"
        w.close()

    expect_pass(body)


def test_partition_stalls_stream_then_delivers_in_order_after_heal() -> None:
    async def body(world: detloop.World) -> None:
        await start_echo(world)
        r, w = await asyncio.open_connection("n1", PORT)
        w.write(b"a" * 10000)  # in flight when the partition starts
        world.partition("n1", "control")
        w.write(b"b" * 10000)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(r.readexactly(1), 3)
        world.heal()
        got = await asyncio.wait_for(r.readexactly(20000), 5)
        assert got == b"a" * 10000 + b"b" * 10000  # no loss, no duplication, no reordering
        w.close()

    for seed in range(5):
        res = expect_pass(body, seed)
        assert any('"k":"retransmit"' in line for line in res.trace_lines)


def test_long_partition_times_out_connection() -> None:
    async def body(world: detloop.World) -> None:
        await start_echo(world)
        r, w = await asyncio.open_connection("n1", PORT)
        world.partition("n1", "control")
        w.write(b"ping")
        with pytest.raises(TimeoutError):
            await r.readexactly(4)  # retransmission timeout aborts the connection
        assert 10 < world.time() < 30

    expect_pass(body, retransmit_timeout=15.0)


def test_oneway_partition_blocks_one_direction_only() -> None:
    async def body(world: detloop.World) -> None:
        await start_echo(world)
        r, w = await asyncio.open_connection("n1", PORT)
        world.partition("n1", "control", oneway=True)  # replies blocked
        w.write(b"x")
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(r.readexactly(1), 2)
        world.heal("n1", "control", oneway=True)
        assert await asyncio.wait_for(r.readexactly(1), 5) == b"x"
        w.close()

    expect_pass(body)


def test_overlapping_partitions_are_reference_counted() -> None:
    async def body(world: detloop.World) -> None:
        net = world.network
        world.partition("n1", "n2")
        world.partition(["n1"], ["n2", "n3"])
        world.heal("n1", "n2")
        assert not net.link_up(world["n1"], world["n2"])  # second partition still active
        world.heal(["n1"], ["n2", "n3"])
        assert net.link_up(world["n1"], world["n2"])
        assert net.link_up(world["n3"], world["n1"])

    expect_pass(body)


@pytest.mark.parametrize("mode", ["process", "host"])
def test_crash_semantics_for_peers(mode: str) -> None:
    async def body(world: detloop.World) -> None:
        await start_echo(world)
        r, w = await asyncio.open_connection("n1", PORT)
        w.write(b"1")
        assert await r.readexactly(1) == b"1"
        world.crash("n1")
        if mode == "process":
            # the OS of the crashed process resets the connection
            with pytest.raises(ConnectionResetError):
                await asyncio.wait_for(r.readexactly(1), 2)
            with pytest.raises(ConnectionRefusedError):
                await asyncio.open_connection("n1", PORT)
        else:
            # the machine vanished: an idle connection notices nothing...
            await asyncio.sleep(2)
            assert not w.is_closing()
            # ...until it sends and retransmissions time out
            w.write(b"2")
            with pytest.raises(TimeoutError):
                await r.readexactly(1)
            with pytest.raises(TimeoutError):
                await asyncio.open_connection("n1", PORT)

    expect_pass(body, crash_mode=mode)


def test_host_crash_then_restart_resets_old_connections() -> None:
    async def body(world: detloop.World) -> None:
        await start_echo(world)
        r, w = await asyncio.open_connection("n1", PORT)
        world.crash("n1")
        world.restart("n1")
        await asyncio.sleep(0.05)
        w.write(b"to-old-incarnation")
        with pytest.raises(ConnectionResetError):
            await asyncio.wait_for(r.readexactly(1), 3)
        r2, w2 = await asyncio.open_connection("n1", PORT)  # new incarnation serves again
        w2.write(b"new")
        assert await r2.readexactly(3) == b"new"
        w2.close()

    expect_pass(body, crash_mode="host")


def test_reset_fault_and_reset_connections() -> None:
    async def body(world: detloop.World) -> None:
        await start_echo(world)
        r, _w = await asyncio.open_connection("n1", PORT)
        assert world.reset_connections("control", "n1") == 1
        with pytest.raises(ConnectionResetError):
            await asyncio.wait_for(r.readexactly(1), 1)

    expect_pass(body)


def test_server_lifecycle_and_errors() -> None:
    async def body(world: detloop.World) -> None:
        async def server(node: detloop.Node) -> None:
            srv = await asyncio.start_server(lambda r, w: None, port=PORT)
            node.volatile["srv"] = srv
            with pytest.raises(OSError) as ei:
                await asyncio.start_server(lambda r, w: None, port=PORT)
            assert ei.value.errno == errno.EADDRINUSE
            with pytest.raises(OSError) as ei:
                await asyncio.start_server(lambda r, w: None, host="n2", port=PORT + 1)
            assert ei.value.errno == errno.EADDRNOTAVAIL
            eph = await asyncio.start_server(lambda r, w: None, port=0)
            assert eph.sockets[0].getsockname()[1] >= 32768
            assert srv.is_serving() and srv.sockets
            srv.close()
            await srv.wait_closed()
            assert not srv.is_serving()
            node.volatile["done"] = True

        world.start(server, nodes=["n1"])
        await asyncio.sleep(0.5)
        assert world["n1"].volatile["done"]
        with pytest.raises(ConnectionRefusedError):
            await asyncio.open_connection("n1", PORT)

    expect_pass(body)


def test_bandwidth_limits_transfer_time() -> None:
    async def body(world: detloop.World) -> None:
        await start_echo(world)
        r, w = await asyncio.open_connection("n1", PORT)
        t0 = world.time()
        w.write(b"z" * 100_000)
        await r.readexactly(100_000)
        world.note("elapsed", seconds=world.time() - t0)
        w.close()

    res = expect_pass(body, bandwidth=100_000.0, latency_min=0.001, latency_max=0.001)
    line = next(line for line in res.trace_lines if '"note":"elapsed"' in line)
    import json

    # 100 KB at 100 KB/s; the echo streams back while the upload is still in progress
    assert 1.0 <= json.loads(line)["seconds"] < 1.5


def test_late_packets_never_reach_a_new_incarnation() -> None:
    async def body(world: detloop.World) -> None:
        seen: list[tuple[int, bytes]] = []

        async def server(node: detloop.Node) -> None:
            async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
                while data := await r.read(100):
                    seen.append((node.generation, data))

            await (await asyncio.start_server(handle, port=PORT)).serve_forever()

        world.start(server, nodes=["n1"])
        await asyncio.sleep(0.05)
        _, w = await asyncio.open_connection("n1", PORT)
        w.write(b"before")
        world.crash("n1")  # segment in flight
        world.restart("n1")
        await asyncio.sleep(1)
        assert all(gen == 1 for gen, _ in seen)
        assert (2, b"before") not in seen

    for seed in range(5):
        expect_pass(body, seed)


def test_message_bus_reliable_without_faults_and_lossy_with_faults() -> None:
    async def body(world: detloop.World) -> None:
        got: list[tuple[str, bytes]] = []

        async def receiver(node: detloop.Node) -> None:
            ep = node.messages(9000)
            while True:
                got.append(await ep.recv())

        world.start(receiver, nodes=["n1"])
        await asyncio.sleep(0.01)
        ep = world.control.messages(9001)
        for i in range(200):
            ep.send("n1", 9000, str(i).encode())
        await asyncio.sleep(1)
        world.note(
            "stats",
            received=len(got),
            distinct=len(set(got)),
            ordered=[int(p) for _, p in got] == sorted(int(p) for _, p in got),
        )
        with pytest.raises(TypeError):
            ep.send("n1", 9000, "not bytes")  # type: ignore[arg-type]

    import json

    clean = expect_pass(body)
    s = json.loads(next(line for line in clean.trace_lines if '"note":"stats"' in line))
    assert s["received"] == s["distinct"] == 200
    lossy = expect_pass(body, faults="messages", msg_loss=0.2, msg_duplicate=0.2)
    s = json.loads(next(line for line in lossy.trace_lines if '"note":"stats"' in line))
    assert s["distinct"] < 200  # loss
    assert s["received"] > s["distinct"]  # duplication
    assert s["ordered"] is False  # reordering


def test_message_partition_and_crash_drop() -> None:
    async def body(world: detloop.World) -> None:
        got: list[Any] = []

        async def receiver(node: detloop.Node) -> None:
            ep = node.messages(9000)
            while True:
                got.append(await ep.recv())

        world.start(receiver, nodes=["n1"])
        await asyncio.sleep(0.01)
        ep = world.control.messages(9001)
        world.partition("n1", "control")
        ep.send("n1", 9000, b"lost")
        await asyncio.sleep(0.1)
        world.heal()
        world.crash("n1")
        ep.send("n1", 9000, b"to-dead-node")
        world.restart("n1")
        await asyncio.sleep(0.1)
        ep.send("n1", 9000, b"delivered")
        await asyncio.sleep(0.1)
        assert got == [("control", b"delivered")]

    expect_pass(body)
