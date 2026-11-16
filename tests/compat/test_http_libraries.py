"""Real aiohttp and httpx traffic over the simulated asyncio network.

No mocks: aiohttp's web server listens through ``loop.create_server``; the
aiohttp client connects through aiohappyeyeballs' ``socket.socket`` +
``loop.sock_connect`` path; httpx/httpcore/anyio connect through
``loop.create_connection``. Requires the ``compat`` extra.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

import detloop

aiohttp = pytest.importorskip("aiohttp")
httpx = pytest.importorskip("httpx")
web = pytest.importorskip("aiohttp.web")

pytestmark = pytest.mark.compat
PORT = 8080


async def web_server(node: detloop.Node) -> None:
    app = web.Application()
    hits = node.volatile.setdefault("hits", [])

    async def hello(request: Any) -> Any:
        hits.append(request.path)
        return web.json_response(
            {"node": node.name, "remote": request.remote, "gen": node.generation}
        )

    async def echo(request: Any) -> Any:
        body = await request.json()
        return web.json_response({"echo": body})

    async def slow(request: Any) -> Any:
        await asyncio.sleep(float(request.query.get("s", "5")))
        return web.Response(text="late")

    async def stream(request: Any) -> Any:
        resp = web.StreamResponse()
        await resp.prepare(request)
        for i in range(20):
            await resp.write(f"chunk-{i}\n".encode() * 200)
            await asyncio.sleep(0.01)
        await resp.write_eof()
        return resp

    app.router.add_get("/", hello)
    app.router.add_post("/echo", echo)
    app.router.add_get("/slow", slow)
    app.router.add_get("/stream", stream)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, port=PORT)
    await site.start()
    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()


def run(body: Any, seed: int = 0, **cfg: Any) -> detloop.RunResult:
    config = (
        detloop.SimConfig(nodes=("n1", "n2")).with_overrides(**cfg)
        if cfg
        else detloop.SimConfig(nodes=("n1", "n2"))
    )
    res = detloop.run(body, seed=seed, config=config)
    assert res.status == "pass", f"{res.signature}: {res.message}\n{res.traceback}"
    return res


def test_aiohttp_client_and_server_requests() -> None:
    async def body(world: detloop.World) -> None:
        world.start(web_server)
        await asyncio.sleep(0.2)
        async with aiohttp.ClientSession() as s:
            async with s.get(f"http://n1:{PORT}/") as r:
                assert r.status == 200
                assert await r.json() == {"node": "n1", "remote": "10.0.0.254", "gen": 1}
            async with s.post(f"http://n2:{PORT}/echo", json={"k": [1, 2, 3]}) as r:
                assert (await r.json())["echo"] == {"k": [1, 2, 3]}
            async with s.get(f"http://n1:{PORT}/stream") as r:
                data = await r.read()
                assert data.count(b"chunk-19") == 200
            results = await asyncio.gather(
                *(s.get(f"http://n{1 + i % 2}:{PORT}/") for i in range(20))
            )
            assert {r.status for r in results} == {200}
            for r in results:
                r.release()
            before = sum('"k":"connect"' in line for line in world.trace.lines)
            for _ in range(5):  # sequential requests reuse aiohttp's keep-alive pool
                async with s.get(f"http://n1:{PORT}/") as r:
                    await r.read()
            assert sum('"k":"connect"' in line for line in world.trace.lines) == before

    run(body)


def test_aiohttp_timeouts_partitions_and_crashes() -> None:
    async def body(world: detloop.World) -> None:
        world.start(web_server)
        await asyncio.sleep(0.2)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=1.0)) as s:
            t0 = world.time()
            with pytest.raises(asyncio.TimeoutError):
                await s.get(f"http://n1:{PORT}/slow?s=5")
            assert world.time() - t0 == pytest.approx(1.0, abs=0.05)
            world.partition("n1", "control")
            with pytest.raises((asyncio.TimeoutError, aiohttp.ClientError)):
                await s.get(f"http://n1:{PORT}/")
            world.heal()
            world.crash("n2")
            with pytest.raises(aiohttp.ClientConnectionError):
                await s.get(f"http://n2:{PORT}/")
            world.restart("n2")
            await asyncio.sleep(0.2)
            async with s.get(f"http://n2:{PORT}/") as r:
                assert (await r.json())["gen"] == 2

    run(body)


def test_httpx_async_client_requests_and_keepalive() -> None:
    async def body(world: detloop.World) -> None:
        world.start(web_server, nodes=["n1"])
        await asyncio.sleep(0.2)
        async with httpx.AsyncClient(base_url=f"http://n1:{PORT}") as c:
            r = await c.get("/")
            assert r.status_code == 200 and r.json()["node"] == "n1"
            r = await c.post("/echo", json={"x": "y"})
            assert r.json() == {"echo": {"x": "y"}}
            for _ in range(5):
                assert (await c.get("/")).status_code == 200
            async with c.stream("GET", "/stream") as resp:
                total = 0
                async for chunk in resp.aiter_bytes():
                    total += len(chunk)
            assert total == sum(len(f"chunk-{i}\n") * 200 for i in range(20))
        # Documented limitation (docs/COMPATIBILITY.md): httpcore checks pooled
        # connections by polling the socket's file descriptor; simulated
        # sockets have none, so httpx reconnects per request instead of reusing
        # keep-alive connections. Requests still succeed.
        connects = [
            line
            for line in world.trace.lines
            if '"k":"connect"' in line and '"node":"control"' in line
        ]
        assert len(connects) == 8

    run(body)


def test_httpx_timeouts_and_connection_failures() -> None:
    async def body(world: detloop.World) -> None:
        world.start(web_server, nodes=["n1"])
        await asyncio.sleep(0.2)
        async with httpx.AsyncClient() as c:
            t0 = world.time()
            with pytest.raises(httpx.ReadTimeout):
                await c.get(f"http://n1:{PORT}/slow?s=5", timeout=1.5)
            assert world.time() - t0 == pytest.approx(1.5, abs=0.05)
            with pytest.raises(httpx.ConnectError):
                await c.get(f"http://n2:{PORT}/")  # n2 runs nothing: refused
            world.partition("n1", "control")
            with pytest.raises(httpx.ConnectTimeout):
                await c.get(f"http://n1:{PORT}/", timeout=httpx.Timeout(2.0))
            world.heal()
            assert (await c.get(f"http://n1:{PORT}/")).status_code == 200
            world.crash("n1")
            with pytest.raises(httpx.TransportError):
                await c.get(f"http://n1:{PORT}/")

    run(body)


def test_http_traffic_is_deterministic_and_seed_sensitive() -> None:
    async def body(world: detloop.World) -> None:
        world.start(web_server)
        await asyncio.sleep(0.2)
        async with aiohttp.ClientSession() as s, httpx.AsyncClient() as c:
            for _ in range(5):
                node = world.rng.choice(["n1", "n2"])
                async with s.get(f"http://{node}:{PORT}/") as r:
                    await r.read()
                await c.get(f"http://{node}:{PORT}/")
                await asyncio.sleep(world.rng.uniform(0, 0.2))

    a = [run(body, seed=s).trace_sha256 for s in range(3)]
    b = [run(body, seed=s).trace_sha256 for s in range(3)]
    assert a == b and len(set(a)) == 3


def test_tls_is_rejected_not_faked() -> None:
    async def body(world: detloop.World) -> None:
        world.start(web_server, nodes=["n1"])
        await asyncio.sleep(0.2)
        async with httpx.AsyncClient() as c:
            await c.get(f"https://n1:{PORT}/")

    res = detloop.run(body, seed=0, config=detloop.SimConfig(nodes=("n1",)))
    # httpx wraps the error, but the violation is sticky: the run fails as nondeterminism
    assert (res.status, res.kind) == ("fail", "nondeterminism"), res.message
