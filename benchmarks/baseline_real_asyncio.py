"""Baseline: the same KV workload on the *real* asyncio loop with random sleeps.

This answers "how often does the bug show up naturally?". It runs the exact
node code from ``detloop.examples.kvstore`` over real localhost TCP. Every
directed link (client->node and node->node) goes through a proxy that delays
each forwarded chunk by a random 1-10 ms (the same range as detloop's default
simulated latency). Faults mirror the simulated fault model: a partition makes
the affected proxies hold traffic (like a stalled TCP link) until healed; a
crash cancels the node's tasks and closes its server and connections, and the
node is restarted later with the same ``disk`` dict.

Limits (documented in docs/BENCHMARKS.md): wall-clock time instead of virtual
time, OS scheduling noise, no reproducibility, and a crash is task
cancellation (cooperative), not a hard stop.

Usage: python benchmarks/baseline_real_asyncio.py --bug stale-read --trials 10
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import random
import sys
import time
from typing import Any

from detloop.checker import KVModel, check_linearizable
from detloop.examples.kvstore import KVClient, KVConfig, Observer, run_node
from detloop.history import History

NODES = ("n1", "n2", "n3")
KEYS = ("a", "b", "c", "d")
DURATION = 4.0
WRITE_RATIO = 0.3


class Proxy:
    """Forwards one directed link with random per-chunk delay; can be blocked."""

    def __init__(self, target: tuple[str, int], rng: random.Random) -> None:
        self.target = target
        self.rng = rng
        self.open = asyncio.Event()
        self.open.set()
        self.conns: set[asyncio.StreamWriter] = set()
        self.server: asyncio.Server | None = None
        self.port = 0

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def _pipe(self, r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        try:
            while data := await r.read(65536):
                await asyncio.sleep(self.rng.uniform(0.001, 0.010))
                await self.open.wait()
                w.write(data)
                await w.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            w.close()

    async def _handle(self, r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        try:
            await self.open.wait()
            ur, uw = await asyncio.open_connection(*self.target)
        except OSError:
            w.close()
            return
        self.conns.update((w, uw))
        await asyncio.gather(self._pipe(r, uw), self._pipe(ur, w))
        self.conns.difference_update((w, uw))

    def reset(self) -> None:
        for c in list(self.conns):
            c.transport.abort()


async def trial(bug: str | None, seed: int) -> dict[str, Any]:
    rng = random.Random(seed)
    random.seed(seed)  # election timeouts use the random module
    history = History()
    leaders: dict[int, str] = {}
    violations: list[str] = []

    def on_leader(node: str, term: int) -> None:
        first = leaders.setdefault(term, node)
        if first != node:
            violations.append(f"term {term} has two leaders: {first} and {node}")

    obs = Observer(on_leader=on_leader)
    bugs = frozenset([bug]) if bug else frozenset()
    real_ports: dict[str, int] = {}
    # reserve real ports by starting placeholder servers, then close them
    for n in NODES:
        s = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
        real_ports[n] = s.sockets[0].getsockname()[1]
        s.close()
        await s.wait_closed()
    proxies: dict[tuple[str, str], Proxy] = {}
    for src in (*NODES, "client"):
        for dst in NODES:
            if src != dst:
                p = Proxy(("127.0.0.1", real_ports[dst]), rng)
                await p.start()
                proxies[(src, dst)] = p
    disks: dict[str, dict[str, Any]] = {n: {} for n in NODES}
    tasks: dict[str, asyncio.Task[Any]] = {}

    def start(n: str) -> None:
        peers = {p: ("127.0.0.1", real_ports[p] if p == n else proxies[(n, p)].port) for p in NODES}
        cfg = KVConfig(peers=peers, bugs=bugs)

        async def boot() -> None:
            for _ in range(50):  # the previous server may still be releasing the port
                try:
                    await run_node(n, cfg, disks[n], obs)
                    return
                except OSError:
                    await asyncio.sleep(0.05)

        tasks[n] = asyncio.create_task(boot())

    for n in NODES:
        start(n)
    await asyncio.sleep(0.05)
    client_peers = {n: ("127.0.0.1", proxies[("client", n)].port) for n in NODES}
    loop = asyncio.get_running_loop()
    t0 = loop.time()

    async def faults() -> None:
        # up to 4 faults (partitions or crashes) inside the workload window
        for _ in range(1 + rng.randrange(4)):
            await asyncio.sleep(rng.uniform(0.2, DURATION / 2))
            if rng.random() < 0.5:
                victim = rng.choice(NODES)
                links = [k for k in proxies if victim in k and "client" not in k]
                for k in links:
                    proxies[k].open.clear()
                await asyncio.sleep(rng.uniform(0.5, 2.0))
                for k in links:
                    proxies[k].open.set()
            else:
                victim = rng.choice(NODES)
                # A real crash needs no cooperation; here we cancel the node and
                # abort its connections so serve_forever()'s wait_closed() returns.
                tasks[victim].cancel()
                for k, p in proxies.items():
                    if victim in k:
                        p.reset()
                with contextlib.suppress(BaseException):
                    await asyncio.wait_for(tasks[victim], 5)
                await asyncio.sleep(rng.uniform(0.2, 1.5))
                start(victim)

    async def client_loop(c: KVClient, counter: list[int]) -> None:
        while loop.time() - t0 < DURATION:
            key = rng.choice(KEYS)
            if rng.random() < WRITE_RATIO:
                counter[0] += 1
                await c.op("put", key, f"{c.name}-{counter[0]}")
            else:
                await c.op("get", key)
            await asyncio.sleep(rng.uniform(0.0, 0.05))

    counter = [0]
    clients = [KVClient(f"c{i}", client_peers, history, rng) for i in range(3)]
    fault_task = asyncio.create_task(faults())
    await asyncio.gather(*(client_loop(c, counter) for c in clients))
    fault_task.cancel()
    with contextlib.suppress(BaseException):
        await fault_task
    for p in proxies.values():
        p.open.set()
    for n, t in tasks.items():
        if t.done():
            start(n)
    await asyncio.sleep(2.0)
    final = KVClient("final", client_peers, history, rng)
    for key in KEYS:
        for _ in range(10):
            before = len(history.ops)
            await final.op("get", key)
            if history.ops[before].status == "ok":
                break
            await asyncio.sleep(0.5)
    result = check_linearizable(history, KVModel())
    for t in tasks.values():
        t.cancel()
    for p in proxies.values():
        p.reset()
        if p.server:
            p.server.close()
    for t in tasks.values():
        with contextlib.suppress(BaseException):
            await asyncio.wait_for(t, 5)
    detected = result.status == "invalid" or bool(violations)
    return {
        "bug": bug,
        "seed": seed,
        "detected": detected,
        "linearizability": result.status,
        "election_safety_violations": len(violations),
        "ops": len(history.ops),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--bug", choices=["none", "stale-read", "lost-ack", "split-brain"], default="none"
    )
    ap.add_argument("--trials", type=int, default=10)
    ap.add_argument("--seed-start", type=int, default=0)
    args = ap.parse_args()
    bug = None if args.bug == "none" else args.bug
    out = []
    for i in range(args.trials):
        start = time.perf_counter()
        res = asyncio.run(trial(bug, args.seed_start + i))
        res["wall_seconds"] = round(time.perf_counter() - start, 3)
        out.append(res)
        print(json.dumps(res), flush=True)
    detected = sum(r["detected"] for r in out)
    print(
        json.dumps({"summary": True, "bug": args.bug, "trials": len(out), "detected": detected}),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
