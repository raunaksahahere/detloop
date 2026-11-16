"""A toy Raft-style replicated key-value store with selectable planted bugs.

This is a teaching system for detloop, **not production Raft**: no log
compaction, no membership changes, no client sessions, whole-log persistence.
It only uses plain asyncio streams, so the same code runs on a real event loop
(see benchmarks/baseline_real_asyncio.py) and inside a detloop simulation.

Consistency contract of the corrected baseline (``bugs=()``):

* **Linearizable** ``put``/``get`` per key, for operations that return ``ok``.
* **Durability**: a ``put`` is acknowledged only after its log entry is stored
  on a majority of nodes' ``disk`` (``node.disk`` survives crashes in the
  simulation; it models storage that is durable as soon as it is written).
* **Election safety**: at most one leader per term.
* Reads are served only by a leader that has committed an entry in its term
  and has confirmed its leadership with a majority after the read arrived
  (Raft's read-index technique).

Planted bugs (independently selectable):

``lost-ack``
    The leader acknowledges a ``put`` as soon as it appends the entry locally,
    before a majority stores it. A crash or partition can then elect a leader
    without the entry and the acknowledged write is lost. Detected by the
    linearizability checker.
``stale-read``
    Every node answers ``get`` from its local state machine without checking
    leadership. Lagging followers and deposed leaders return old values.
    Detected by the linearizability checker.
``split-brain``
    A node grants its vote to *every* candidate with a current-or-newer term,
    even after voting for someone else in that term. Two candidates can win
    the same term. Detected by the election-safety invariant.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

BUGS = ("lost-ack", "stale-read", "split-brain")


@dataclass(frozen=True)
class KVConfig:
    peers: dict[str, tuple[str, int]]
    bugs: frozenset[str] = frozenset()
    election_timeout: tuple[float, float] = (0.30, 0.60)
    heartbeat: float = 0.05
    rpc_timeout: float = 0.20

    def __post_init__(self) -> None:
        unknown = set(self.bugs) - set(BUGS)
        if unknown:
            raise ValueError(f"unknown planted bugs: {sorted(unknown)}")


@dataclass
class Observer:
    """Callbacks the harness uses to watch node-internal events."""

    on_leader: Callable[[str, int], None] = lambda node, term: None
    events: list[tuple[str, Any]] = field(default_factory=list)


async def _send(writer: asyncio.StreamWriter, msg: dict[str, Any]) -> None:
    writer.write(json.dumps(msg, sort_keys=True).encode() + b"\n")
    await writer.drain()


async def _recv(reader: asyncio.StreamReader) -> dict[str, Any]:
    line = await reader.readline()
    if not line:
        raise ConnectionError("connection closed")
    return json.loads(line)  # type: ignore[no-any-return]


class RaftNode:
    def __init__(self, name: str, cfg: KVConfig, disk: dict[str, Any], observer: Observer) -> None:
        self.name = name
        self.cfg = cfg
        self.disk = disk
        self.observer = observer
        self.others = [p for p in cfg.peers if p != name]
        self.majority = len(cfg.peers) // 2 + 1
        disk.setdefault("term", 0)
        disk.setdefault("voted_for", None)
        disk.setdefault("log", [])  # list of [term, command]
        self.role = "follower"
        self.leader: str | None = None
        self.commit = 0  # 1-based index of the highest committed entry
        self.applied = 0
        self.kv: dict[str, Any] = {}
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self.waiters: dict[int, asyncio.Future[bool]] = {}
        self.heard = asyncio.Event()
        self.kick = asyncio.Event()
        self.round = 0  # leadership confirmation rounds completed
        self.round_event = asyncio.Event()
        self.acks: dict[int, set[str]] = {}
        self.term_start_index = 0
        self.conns: dict[str, tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        self.locks: dict[str, asyncio.Lock] = {p: asyncio.Lock() for p in self.others}
        self.tasks: set[asyncio.Task[Any]] = set()

    # -- persistent state ----------------------------------------------------
    @property
    def term(self) -> int:
        return int(self.disk["term"])

    @property
    def log(self) -> list[list[Any]]:
        return self.disk["log"]  # type: ignore[no-any-return]

    def last(self) -> tuple[int, int]:
        log = self.log
        return (log[-1][0], len(log)) if log else (0, 0)

    def set_term(self, term: int) -> None:
        if term > self.term:
            self.disk["term"] = term
            self.disk["voted_for"] = None
            self.become_follower(None)

    def become_follower(self, leader: str | None) -> None:
        if self.role == "leader":
            for fut in self.waiters.values():
                if not fut.done():
                    fut.set_result(False)
            self.waiters.clear()
        self.role = "follower"
        self.leader = leader

    # -- RPC plumbing ----------------------------------------------------------
    async def call(self, peer: str, msg: dict[str, Any]) -> dict[str, Any]:
        async with self.locks[peer]:
            try:
                conn = self.conns.get(peer)
                if conn is None:
                    host, port = self.cfg.peers[peer]
                    conn = await asyncio.wait_for(
                        asyncio.open_connection(host, port), self.cfg.rpc_timeout
                    )
                    self.conns[peer] = conn
                reader, writer = conn
                await _send(writer, msg)
                return await asyncio.wait_for(_recv(reader), self.cfg.rpc_timeout)
            except BaseException:
                dropped = self.conns.pop(peer, None)
                if dropped is not None:
                    dropped[1].close()
                raise

    def spawn(self, coro: Any) -> None:
        t = asyncio.get_running_loop().create_task(coro)
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)

    # -- server ------------------------------------------------------------------
    async def serve(self) -> None:
        host, port = self.cfg.peers[self.name]
        server = await asyncio.start_server(self.handle, host, port)
        self.spawn(self.election_loop())
        async with server:
            await server.serve_forever()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                msg = await _recv(reader)
                kind = msg["type"]
                if kind == "vote":
                    reply = self.on_vote(msg)
                elif kind == "append":
                    reply = self.on_append(msg)
                elif kind == "client":
                    reply = await self.on_client(msg)
                else:
                    reply = {"ok": False, "err": "bad request"}
                await _send(writer, reply)
        except (ConnectionError, asyncio.IncompleteReadError, TimeoutError):
            pass
        finally:
            writer.close()

    # -- elections ------------------------------------------------------------------
    async def election_loop(self) -> None:
        lo, hi = self.cfg.election_timeout
        while True:
            if self.role == "leader":
                await asyncio.sleep(self.cfg.heartbeat)
                continue
            self.heard.clear()
            try:
                await asyncio.wait_for(self.heard.wait(), random.uniform(lo, hi))
            except TimeoutError:
                await self.run_election()

    async def run_election(self) -> None:
        self.disk["term"] = self.term + 1
        self.disk["voted_for"] = self.name
        term = self.term
        self.role = "candidate"
        self.leader = None
        last_term, last_index = self.last()
        msg = {
            "type": "vote",
            "term": term,
            "candidate": self.name,
            "last_term": last_term,
            "last_index": last_index,
        }

        async def ask(peer: str) -> bool:
            try:
                r = await self.call(peer, msg)
            except (OSError, TimeoutError, ConnectionError, ValueError):
                return False
            if r["term"] > self.term:
                self.set_term(r["term"])
            return bool(r.get("granted"))

        results = await asyncio.gather(*(ask(p) for p in self.others))
        votes = 1 + sum(results)
        if self.role == "candidate" and self.term == term and votes >= self.majority:
            self.become_leader()

    def on_vote(self, msg: dict[str, Any]) -> dict[str, Any]:
        if msg["term"] > self.term:
            self.set_term(msg["term"])
        last_term, last_index = self.last()
        up_to_date = (msg["last_term"], msg["last_index"]) >= (last_term, last_index)
        if "split-brain" in self.cfg.bugs:
            free = True  # BUG: ignores an earlier vote in the same term
        else:
            free = self.disk["voted_for"] in (None, msg["candidate"])
        granted = msg["term"] == self.term and free and up_to_date
        if granted:
            self.disk["voted_for"] = msg["candidate"]
            self.heard.set()
        return {"term": self.term, "granted": granted}

    def become_leader(self) -> None:
        self.role = "leader"
        self.leader = self.name
        self.observer.on_leader(self.name, self.term)
        _, last_index = self.last()
        self.next_index = dict.fromkeys(self.others, last_index + 1)
        self.match_index = dict.fromkeys(self.others, 0)
        self.log.append([self.term, None])  # no-op entry of the new term
        self.term_start_index = len(self.log)
        term = self.term
        for p in self.others:
            self.spawn(self.replicate(p, term))

    # -- replication ------------------------------------------------------------------
    async def replicate(self, peer: str, term: int) -> None:
        while self.role == "leader" and self.term == term:
            ni = self.next_index[peer]
            prev_index = ni - 1
            prev_term = self.log[prev_index - 1][0] if prev_index > 0 else 0
            entries = self.log[ni - 1 :]
            started_round = self.round
            msg = {
                "type": "append",
                "term": term,
                "leader": self.name,
                "prev_index": prev_index,
                "prev_term": prev_term,
                "entries": entries,
                "commit": self.commit,
            }
            try:
                r = await self.call(peer, msg)
            except (OSError, TimeoutError, ConnectionError, ValueError):
                await asyncio.sleep(self.cfg.heartbeat)
                continue
            if r["term"] > self.term:
                self.set_term(r["term"])
                return
            if self.role != "leader" or self.term != term:
                return
            if r["ok"]:
                self.match_index[peer] = max(self.match_index[peer], prev_index + len(entries))
                self.next_index[peer] = self.match_index[peer] + 1
                self.note_ack(peer, started_round)
                self.advance_commit()
            else:
                self.next_index[peer] = max(1, min(ni - 1, r.get("hint", ni - 1)))
                continue
            self.kick.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.kick.wait(), self.cfg.heartbeat)

    def note_ack(self, peer: str, started_round: int) -> None:
        # A round is confirmed once a majority (counting the leader) acknowledged
        # an append that was sent after the round began.
        if started_round < self.round:
            return
        acks = self.acks
        acks.setdefault(started_round, set()).add(peer)
        while len(acks.get(self.round, ())) + 1 >= self.majority:
            acks.pop(self.round, None)
            self.round += 1
            self.round_event.set()
            self.round_event = asyncio.Event()

    def advance_commit(self) -> None:
        for n in range(len(self.log), self.commit, -1):
            if self.log[n - 1][0] != self.term:
                break
            replicas = 1 + sum(1 for p in self.others if self.match_index[p] >= n)
            if replicas >= self.majority:
                self.commit = n
                self.apply()
                break

    def on_append(self, msg: dict[str, Any]) -> dict[str, Any]:
        if msg["term"] < self.term:
            return {"term": self.term, "ok": False}
        if msg["term"] > self.term:
            self.set_term(msg["term"])
        self.become_follower(msg["leader"])
        self.heard.set()
        prev = msg["prev_index"]
        log = self.log
        if prev > len(log) or (prev > 0 and log[prev - 1][0] != msg["prev_term"]):
            return {"term": self.term, "ok": False, "hint": min(prev, len(log))}
        for i, entry in enumerate(msg["entries"]):
            idx = prev + i + 1
            if idx <= len(log):
                if log[idx - 1][0] != entry[0]:
                    del log[idx - 1 :]
                    log.append(entry)
            else:
                log.append(entry)
        last_new = prev + len(msg["entries"])
        if msg["commit"] > self.commit:
            self.commit = min(msg["commit"], last_new)
            self.apply()
        return {"term": self.term, "ok": True}

    def apply(self) -> None:
        while self.applied < self.commit:
            self.applied += 1
            cmd = self.log[self.applied - 1][1]
            if cmd is not None:
                self.kv[cmd["key"]] = cmd["value"]
            fut = self.waiters.pop(self.applied, None)
            if fut is not None and not fut.done():
                fut.set_result(True)

    # -- clients ------------------------------------------------------------------
    async def on_client(self, msg: dict[str, Any]) -> dict[str, Any]:
        op = msg["op"]
        if op == "get" and "stale-read" in self.cfg.bugs:
            return {"ok": True, "value": self.kv.get(msg["key"])}  # BUG: local read
        if self.role != "leader":
            return {"ok": False, "err": "not_leader", "leader": self.leader}
        term = self.term
        if op == "put":
            self.log.append([term, {"key": msg["key"], "value": msg["value"]}])
            index = len(self.log)
            self.kick.set()
            if "lost-ack" in self.cfg.bugs:
                return {"ok": True}  # BUG: acknowledged before replication
            fut = asyncio.get_running_loop().create_future()
            self.waiters[index] = fut
            committed = await fut
            if committed and self.term == term and self.log[index - 1][0] == term:
                return {"ok": True}
            return {"ok": False, "err": "unknown"}
        if op == "get":
            # read-index: the leader must have committed an entry of its term and
            # must confirm leadership with a majority after the read arrived.
            read_index = max(self.commit, self.term_start_index)
            # Round ``self.round`` may have started before this read arrived; the
            # round after it started strictly later, so wait for it to complete.
            target_round = self.round + 2
            self.kick.set()
            while self.round < target_round or self.applied < read_index:
                if self.role != "leader" or self.term != term:
                    return {"ok": False, "err": "unknown"}
                ev = self.round_event
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(ev.wait(), self.cfg.heartbeat * 2)
            return {"ok": True, "value": self.kv.get(msg["key"])}
        return {"ok": False, "err": "bad op"}


async def run_node(
    name: str, cfg: KVConfig, disk: dict[str, Any], observer: Observer | None = None
) -> None:
    """Entry point for one KV node (real asyncio or detloop)."""
    node = RaftNode(name, cfg, disk, observer or Observer())
    await node.serve()


class KVClient:
    """Client with leader discovery; records each operation in a history.

    Status rules: ``not_leader`` before execution -> ``fail`` (definitely not
    applied); timeouts, resets after sending, or ``unknown`` -> ``info``; a
    request that was never sent -> ``fail``. Indeterminate requests are not
    retried, so a ``put`` is applied at most once.
    """

    def __init__(
        self,
        name: str,
        peers: dict[str, tuple[str, int]],
        history: Any,
        rng: Any,
        timeout: float = 1.0,
    ) -> None:
        self.name = name
        self.peers = peers
        self.history = history
        self.rng = rng
        self.timeout = timeout
        self.leader: str | None = None

    async def _request(self, node: str, msg: dict[str, Any]) -> dict[str, Any]:
        host, port = self.peers[node]
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), self.timeout)
        try:
            sent = False
            try:
                await _send(writer, msg)
                sent = True
                return await asyncio.wait_for(_recv(reader), self.timeout)
            except BaseException as exc:
                exc.sent = sent  # type: ignore[attr-defined]
                raise
        finally:
            writer.close()

    async def op(self, f: str, key: str, value: Any = None) -> Any:
        op = self.history.invoke(self.name, f, key, value)
        names = list(self.peers)
        for _ in range(len(names) + 1):
            node = self.leader if self.leader in self.peers else self.rng.choice(names)
            msg = {"type": "client", "op": f, "key": key, "value": value}
            try:
                reply = await self._request(node, msg)
            except (OSError, TimeoutError, ConnectionError, ValueError) as exc:
                self.leader = None
                if getattr(exc, "sent", False):
                    op.info(exc)
                    return None
                continue
            if reply.get("ok"):
                op.ok(reply.get("value") if f == "get" else None)
                return reply.get("value")
            if reply.get("err") == "not_leader":
                self.leader = reply.get("leader")
                continue
            op.info(reply.get("err"))
            return None
        op.fail("no leader found")
        return None
