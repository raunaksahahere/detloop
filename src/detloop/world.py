"""The simulated world: nodes, incarnations, faults, invariants.

A :class:`World` is created per run and passed to the simulated test function.
See docs/API.md for the full contract and docs/FAULT_MODEL.md for crash
semantics.
"""

from __future__ import annotations

import asyncio
import contextvars
from collections.abc import Awaitable, Callable, Coroutine, Iterable
from typing import TYPE_CHECKING, Any

from . import _compat
from ._owner import OWNER, Owner
from .config import SimConfig
from .errors import ConfigError, InvariantViolation, NodeFailure
from .faults import Fault
from .history import History
from .loop import NS, SimLoop
from .net.messages import MessageEndpoint
from .net.network import Network
from .rng import Decisions, SimRandom
from .trace import Trace

if TYPE_CHECKING:
    from .net.server import SimServer
    from .net.stream import Endpoint

NodeRef = "Node | str"
Factory = Callable[..., Awaitable[Any]]


class Registry:
    """Network resources owned by one incarnation (or by the control context)."""

    __slots__ = ("endpoints", "listeners", "msg_endpoints")

    def __init__(self) -> None:
        self.endpoints: dict[str, Endpoint] = {}
        self.listeners: list[SimServer] = []
        self.msg_endpoints: list[MessageEndpoint] = []


class Incarnation(Owner):
    """One lifetime of a node, from start/restart until crash."""

    __slots__ = ("context", "generation", "main_task", "node", "registry", "volatile")

    def __init__(self, node: Node, generation: int, base: contextvars.Context) -> None:
        super().__init__(f"{node.name}#{generation}")
        self.node = node
        self.generation = generation
        self.registry = Registry()
        self.volatile: dict[str, Any] = {}
        self.main_task: asyncio.Task[Any] | None = None
        ctx = base.copy()
        ctx.run(OWNER.set, self)
        self.context = ctx


class Node:
    """A simulated host with an address, a lifecycle and crash/restart semantics.

    * ``disk`` survives crashes (a retained in-memory mapping; *not* a
      filesystem or fsync model: every assignment is durable immediately).
    * ``volatile`` is a fresh dict for every incarnation.
    """

    def __init__(self, world: World, name: str, address: str, crashable: bool = True) -> None:
        self.world = world
        self.name = name
        self.address = address
        self.crashable = crashable
        self.disk: dict[str, Any] = {}
        self.generation = 0
        self.incarnation: Incarnation | None = None
        self.state = "new"  # new | up | down | exited
        self.factory: Factory | None = None
        self.args: tuple[Any, ...] = ()
        self.clock_offset_ns = 0
        self.clock_drift_ppm = 0.0
        self._drift_base_ns = 0
        self.exit_value: Any = None

    def __repr__(self) -> str:
        return f"<Node {self.name} {self.address} {self.state} gen={self.generation}>"

    @property
    def up(self) -> bool:
        return self.state in ("up", "exited") and (
            self.incarnation is not None or not self.crashable
        )

    @property
    def volatile(self) -> dict[str, Any]:
        if self.incarnation is None:
            raise RuntimeError(f"node {self.name} is not running")
        return self.incarnation.volatile

    # -- lifecycle -------------------------------------------------------
    def start(self, factory: Factory, *args: Any) -> asyncio.Task[Any]:
        """Start the node by running ``await factory(node, *args)`` as its main task.

        The same factory is re-run (with fresh volatile state) on restart.
        """
        if not self.crashable:
            raise ConfigError("the control node cannot be started")
        if self.state == "up":
            raise RuntimeError(f"node {self.name} is already running")
        self.factory = factory
        self.args = args
        return self._boot()

    def _boot(self) -> asyncio.Task[Any]:
        assert self.factory is not None
        w = self.world
        self.generation += 1
        inc = Incarnation(self, self.generation, w.base_context)
        self.incarnation = inc
        self.state = "up"
        w.trace.emit("node_start", 1, node=inc.label)
        task = w.loop.create_task(
            self._main(inc), name=f"{inc.label}:main", context=inc.context.copy()
        )
        inc.main_task = task
        return task

    async def _main(self, inc: Incarnation) -> None:
        assert self.factory is not None
        try:
            self.exit_value = await self.factory(self, *self.args)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.world._node_error(self, inc, exc)
        else:
            if inc.alive:
                self.world.trace.emit("node_exit", 1, node=inc.label)

    def crash(self) -> None:
        """Crash the node: freeze its tasks, drop its timers/callbacks, kill its connections."""
        if not self.crashable:
            raise ConfigError("the control node cannot crash")
        inc = self.incarnation
        if inc is None or self.state not in ("up", "exited"):
            raise RuntimeError(f"node {self.name} is not running")
        w = self.world
        inc.alive = False
        self.incarnation = None
        self.state = "down"
        w.loop.drop_owner(inc)
        frozen = w.loop.tasks_of(inc)
        for t in frozen:
            _compat.silence_pending_destroy(t)
        w._frozen.extend(frozen)
        w.network.crash_incarnation(inc)
        w.trace.emit("node_crash", 1, node=inc.label, frozen_tasks=len(frozen))

    def restart(self) -> asyncio.Task[Any]:
        """Start a new incarnation with the stored factory (crashing first if running)."""
        if self.factory is None:
            raise RuntimeError(f"node {self.name} was never started")
        if self.state in ("up", "exited"):
            self.crash()
        return self._boot()

    def spawn(
        self, coro: Coroutine[Any, Any, Any], *, name: str | None = None
    ) -> asyncio.Task[Any]:
        """Run ``coro`` as a task owned by the node's current incarnation."""
        inc = self.incarnation
        if inc is None:
            coro.close()
            raise RuntimeError(f"node {self.name} is not running")
        return self.world.loop.create_task(coro, name=name, context=inc.context.copy())

    def messages(self, port: int) -> MessageEndpoint:
        """Bind a datagram-style message port owned by this incarnation."""
        inc = self.incarnation
        if self.crashable and inc is None:
            raise RuntimeError(f"node {self.name} is not running")
        return self.world.network.bind_messages(self, inc, port)

    # -- local clock -------------------------------------------------------
    def set_clock(self, offset: float | None = None, drift_ppm: float | None = None) -> None:
        """Skew this node's *local wall clock* (``time.time()``); ``loop.time()`` is unaffected."""
        now = self.world.loop.now_ns
        if drift_ppm is not None:
            # fold accumulated drift into the offset, then restart drift accounting
            self.clock_offset_ns += round(self.clock_drift_ppm * (now - self._drift_base_ns) / 1e6)
            self._drift_base_ns = now
            self.clock_drift_ppm = drift_ppm
        if offset is not None:
            self.clock_offset_ns = round(offset * NS)
        self.world.trace.emit(
            "clock",
            1,
            node=self.name,
            offset_ns=self.clock_offset_ns,
            drift_ppm=self.clock_drift_ppm,
        )

    def local_time_ns(self) -> int:
        now = self.world.loop.now_ns
        drift = (
            round(self.clock_drift_ppm * (now - self._drift_base_ns) / 1e6)
            if self.clock_drift_ppm
            else 0
        )
        return now + self.clock_offset_ns + drift

    def wall_time(self) -> float:
        """This node's (possibly skewed) wall-clock time in seconds since the epoch."""
        return self.world.config.epoch + self.local_time_ns() / NS


class World:
    """Everything a simulated test can see and control."""

    def __init__(self, config: SimConfig, seed: int, decisions: Decisions, trace: Trace) -> None:
        self.config = config
        self.seed = seed
        self.decisions = decisions
        self.trace = trace
        self.loop = SimLoop(config=config, decisions=decisions, trace=trace, world=self)
        self.base_context = contextvars.copy_context()
        self.base_context.run(OWNER.set, None)
        self.control_context = self.base_context.copy()
        self.network = Network(self)
        self.nodes: dict[str, Node] = {}
        self.control = Node(self, "control", "10.0.0.254", crashable=False)
        self.control.state = "up"
        self.network.register_host(self.control)
        self._control_registry = Registry()
        for name in config.nodes:
            self.add_node(name)
        self.rng = SimRandom(decisions)
        self.history = History(self)
        self._frozen: list[asyncio.Task[Any]] = []
        self.faults: list[Fault] = []
        self._invariants: list[tuple[str, Callable[[], Any]]] = []
        self.violations: list[InvariantViolation] = []
        self.node_errors: list[NodeFailure] = []
        self._fault_state: dict[str, Any] = {}

    # -- node access -------------------------------------------------------
    def add_node(self, name: str) -> Node:
        """Add a node (for example a client host). Addresses are 10.0.0.<index>."""
        if name in self.nodes or name in ("control", "localhost"):
            raise ConfigError(f"node name {name!r} is taken or reserved")
        index = len(self.nodes) + 1
        if index > 250:
            raise ConfigError("at most 250 nodes are supported")
        node = Node(self, name, f"10.0.0.{index}")
        self.nodes[name] = node
        self.network.register_host(node)
        return node

    def node(self, ref: Node | str) -> Node:
        if isinstance(ref, Node):
            return ref
        if ref == "control":
            return self.control
        try:
            return self.nodes[ref]
        except KeyError:
            raise ConfigError(f"unknown node {ref!r}; nodes: {list(self.nodes)}") from None

    def __getitem__(self, ref: str) -> Node:
        return self.node(ref)

    @property
    def node_names(self) -> list[str]:
        return list(self.nodes)

    def _nodes(self, refs: Node | str | Iterable[Node | str] | None) -> list[Node]:
        if refs is None:
            return list(self.nodes.values())
        if isinstance(refs, Node | str):
            return [self.node(refs)]
        return [self.node(r) for r in refs]

    def _registry(self, inc: Incarnation | None) -> Registry:
        return self._control_registry if inc is None else inc.registry

    # -- lifecycle helpers ---------------------------------------------------
    def start(
        self, factory: Factory, *args: Any, nodes: Iterable[Node | str] | None = None
    ) -> None:
        """Start ``factory`` on every node (or on ``nodes``)."""
        for n in self._nodes(nodes):
            n.start(factory, *args)

    def crash(self, node: Node | str, *, restart_after: float | None = None) -> None:
        n = self.node(node)
        n.crash()
        if restart_after is not None:
            gen = n.generation
            self.loop.call_later(
                restart_after, self._restart_if_down, n, gen, context=self.control_context.copy()
            )

    def _restart_if_down(self, n: Node, generation: int) -> None:
        if n.state == "down" and n.generation == generation:
            n.restart()

    def restart(self, node: Node | str) -> None:
        self.node(node).restart()

    def spawn(
        self, coro: Coroutine[Any, Any, Any], *, name: str | None = None
    ) -> asyncio.Task[Any]:
        """Run ``coro`` as a control-owned task (never crashed)."""
        return self.loop.create_task(coro, name=name, context=self.control_context.copy())

    # -- network faults ------------------------------------------------------
    def partition(
        self,
        a: Node | str | Iterable[Node | str],
        b: Node | str | Iterable[Node | str] | None = None,
        *,
        oneway: bool = False,
    ) -> None:
        """Block traffic between ``a`` and ``b`` (both directions unless ``oneway``,
        in which case only ``a -> b`` is blocked). ``b=None`` isolates ``a`` from
        every other node, including the control node."""
        side_a = self._nodes(a)
        if b is None:
            names = {n.name for n in side_a}
            side_b = [n for n in [*self.nodes.values(), self.control] if n.name not in names]
        else:
            side_b = self._nodes(b)
        for x in side_a:
            for y in side_b:
                if x is y:
                    continue
                self.network.block(x, y)
                if not oneway:
                    self.network.block(y, x)
        self.trace.emit(
            "partition",
            1,
            a=[n.name for n in side_a],
            b=[n.name for n in side_b],
            oneway=oneway or None,
        )

    def heal(
        self,
        a: Node | str | Iterable[Node | str] | None = None,
        b: Node | str | Iterable[Node | str] | None = None,
        *,
        oneway: bool = False,
    ) -> None:
        """Undo a :meth:`partition` with the same arguments; ``heal()`` heals everything."""
        if a is None:
            self.network.heal_all()
            self.trace.emit("heal", 1, all=True)
            return
        side_a = self._nodes(a)
        if b is None:
            names = {n.name for n in side_a}
            side_b = [n for n in [*self.nodes.values(), self.control] if n.name not in names]
        else:
            side_b = self._nodes(b)
        for x in side_a:
            for y in side_b:
                if x is y:
                    continue
                self.network.unblock(x, y)
                if not oneway:
                    self.network.unblock(y, x)
        self.trace.emit(
            "heal", 1, a=[n.name for n in side_a], b=[n.name for n in side_b], oneway=oneway or None
        )

    def reset_connections(self, a: Node | str, b: Node | str) -> int:
        """Reset all live connections between two nodes.

        Both sides see ``ConnectionResetError``."""
        n = self.network.reset_between(self.node(a), self.node(b))
        self.trace.emit("reset", 1, a=self.node(a).name, b=self.node(b).name, connections=n)
        return n

    def slow(self, a: Node | str, b: Node | str, factor: float) -> Callable[[], None]:
        """Multiply latency between ``a`` and ``b`` by ``factor``; returns an undo function."""
        x, y = self.node(a), self.node(b)
        self.network.push_latency_factor(x, y, factor)
        self.network.push_latency_factor(y, x, factor)
        self.trace.emit("latency", 1, a=x.name, b=y.name, factor=factor)

        def undo() -> None:
            self.network.pop_latency_factor(x, y, factor)
            self.network.pop_latency_factor(y, x, factor)
            self.trace.emit("latency_end", 1, a=x.name, b=y.name, factor=factor)

        return undo

    # -- fault schedules -----------------------------------------------------
    def install_faults(self, faults: list[Fault]) -> None:
        self.faults = list(faults)
        for f in self.faults:
            self.loop._call_at_ns(f.at, self._apply_fault, (f,), self.control_context.copy())

    def _end_at(self, f: Fault, fn: Callable[..., None], *args: Any) -> None:
        self.loop._call_at_ns(
            f.at + f.duration, self._framework_call, (fn, *args), self.control_context.copy()
        )

    def _framework_call(self, fn: Callable[..., None], *args: Any) -> None:
        """Run a detloop-owned callback; a failure is an internal error, never a loop warning."""
        try:
            fn(*args)
        except Exception as exc:
            from .errors import InternalError

            err = InternalError(
                f"detloop internal error in {getattr(fn, '__qualname__', fn)}: "
                f"{type(exc).__name__}: {exc}"
            )
            err.__cause__ = exc
            self.loop.abort(err)

    def _apply_fault(self, f: Fault) -> None:
        self._framework_call(self._apply_fault_unchecked, f)

    def _apply_fault_unchecked(self, f: Fault) -> None:
        p = f.params
        self.trace.emit("fault", 1, fault=f.id, kind=f.kind, params=p, duration=f.duration or None)
        if f.kind == "partition":
            if p.get("oneway"):
                a, b = self.node(p["a"]), self.node(p["b"])
                self.network.block(a, b)
                self._end_at(f, self._end_oneway, f, a, b)
            else:
                ga, gb = self._nodes(p["groups"][0]), self._nodes(p["groups"][1])
                for x in ga:
                    for y in gb:
                        self.network.block(x, y)
                        self.network.block(y, x)
                self._end_at(f, self._end_split, f, ga, gb)
        elif f.kind == "crash":
            n = self.node(p["node"])
            if n.state not in ("up", "exited") or n.incarnation is None:
                self.trace.emit("fault_skipped", 1, fault=f.id, reason="node not running")
                return
            n.crash()
            if f.duration:
                self._end_at(f, self._end_crash, f, n, n.generation)
        elif f.kind == "reset":
            self.network.reset_between(self.node(p["a"]), self.node(p["b"]))
        elif f.kind == "latency":
            a, b = self.node(p["a"]), self.node(p["b"])
            self.network.push_latency_factor(a, b, p["factor"])
            self.network.push_latency_factor(b, a, p["factor"])
            self._end_at(f, self._end_latency, f, a, b)
        elif f.kind == "clock":
            n = self.node(p["node"])
            saved = (n.clock_offset_ns, n.clock_drift_ppm)
            n.set_clock(offset=p["offset_ns"] / NS, drift_ppm=float(p["drift_ppm"]))
            if f.duration:
                self._end_at(f, self._end_clock, f, n, saved)
        else:  # pragma: no cover - validated at load time
            raise ConfigError(f"unknown fault kind {f.kind!r}")

    def _end_oneway(self, f: Fault, a: Node, b: Node) -> None:
        self.network.unblock(a, b)
        self.trace.emit("fault_end", 1, fault=f.id)

    def _end_split(self, f: Fault, ga: list[Node], gb: list[Node]) -> None:
        for x in ga:
            for y in gb:
                self.network.unblock(x, y)
                self.network.unblock(y, x)
        self.trace.emit("fault_end", 1, fault=f.id)

    def _end_crash(self, f: Fault, n: Node, generation: int) -> None:
        if n.state == "down" and n.generation == generation:
            self.trace.emit("fault_end", 1, fault=f.id)
            n.restart()

    def _end_latency(self, f: Fault, a: Node, b: Node) -> None:
        self.network.pop_latency_factor(a, b, f.params["factor"])
        self.network.pop_latency_factor(b, a, f.params["factor"])
        self.trace.emit("fault_end", 1, fault=f.id)

    def _end_clock(self, f: Fault, n: Node, saved: tuple[int, float]) -> None:
        n.set_clock(offset=saved[0] / NS, drift_ppm=saved[1])
        self.trace.emit("fault_end", 1, fault=f.id)

    # -- invariants ------------------------------------------------------------
    def check(self, condition: Any, name: str, message: str = "") -> bool:
        """Assert an invariant. A failure is recorded and fails the run as soon as the
        current callback returns, even if the caller (for example node code)
        swallows exceptions. Returns ``bool(condition)``."""
        if condition:
            return True
        violation = InvariantViolation(name, message)
        self.violations.append(violation)
        self.trace.emit("invariant_failed", 1, name=name, message=message)
        self.loop.abort(violation)
        return False

    def fail(self, name: str, message: str = "") -> None:
        """Unconditionally fail the run with an invariant violation named ``name``."""
        self.check(False, name, message)

    def invariant(self, name: str, predicate: Callable[[], Any], *, every: float = 0.05) -> None:
        """Check ``predicate()`` every ``every`` virtual seconds and at the end of the run."""
        self._invariants.append((name, predicate))

        def tick() -> None:
            try:
                ok = predicate()
            except Exception as exc:  # a broken predicate fails the run with its own error
                self.loop.abort(exc)
                return
            if not self.check(ok, name):
                return
            self.loop.call_later(every, tick, context=self.control_context.copy())

        self.loop.call_later(every, tick, context=self.control_context.copy())

    def check_invariants(self) -> None:
        for name, predicate in self._invariants:
            self.check(predicate(), name, "checked at end of run")

    def _node_error(self, node: Node, inc: Incarnation, exc: BaseException) -> None:
        failure = NodeFailure(inc.label, exc)
        self.node_errors.append(failure)
        self.trace.emit(
            "node_error", 1, node=inc.label, error=type(exc).__qualname__, message=str(exc)
        )
        if self.config.fail_on_node_error:
            self.loop.abort(failure)

    # -- misc ------------------------------------------------------------------
    def time(self) -> float:
        """Virtual monotonic time in seconds (same as ``loop.time()``)."""
        return self.loop.time()

    @property
    def now_ns(self) -> int:
        return self.loop.now_ns

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    def note(self, kind: str, **fields: Any) -> None:
        """Record a custom event in the trace (shown in the HTML report)."""
        self.trace.emit("note", 1, note=kind, **fields)

    def current_node(self) -> Node:
        """The node owning the currently running code (``control`` for the test body)."""
        return self.network.current_node()
