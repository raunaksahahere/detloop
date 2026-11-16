"""Determinism guards.

While a simulation runs, :func:`installed` replaces a documented set of
module attributes so that common routes to host time, entropy, threads,
processes and the real network are either *virtualized* or *rejected*:

=====================================  ==========================================
API                                    Inside a simulation
=====================================  ==========================================
``time.time/time_ns``                  current node's virtual wall clock
``time.monotonic/perf_counter(_ns)``   virtual monotonic time (``loop.time()``)
``time.sleep``                         :class:`NondeterminismError`
``random`` module functions            seeded from the run seed; state restored
``os.urandom``, ``secrets``,           strict: :class:`NondeterminismError`;
``random.SystemRandom``, ``uuid4``     seeded: deterministic bytes
``socket.socket``                      :class:`~detloop.net.socket.SimSocket`
``socket.getaddrinfo/gethostbyname``   simulated DNS
``socket.gethostname``                 current node name
``socket.create_connection``,          :class:`NondeterminismError`
``socket.socketpair/create_server``
``threading.Thread.start``             :class:`NondeterminismError`
``subprocess.Popen``, ``os.system``,   :class:`NondeterminismError`
``os.popen``, ``os.fork``
=====================================  ==========================================

Patched functions only change behaviour on the simulation thread; other
threads (pytest internals, the wall-clock watchdog) see the originals. All
patches are restored on success, failure, cancellation and interrupts.
Nested simulations in one thread are rejected.

This is **not a sandbox**: code that cached an alias before the simulation
started (``from time import monotonic``), C extensions, ``datetime.now()``
and direct system calls are not intercepted. See docs/DETERMINISM.md.
"""

from __future__ import annotations

import os
import random
import socket
import subprocess
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from .errors import ConfigError, NondeterminismError
from .rng import SplitMix64

if TYPE_CHECKING:
    from .world import World

NS = 1_000_000_000

_ORIGINALS: dict[tuple[Any, str], Any] = {}
_active_lock = threading.Lock()
_active: dict[int, World] = {}


def _orig(target: Any, name: str) -> Any:
    key = (target, name)
    if key not in _ORIGINALS:
        _ORIGINALS[key] = getattr(target, name)
    return _ORIGINALS[key]


def active_world() -> World | None:
    """The world simulated on the current thread, if any."""
    return _active.get(threading.get_ident())


def violation(message: str) -> NondeterminismError:
    """Build a :class:`NondeterminismError`, recording it stickily if a simulation is active."""
    world = active_world()
    if world is None:
        return NondeterminismError(message)
    return _violation(world, message)


def _violation(world: World, message: str) -> NondeterminismError:
    exc = NondeterminismError(message)
    world.loop.abort(exc)  # sticky: fails the run even if the caller swallows it
    world.trace.emit("nondeterminism", 1, message=message)
    return exc


_installed_patches: list[tuple[Any, str, Any]] = []


@contextmanager
def installed(world: World) -> Iterator[None]:
    """Activate guards for ``world`` on the current thread (patches are
    reference-counted across threads and removed when the last simulation ends)."""
    global _installed_patches
    tid = threading.get_ident()
    with _active_lock:
        if tid in _active:
            raise ConfigError("nested simulations on one thread are not supported")
        if not _active:
            _installed_patches = _install_patches()
        _active[tid] = world
    saved_random = random.getstate()
    random.seed(world.decisions.entropy_seed())
    world._entropy = SplitMix64(world.decisions.entropy_seed() ^ 0x5EED)  # type: ignore[attr-defined]
    try:
        yield
    finally:
        random.setstate(saved_random)
        with _active_lock:
            _active.pop(tid, None)
            if not _active:
                for target, name, original in reversed(_installed_patches):
                    setattr(target, name, original)
                _installed_patches = []


def _wrap(target: Any, name: str, make: Callable[[Any], Any], patches: list[tuple[Any, str, Any]]) -> None:
    original = _orig(target, name)
    current = getattr(target, name)
    patches.append((target, name, current))
    setattr(target, name, make(original))


def _install_patches() -> list[tuple[Any, str, Any]]:
    patches: list[tuple[Any, str, Any]] = []

    # --- time ------------------------------------------------------------
    def virt_mono(orig: Callable[[], float]) -> Callable[[], float]:
        def monotonic() -> float:
            w = active_world()
            return orig() if w is None else w.loop.now_ns / NS

        return monotonic

    def virt_mono_ns(orig: Callable[[], int]) -> Callable[[], int]:
        def monotonic_ns() -> int:
            w = active_world()
            return orig() if w is None else w.loop.now_ns

        return monotonic_ns

    def virt_wall(orig: Callable[[], float]) -> Callable[[], float]:
        def wall() -> float:
            w = active_world()
            return orig() if w is None else w.current_node().wall_time()

        return wall

    def virt_wall_ns(orig: Callable[[], int]) -> Callable[[], int]:
        def wall_ns() -> int:
            w = active_world()
            if w is None:
                return orig()
            return round(w.config.epoch * NS) + w.current_node().local_time_ns()

        return wall_ns

    def no_sleep(orig: Callable[[float], None]) -> Callable[[float], None]:
        def sleep(seconds: float) -> None:
            w = active_world()
            if w is None:
                return orig(seconds)
            raise _violation(w, "time.sleep() blocks the simulation thread; use 'await asyncio.sleep()'")

        return sleep

    _wrap(time, "monotonic", virt_mono, patches)
    _wrap(time, "perf_counter", virt_mono, patches)
    _wrap(time, "monotonic_ns", virt_mono_ns, patches)
    _wrap(time, "perf_counter_ns", virt_mono_ns, patches)
    _wrap(time, "time", virt_wall, patches)
    _wrap(time, "time_ns", virt_wall_ns, patches)
    _wrap(time, "sleep", no_sleep, patches)

    # --- entropy ----------------------------------------------------------
    def guarded_urandom(orig: Callable[[int], bytes]) -> Callable[[int], bytes]:
        def urandom(n: int) -> bytes:
            w = active_world()
            if w is None:
                return orig(n)
            if w.config.entropy == "seeded":
                return w._entropy.bytes(n)  # type: ignore[attr-defined,no-any-return]
            raise _violation(
                w,
                "os.urandom()/secrets/SystemRandom/uuid4 read uncontrolled entropy; "
                "use World.rng, or opt in with entropy='seeded'",
            )

        return urandom

    _wrap(os, "urandom", guarded_urandom, patches)
    if hasattr(random, "_urandom"):
        _wrap(random, "_urandom", guarded_urandom, patches)

    # --- sockets ----------------------------------------------------------
    from .net.socket import SimSocket

    def socket_factory(orig: Any) -> Any:
        def make_socket(family: int = -1, type: int = -1, proto: int = -1, fileno: int | None = None) -> Any:
            w = active_world()
            if w is None:
                return orig(family, type, proto, fileno)
            fam = socket.AF_INET if family == -1 else family
            typ = socket.SOCK_STREAM if type == -1 else type
            return SimSocket(fam, typ, 0 if proto == -1 else proto, fileno)

        return make_socket

    def blocked(what: str, hint: str) -> Callable[[Any], Any]:
        def make(orig: Any) -> Any:
            def guard(*args: Any, **kwargs: Any) -> Any:
                w = active_world()
                if w is None:
                    return orig(*args, **kwargs)
                raise _violation(w, f"{what} is outside the determinism boundary; {hint}")

            return guard

        return make

    def sim_getaddrinfo(orig: Any) -> Any:
        def getaddrinfo(host: Any, port: Any, family: int = 0, type: int = 0, proto: int = 0, flags: int = 0) -> Any:
            w = active_world()
            if w is None:
                return orig(host, port, family, type, proto, flags)
            return w.network.getaddrinfo(host, port, family=family, type=type, proto=proto, flags=flags)

        return getaddrinfo

    def sim_gethostbyname(orig: Any) -> Any:
        def gethostbyname(host: str) -> str:
            w = active_world()
            if w is None:
                return orig(host)  # type: ignore[no-any-return]
            return w.network.resolve(host)

        return gethostbyname

    def sim_gethostname(orig: Any) -> Any:
        def gethostname() -> str:
            w = active_world()
            if w is None:
                return orig()  # type: ignore[no-any-return]
            return w.current_node().name

        return gethostname

    _wrap(socket, "socket", socket_factory, patches)
    _wrap(socket, "getaddrinfo", sim_getaddrinfo, patches)
    _wrap(socket, "gethostbyname", sim_gethostbyname, patches)
    _wrap(socket, "gethostname", sim_gethostname, patches)
    _wrap(socket, "create_connection", blocked("socket.create_connection()", "use asyncio.open_connection()"), patches)
    _wrap(socket, "create_server", blocked("socket.create_server()", "use asyncio.start_server()"), patches)
    _wrap(socket, "socketpair", blocked("socket.socketpair()", "nested event loops and pipes are not simulated"), patches)

    # --- threads and processes ---------------------------------------------
    def guarded_thread_start(orig: Any) -> Any:
        def start(self: threading.Thread) -> None:
            w = active_world()
            if w is None:
                return orig(self)  # type: ignore[no-any-return]
            raise _violation(
                w,
                f"starting thread {self.name!r} inside a simulation; threads (including executors and "
                "asyncio.to_thread) are outside the determinism boundary",
            )

        return start

    _wrap(threading.Thread, "start", guarded_thread_start, patches)

    def guarded_popen_init(orig: Any) -> Any:
        def __init__(self: Any, *args: Any, **kwargs: Any) -> None:  # noqa: N807
            w = active_world()
            if w is None:
                return orig(self, *args, **kwargs)  # type: ignore[no-any-return]
            raise _violation(w, "subprocesses are outside the determinism boundary")

        return __init__

    _wrap(subprocess.Popen, "__init__", guarded_popen_init, patches)
    for name in ("system", "popen", "fork", "forkpty"):
        if hasattr(os, name):
            _wrap(os, name, blocked(f"os.{name}()", "processes are not simulated"), patches)
    return patches
