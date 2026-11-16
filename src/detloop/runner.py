"""Running one simulation: :func:`run` and :class:`RunResult`.

Outcome classes (see docs/API.md):

=============  ==============================================================
``pass``       the test function returned and no invariant failed
``fail``       assertion, invariant, linearizability violation, exception,
               node error, deadlock, starvation, nondeterminism, unsupported
``incomplete`` a budget was exhausted, the checker returned *unknown*, or the
               supervisory wall-clock timeout fired. Never counted as a pass.
``error``      an internal detloop error
=============  ==============================================================
"""

from __future__ import annotations

import asyncio
import gc
import inspect
import os
import signal
import threading
import time as _time
import traceback
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from . import guards
from ._version import __version__
from .config import SimConfig
from .errors import (
    BudgetExceeded,
    CheckIncomplete,
    ConfigError,
    DeadlockError,
    InternalError,
    InvariantViolation,
    LinearizabilityViolation,
    NodeFailure,
    NondeterminismError,
    ReplayDivergence,
    StarvationError,
    UnsupportedOperation,
    WallClockTimeout,
)
from .faults import Fault, generate, schedule_to_json
from .rng import Decisions, Tape
from .trace import Trace, sanitize_text
from .world import World

_perf = _time.perf_counter  # cached before any simulation patches time

PASS, FAIL, INCOMPLETE, ERROR = "pass", "fail", "incomplete", "error"


@dataclass
class RunResult:
    seed: int
    status: str
    kind: str
    signature: str
    message: str
    steps: int = 0
    virtual_ns: int = 0
    trace_sha256: str = ""
    trace_events: int = 0
    trace_truncated: bool = False
    faults: list[dict[str, Any]] = field(default_factory=list)
    tapes: dict[str, dict[str, list[int]]] = field(default_factory=dict)
    decisions: int = 0
    loop_errors: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    history: dict[str, int] = field(default_factory=dict)
    details: list[str] = field(default_factory=list)
    network: dict[str, int] = field(default_factory=dict)
    config_fingerprint: str = ""
    decision_mode: str = "seeded"
    # non-canonical metadata
    wall_seconds: float = 0.0
    trace_lines: list[str] | None = None
    traceback: str | None = None

    @property
    def passed(self) -> bool:
        return self.status == PASS

    @property
    def failed(self) -> bool:
        return self.status != PASS

    def summary(self) -> dict[str, Any]:
        return {
            "seed": self.seed,
            "status": self.status,
            "kind": self.kind,
            "signature": self.signature,
            "message": self.message,
            "steps": self.steps,
            "virtual_time": self.virtual_ns / 1e9,
            "trace_sha256": self.trace_sha256,
            "trace_events": self.trace_events,
            "faults": len(self.faults),
            "decisions": self.decisions,
            "wall_seconds": round(self.wall_seconds, 6),
        }


def _user_frame(tb: Any) -> str:
    """``file:line`` of the innermost frame outside detloop and asyncio."""
    here = os.path.dirname(os.path.abspath(__file__))
    asyncio_dir = os.path.dirname(asyncio.__file__)
    best = ""
    for frame in traceback.extract_tb(tb):
        fn = os.path.abspath(frame.filename)
        if fn.startswith((here + os.sep, asyncio_dir + os.sep)):
            continue
        best = f"{os.path.basename(frame.filename)}:{frame.lineno}"
    return best


def classify(exc: BaseException) -> tuple[str, str, str]:
    """Return ``(status, kind, signature)`` for an exception that ended a run."""
    name = type(exc).__qualname__
    if isinstance(exc, InvariantViolation):
        return FAIL, "invariant", f"invariant:{exc.name}"
    if isinstance(exc, LinearizabilityViolation):
        return FAIL, "linearizability", "linearizability"
    if isinstance(exc, CheckIncomplete):
        return INCOMPLETE, "checker_unknown", "checker_unknown"
    if isinstance(exc, DeadlockError):
        return FAIL, "deadlock", "deadlock"
    if isinstance(exc, StarvationError):
        return FAIL, "starvation", "starvation"
    if isinstance(exc, BudgetExceeded):
        return INCOMPLETE, "budget", f"budget:{exc.budget}"
    if isinstance(exc, WallClockTimeout):
        return INCOMPLETE, "wall_timeout", "wall_timeout"
    if isinstance(exc, NondeterminismError):
        return FAIL, "nondeterminism", "nondeterminism"
    if isinstance(exc, UnsupportedOperation):
        return FAIL, "unsupported", "unsupported"
    if isinstance(exc, NodeFailure):
        inner = type(exc.exc).__qualname__
        return FAIL, "node_error", f"node_error:{inner}:{_user_frame(exc.exc.__traceback__)}"
    if isinstance(exc, InternalError | ReplayDivergence):
        return (
            ERROR,
            "internal_error" if isinstance(exc, InternalError) else "replay_divergence",
            name,
        )
    if isinstance(exc, AssertionError):
        return FAIL, "assertion", f"assertion:{_user_frame(exc.__traceback__)}"
    return FAIL, "exception", f"exception:{name}:{_user_frame(exc.__traceback__)}"


class _Watchdog:
    """Supervisory wall-clock timeout, independent of virtual time.

    On the main thread it interrupts even a CPU-bound coroutine that never
    yields (via a temporary SIGINT handler); elsewhere it can only stop the
    run at the loop's next periodic check. The explorer additionally kills
    worker processes that overrun.
    """

    def __init__(self, timeout: float | None, flag: threading.Event) -> None:
        self.timeout = timeout
        self.flag = flag
        self.fired = False
        self._done = threading.Event()
        self._thread: threading.Thread | None = None
        self._old_handler: Any = None
        self._main = threading.current_thread() is threading.main_thread()

    def __enter__(self) -> _Watchdog:
        if not self.timeout:
            return self
        if self._main:
            try:
                self._old_handler = signal.getsignal(signal.SIGINT)
                signal.signal(signal.SIGINT, self._on_sigint)
            except ValueError:  # pragma: no cover - embedded interpreters
                self._main = False
        self._thread = threading.Thread(target=self._run, name="detloop-watchdog", daemon=True)
        self._thread.start()
        return self

    def _run(self) -> None:
        assert self.timeout is not None
        if self._done.wait(self.timeout):
            return
        self.fired = True
        self.flag.set()
        if self._main:
            import _thread

            _thread.interrupt_main(signal.SIGINT)

    def _on_sigint(self, signum: int, frame: Any) -> None:
        if self.fired:
            raise WallClockTimeout(f"supervisory wall-clock timeout of {self.timeout}s")
        raise KeyboardInterrupt

    def __exit__(self, *exc: object) -> None:
        self._done.set()
        if self._thread is not None:
            self._thread.join()
        if self._main and self._old_handler is not None:
            signal.signal(signal.SIGINT, self._old_handler)


def run(
    fn: Callable[..., Any],
    *,
    seed: int,
    config: SimConfig | None = None,
    args: tuple[Any, ...] = (),
    kwargs: dict[str, Any] | None = None,
    faults: list[Fault] | None = None,
    tapes: dict[str, Tape] | None = None,
    decision_mode: str | None = None,
    keep_trace: bool = False,
    trace_level: str | None = None,
) -> RunResult:
    """Run ``await fn(world, *args, **kwargs)`` once in a fresh simulated world.

    ``faults`` replaces the generated fault schedule; ``tapes`` with
    ``decision_mode='strict'`` replays recorded decisions exactly and with
    ``'guided'`` uses them as shrinking hints.
    """
    if not inspect.iscoroutinefunction(fn):
        raise ConfigError(f"{getattr(fn, '__qualname__', fn)!r} must be an 'async def' function")
    if asyncio.events._get_running_loop() is not None:
        raise ConfigError("detloop.run() cannot be called from a running event loop")
    cfg = config or SimConfig()
    if trace_level is not None:
        cfg = cfg.with_overrides(trace=trace_level)
    mode = decision_mode or ("seeded" if tapes is None else "strict")
    decisions = Decisions(seed, mode=mode, tapes=tapes)
    trace = Trace(cfg.trace, cfg.budgets.max_trace_events)
    world = World(cfg, seed, decisions, trace)
    loop = world.loop
    schedule = (
        faults
        if faults is not None
        else generate(cfg.faults, cfg.nodes, decisions.generator("faults"))
    )
    trace.header(
        seed=seed,
        config=cfg.fingerprint(),
        scheduler=cfg.scheduler,
        nodes=list(cfg.nodes),
        faults=schedule_to_json(schedule),
    )
    kwargs = kwargs or {}
    outcome: dict[str, Any] = {}

    async def main() -> None:
        world.install_faults(schedule)
        await fn(world, *args, **kwargs)
        world.check_invariants()
        # give a pending invariant abort a chance to surface
        await asyncio.sleep(0)

    gc_was_enabled = gc.isenabled()
    started = _perf()
    tb_text: str | None = None
    try:
        gc.collect()
        gc.disable()
        with _Watchdog(cfg.budgets.wall_timeout, loop.wall_flag) as dog, guards.installed(world):
            runner = asyncio.Runner(loop_factory=lambda: loop)
            try:
                try:
                    runner.run(main(), context=world.control_context.copy())
                    outcome["status"] = (PASS, "pass", "pass")
                    outcome["message"] = "passed"
                except KeyboardInterrupt:
                    if dog.fired:
                        exc2: BaseException = WallClockTimeout(
                            f"supervisory wall-clock timeout of {dog.timeout}s"
                        )
                        outcome["status"] = classify(exc2)
                        outcome["message"] = str(exc2)
                    else:
                        raise
                except BaseException as exc:
                    if isinstance(exc, SystemExit | GeneratorExit):
                        raise
                    outcome["status"] = classify(exc)
                    outcome["message"] = sanitize_text(f"{type(exc).__qualname__}: {exc}")
                    tb_text = "".join(traceback.format_exception(exc))
                    if isinstance(exc, DeadlockError):
                        outcome["details"] = exc.pending
                # The failure message is not canonical: messages of user exceptions
                # change with tooling (e.g. pytest's assertion rewriting). It is
                # kept in the RunResult and manifest instead.
                trace.emit("end", 1, status=outcome["status"][0], signature=outcome["status"][2])
                trace.seal()
                outcome["trace_sha"] = trace.sha256()
                outcome["trace_count"] = trace.count
                _teardown(world, outcome)
            finally:
                try:
                    runner.close()
                except BaseException as exc:  # teardown never changes the outcome
                    if isinstance(exc, SystemExit):
                        raise
                    loop.warnings.append(
                        f"teardown: {type(exc).__qualname__}: {sanitize_text(str(exc))}"
                    )
                    if not loop.is_closed():
                        loop._running = False
                        loop.close()
                _silence_leftovers(world)
    finally:
        if gc_was_enabled:
            gc.enable()
    wall = _perf() - started
    status, kind, signature = outcome.get("status", (ERROR, "internal_error", "internal_error"))
    return RunResult(
        seed=seed,
        status=status,
        kind=kind,
        signature=signature,
        message=outcome.get("message", ""),
        steps=loop.steps,
        virtual_ns=loop.now_ns,
        trace_sha256=outcome.get("trace_sha", trace.sha256()),
        trace_events=outcome.get("trace_count", trace.count),
        trace_truncated=trace.truncated,
        faults=schedule_to_json(schedule),
        tapes=decisions.tapes_json(),
        decisions=decisions.total_decisions(),
        loop_errors=list(loop.loop_errors),
        warnings=list(loop.warnings),
        history=world.history.summary(),
        details=outcome.get("details", []),
        network=dict(world.network.stats),
        config_fingerprint=cfg.fingerprint(),
        decision_mode=mode,
        wall_seconds=wall,
        trace_lines=list(trace.lines) if keep_trace else None,
        traceback=tb_text,
    )


def _close_coro(task: asyncio.Task[Any]) -> None:
    coro = task.get_coro()
    try:
        if coro is not None and hasattr(coro, "close"):
            coro.close()
    except BaseException:  # noqa: S110 - effects of torn-down code are discarded
        pass


def _teardown(world: World, outcome: dict[str, Any]) -> None:
    """Deterministically dispose of everything left once the outcome is fixed.

    1. Tasks of crashed nodes stayed frozen; their coroutines are closed now.
    2. Every other pending task is cancelled in creation order and the loop
       runs (bounded by ``teardown_steps``) so cleanup code can finish.
    3. Anything still pending has its coroutine closed explicitly, so no
       ``finally`` block runs later from the garbage collector.
    4. Every simulated transport is closed, like sockets at process exit;
       transports the program left open are reported as a warning.
    Nothing here is traced or affects the result.
    """
    loop = world.loop
    loop.begin_teardown()
    for task in world._frozen:
        _close_coro(task)
    live = [t for _, t, _ in loop.pending_tasks()]
    for t in live:
        t.cancel()
    if live:
        try:
            loop.run_until_complete(asyncio.gather(*live, return_exceptions=True))
        except BaseException as exc:
            if isinstance(exc, SystemExit):
                raise
            loop.warnings.append(
                f"teardown incomplete: {type(exc).__qualname__}: {sanitize_text(str(exc))}"
            )
    for _, t, _ in loop.pending_tasks():
        _close_coro(t)
    open_transports = world.network.close_all()
    if open_transports:
        loop.warnings.append(
            f"{open_transports} simulated transport(s) were still open at the end of the run"
        )


def _silence_leftovers(world: World) -> None:
    from . import _compat

    for _, t, _ in world.loop.pending_tasks():
        _compat.silence_pending_destroy(t)
    for t in world._frozen:
        _compat.silence_pending_destroy(t)


def framework_identity() -> dict[str, str]:
    from ._compat import runtime_identity

    return {"detloop": __version__, **runtime_identity()}
