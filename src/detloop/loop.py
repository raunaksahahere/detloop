"""The deterministic simulation event loop.

:class:`SimLoop` implements :class:`asyncio.AbstractEventLoop` directly (see
ADR 0001). Key semantics (normative; see docs/DETERMINISM.md):

* **Virtual time** is an integer number of nanoseconds. ``loop.time()`` returns
  ``ns / 1e9``. Time advances only when no handle is runnable, jumping to the
  earliest pending timer.
* **Iterations** mirror ``BaseEventLoop._run_once``: an iteration first moves
  due timers to the ready queues, then runs the *batch* of handles that were
  ready at its start. Handles scheduled during a batch run in the next
  iteration, after the timers that become due. With the ``fifo`` scheduler the
  execution order is exactly asyncio's registration order.
* **Ownership-aware scheduling**: each owner (the control context or a node
  incarnation) has its own FIFO ready queue, so ``call_soon`` registration
  order is preserved per node. The ``interleave`` scheduler (default) picks
  which owner runs the next handle with a recorded decision; this explores
  the interleavings of independent nodes without breaking asyncio's ordering
  guarantee inside a node. The opt-in ``relaxed`` scheduler additionally picks
  any handle of the current batch inside the chosen queue, which *violates*
  asyncio's FIFO guarantee and may produce failures real asyncio cannot.
* **Crash isolation**: handles owned by a dead incarnation are never executed.
  Because every step of a task is a handle, a crashed node's coroutines can
  never run again, even if they would suppress ``CancelledError``.
"""

from __future__ import annotations

import asyncio
import collections
import heapq
import logging
import math
import os
import sys
import threading
import warnings
import weakref
from collections.abc import Callable, Coroutine
from contextvars import Context
from typing import TYPE_CHECKING, Any, TypeVar

from . import _compat
from ._owner import OWNER, Owner, owner_label
from .config import SimConfig
from .errors import (
    BudgetExceeded,
    DeadlockError,
    NondeterminismError,
    StarvationError,
    UnsupportedOperation,
    WallClockTimeout,
)
from .rng import Decisions
from .trace import Trace, sanitize_text

if TYPE_CHECKING:
    from .world import World

_T = TypeVar("_T")
logger = logging.getLogger("detloop.loop")

NS = 1_000_000_000
_NEVER = 1 << 62  # "infinitely far" virtual time
_UNSET = object()

#: Exception-handler messages produced from ``__del__`` (garbage collection).
#: They are kept out of the canonical trace.
_GC_MESSAGES = frozenset(
    {
        "Task was destroyed but it is pending!",
        "Task exception was never retrieved",
        "Future exception was never retrieved",
        "Unclosed connection",
    }
)


class SimHandle(asyncio.Handle):
    __slots__ = ("_owner", "_seq")


class SimTimerHandle(asyncio.TimerHandle):
    __slots__ = ("_owner", "_seq", "_when_ns")


def _run_until_complete_cb(fut: asyncio.Future[Any]) -> None:
    fut.get_loop().stop()


def to_ns(seconds: float) -> int:
    if seconds != seconds:
        raise ValueError("time must not be NaN")
    if seconds == math.inf:
        return _NEVER
    if seconds == -math.inf:
        return -_NEVER
    return round(seconds * NS)


class SimLoop(asyncio.AbstractEventLoop):
    """A deterministic, virtual-time asyncio event loop.

    Normally created by :func:`detloop.run`; it is usable standalone for
    unit tests::

        loop = SimLoop()
        with asyncio.Runner(loop_factory=lambda: loop) as runner:
            runner.run(main())
    """

    def __init__(
        self,
        *,
        config: SimConfig | None = None,
        decisions: Decisions | None = None,
        trace: Trace | None = None,
        world: World | None = None,
    ) -> None:
        self._config = config or SimConfig(nodes=())
        self._decisions = decisions or Decisions(0)
        self._trace = trace or Trace("off")
        self._trace.bind_clock(lambda: self._now)
        self._world = world
        self._mode = self._config.scheduler
        b = self._config.budgets
        self._max_steps = b.max_steps
        self._max_idle = b.max_steps_without_progress
        self._max_ns = to_ns(b.max_virtual_time)
        self._teardown_steps = b.teardown_steps

        self._now = 0
        self._seq = 0
        self._batch_limit = 0
        self._fifo: collections.deque[SimHandle] = collections.deque()
        self._queues: dict[Owner | None, collections.deque[SimHandle]] = {}
        self._due: dict[Owner | None, collections.deque[SimTimerHandle]] = {}
        self._limits: dict[Owner | None, int] = {}
        self._timers: list[tuple[int, int, SimTimerHandle]] = []
        self._timer_cancelled = 0

        self._closed = False
        self._running = False
        self._stopping = False
        self._thread_id: int | None = None
        self._debug = False
        self.slow_callback_duration = 0.1
        self._exception_handler: Any = None
        self._task_factory: Any = None
        self._default_executor: Any = None
        self._asyncgens: dict[int, Any] = {}
        self._asyncgen_keys: weakref.WeakKeyDictionary[Any, int] = weakref.WeakKeyDictionary()
        self._agen_counter = 0
        self._asyncgens_shutdown_called = False

        self.steps = 0
        self._steps_at_progress = 0
        self.dropped_handles = 0
        self._abort: BaseException | None = None
        self._teardown = False
        self._teardown_budget_end = 0
        self._tasks: dict[int, tuple[asyncio.Task[Any], Owner | None]] = {}
        self._prune_at = 64
        self._task_ids: weakref.WeakKeyDictionary[asyncio.Task[Any], int] = weakref.WeakKeyDictionary()
        self._next_task_id = 1
        self.loop_errors: list[dict[str, Any]] = []
        self.warnings: list[str] = []
        self._xthread_lock = threading.Lock()
        self._xthread_calls: list[str] = []
        self.wall_flag = threading.Event()

    # ------------------------------------------------------------------
    # Clock
    def time(self) -> float:
        return self._now / NS

    @property
    def now_ns(self) -> int:
        return self._now

    # ------------------------------------------------------------------
    # Ownership
    def _owner_for(self, context: Context | None) -> Owner | None:
        if context is None:
            return OWNER.get()  # type: ignore[no-any-return]
        owner = context.get(OWNER, _UNSET)
        if owner is _UNSET:
            # A context that never saw an owner (e.g. ``contextvars.Context()``
            # created by service code). Control contexts carry an explicit None.
            current = OWNER.get()
            owner = current
            if current is not None:
                # A context created from scratch inside a node: adopt it so
                # that work scheduled from it cannot outlive a crash.
                try:
                    context.run(OWNER.set, current)
                except RuntimeError:
                    pass
        return owner  # type: ignore[no-any-return]

    # ------------------------------------------------------------------
    # Scheduling primitives
    def _check_closed(self) -> None:
        if self._closed:
            raise RuntimeError("Event loop is closed")

    def call_soon(
        self, callback: Callable[..., object], *args: Any, context: Context | None = None
    ) -> SimHandle:
        self._check_closed()
        if self._debug:
            self._check_callback(callback, "call_soon")
        owner = self._owner_for(context)
        h = SimHandle(callback, args, self, context)
        h._owner = owner
        h._seq = self._seq
        self._seq += 1
        self._enqueue(h)
        return h

    def _check_callback(self, callback: Any, method: str) -> None:
        if asyncio.iscoroutine(callback) or asyncio.iscoroutinefunction(callback):
            raise TypeError(f"coroutines cannot be used with {method}()")
        if not callable(callback):
            raise TypeError(f"a callable object was expected by {method}(), got {callback!r}")

    def _enqueue(self, h: SimHandle) -> None:
        if self._mode == "fifo":
            self._fifo.append(h)
            return
        q = self._queues.get(h._owner)
        if q is None:
            q = self._queues[h._owner] = collections.deque()
        q.append(h)

    def call_soon_threadsafe(
        self, callback: Callable[..., object], *args: Any, context: Context | None = None
    ) -> SimHandle:
        if self._thread_id is not None and threading.get_ident() != self._thread_id:
            msg = f"call_soon_threadsafe({getattr(callback, '__qualname__', callback)!r}) from another thread"
            with self._xthread_lock:
                self._xthread_calls.append(msg)
            raise NondeterminismError(
                f"{msg}: threads are outside the simulation's determinism boundary"
            )
        return self.call_soon(callback, *args, context=context)

    def call_later(
        self,
        delay: float,
        callback: Callable[..., object],
        *args: Any,
        context: Context | None = None,
    ) -> SimTimerHandle:
        if delay is None:
            raise TypeError("delay must not be None")
        return self._call_at_ns(self._now + to_ns(delay), callback, args, context)

    def call_at(
        self,
        when: float,
        callback: Callable[..., object],
        *args: Any,
        context: Context | None = None,
    ) -> SimTimerHandle:
        if when is None:
            raise TypeError("when cannot be None")
        return self._call_at_ns(to_ns(when), callback, args, context)

    def _call_at_ns(
        self,
        when_ns: int,
        callback: Callable[..., object],
        args: tuple[Any, ...],
        context: Context | None,
    ) -> SimTimerHandle:
        self._check_closed()
        if self._debug:
            self._check_callback(callback, "call_at")
        owner = self._owner_for(context)
        h = SimTimerHandle(when_ns / NS, callback, args, self, context)
        h._owner = owner
        h._seq = self._seq
        h._when_ns = when_ns
        h._scheduled = True
        self._seq += 1
        heapq.heappush(self._timers, (when_ns, h._seq, h))
        return h

    def _timer_handle_cancelled(self, handle: asyncio.TimerHandle) -> None:
        if getattr(handle, "_scheduled", False):
            self._timer_cancelled += 1
            if self._timer_cancelled > 256 and self._timer_cancelled * 2 > len(self._timers):
                live = [e for e in self._timers if not e[2]._cancelled]
                heapq.heapify(live)
                self._timers = live
                self._timer_cancelled = 0

    # ------------------------------------------------------------------
    # The scheduler
    #
    # ``fifo``: one global iteration structure, exactly like
    # ``BaseEventLoop._run_once``: a batch is every handle ready when the
    # iteration began; timers that became due join after them.
    #
    # ``interleave``/``relaxed``: every owner (control or node incarnation)
    # behaves like its own event loop with its own iterations and batch
    # boundary, so a node can run several iterations while another is
    # mid-batch, as independent processes can. Within one owner the order is
    # asyncio's. The decision is which owner runs the next handle.
    def _has_ready(self) -> bool:
        if self._mode == "fifo":
            return bool(self._fifo)
        return any(self._queues.values()) or any(self._due.values())

    def _batch_pending(self) -> bool:
        if self._mode == "fifo":
            return bool(self._fifo) and self._fifo[0]._seq < self._batch_limit
        limits = self._limits
        return any(q and q[0]._seq < limits.get(k, -1) for k, q in self._queues.items())

    def _advance_time(self) -> bool:
        """Jump virtual time to the earliest live timer; False if there is none."""
        timers = self._timers
        while timers and timers[0][2]._cancelled:
            heapq.heappop(timers)
            self._timer_cancelled -= 1
        if not timers:
            return False
        when = timers[0][0]
        if when > self._now:
            if when > self._max_ns and not self._teardown:
                raise BudgetExceeded(
                    "virtual_time",
                    f"virtual time budget of {self._max_ns / NS:g}s exhausted "
                    f"(next timer at {when / NS:g}s)",
                )
            self._now = when
            self._steps_at_progress = self.steps
        return True

    def _pop_due(self) -> list[SimTimerHandle]:
        timers = self._timers
        now = self._now
        out: list[SimTimerHandle] = []
        while timers and timers[0][0] <= now:
            _, _, h = heapq.heappop(timers)
            if h._cancelled:
                self._timer_cancelled -= 1
                continue
            h._scheduled = False
            out.append(h)
        return out

    def _pick(self, stopping: bool = False) -> SimHandle | None:
        if self._mode == "fifo":
            return self._pick_fifo(stopping)
        return self._pick_owner(stopping)

    def _pick_fifo(self, stopping: bool) -> SimHandle | None:
        q = self._fifo
        while True:
            if q and q[0]._seq < self._batch_limit:
                return q.popleft()
            if stopping:
                return None
            if not q and not self._advance_time():
                return None
            for h in self._pop_due():
                h._seq = self._seq
                self._seq += 1
                q.append(h)
            self._batch_limit = self._seq

    def _pick_owner(self, stopping: bool) -> SimHandle | None:
        queues, due, limits = self._queues, self._due, self._limits
        while True:
            for h in self._pop_due():
                d = due.get(h._owner)
                if d is None:
                    d = due[h._owner] = collections.deque()
                d.append(h)
            cands: list[tuple[int, Owner | None]] = []
            for k, q in queues.items():
                if q and (not stopping or q[0]._seq < limits.get(k, -1)):
                    cands.append((q[0]._seq, k))
            if not stopping:
                for k, d in due.items():
                    if d and not queues.get(k):
                        cands.append((d[0]._seq, k))
            if cands:
                break
            if stopping or not self._advance_time():
                return None
        if len(cands) == 1:
            key = cands[0][1]
        else:
            cands.sort(key=lambda c: c[0])
            idx = self._decisions.choose("sched", len(cands), "owner")
            key = cands[idx][1]
            if self._trace.level >= 2:
                self._trace.emit(
                    "choose", 2, n=len(cands), pick=owner_label(key),
                    of=[owner_label(c[1]) for c in cands],
                )
        q = queues.get(key)
        limit = limits.get(key, -1)
        if q is None or not q or q[0]._seq >= limit:
            # This owner starts a new iteration: its due timers join after the
            # handles already queued, and everything now queued is its batch.
            if q is None:
                q = queues[key] = collections.deque()
            d = due.pop(key, None)
            if d:
                for h in d:
                    h._seq = self._seq
                    self._seq += 1
                    q.append(h)
            limit = limits[key] = self._seq
        if self._mode == "relaxed":
            eligible = 0
            for h in q:
                if h._seq >= limit:
                    break
                eligible += 1
            if eligible > 1:
                i = self._decisions.choose("sched", eligible, "handle")
                h = q[i]
                del q[i]
                if not q:
                    del queues[key]
                return h
        h = q.popleft()
        if not q:
            del queues[key]
        return h

    def _prime_iteration(self) -> None:
        """``stop()`` before ``run_forever()``: run one iteration without advancing time."""
        if self._mode == "fifo":
            for h in self._pop_due():
                h._seq = self._seq
                self._seq += 1
                self._fifo.append(h)
            self._batch_limit = self._seq
            return
        for h in self._pop_due():
            self._due.setdefault(h._owner, collections.deque()).append(h)
        for key in list(self._due):
            q = self._queues.setdefault(key, collections.deque())
            for h in self._due.pop(key):
                h._seq = self._seq
                self._seq += 1
                q.append(h)
        for key in self._queues:
            self._limits[key] = self._seq

    def _step(self) -> bool:
        h = self._pick(self._stopping)
        if h is None:
            return False
        self._execute(h)
        return True

    def _execute(self, h: SimHandle) -> None:
        if h._cancelled:
            return
        owner = h._owner
        if owner is not None and not owner.alive and not self._teardown:
            self.dropped_handles += 1
            return
        self.steps += 1
        if self._teardown:
            if self.steps > self._teardown_budget_end:
                raise BudgetExceeded("teardown_steps", "teardown step budget exhausted")
        else:
            if self.steps > self._max_steps:
                raise BudgetExceeded(
                    "steps", f"step budget of {self._max_steps} handles exhausted"
                )
            if self.steps - self._steps_at_progress > self._max_idle:
                raise StarvationError(
                    f"{self._max_idle} steps ran without virtual time advancing at "
                    f"t={self._now / NS:g}s (busy loop or endlessly rescheduling callback)"
                )
        if not self.steps & 4095:
            self._periodic_checks()
        task = None
        if self._trace.level >= 2:
            task = _compat.task_of_callback(h._callback)
        _compat.run_handle(h)
        if task is not None and task.done():
            self._trace_task_done(task)
        if self._abort is not None and not self._teardown:
            exc, self._abort = self._abort, None
            raise exc

    def _periodic_checks(self) -> None:
        if self.wall_flag.is_set():
            raise WallClockTimeout("supervisory wall-clock timeout")
        if self._xthread_calls and not self._teardown:
            raise NondeterminismError(self._xthread_calls[0])
        if len(self._tasks) > self._prune_at:
            self._prune_tasks()

    def abort(self, exc: BaseException) -> None:
        """Fail the run with ``exc`` as soon as the current handle returns."""
        if self._abort is None:
            self._abort = exc

    # ------------------------------------------------------------------
    # Running
    def run_forever(self) -> None:
        self._check_closed()
        if self._running:
            raise RuntimeError("This event loop is already running")
        if _compat.get_running_loop_or_none() is not None:
            raise RuntimeError("Cannot run the event loop while another loop is running")
        self._thread_id = threading.get_ident()
        old_hooks = sys.get_asyncgen_hooks()
        sys.set_asyncgen_hooks(
            firstiter=self._asyncgen_firstiter_hook, finalizer=self._asyncgen_finalizer_hook
        )
        self._running = True
        _compat.set_running_loop(self)
        try:
            if self._stopping:
                self._prime_iteration()
            while True:
                if self._stopping and not self._batch_pending():
                    break
                if not self._step():
                    if self._stopping:
                        break
                    raise self._deadlock_error()
        finally:
            self._stopping = False
            self._running = False
            self._thread_id = None
            _compat.set_running_loop(None)
            sys.set_asyncgen_hooks(*old_hooks)

    def run_until_complete(self, future: Any) -> Any:
        self._check_closed()
        if self._running:
            raise RuntimeError("This event loop is already running")
        new_task = not asyncio.isfuture(future)
        future = asyncio.ensure_future(future, loop=self)
        if new_task:
            _compat.silence_pending_destroy(future)
        future.add_done_callback(_run_until_complete_cb)
        try:
            self.run_forever()
        except BaseException:
            if new_task and future.done() and not future.cancelled():
                future.exception()
            raise
        finally:
            future.remove_done_callback(_run_until_complete_cb)
        if not future.done():
            raise RuntimeError("Event loop stopped before Future completed.")
        return future.result()

    def stop(self) -> None:
        self._stopping = True

    def is_running(self) -> bool:
        return self._running

    def is_closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if self._running:
            raise RuntimeError("Cannot close a running event loop")
        if self._closed:
            return
        self._closed = True
        self._fifo.clear()
        self._queues.clear()
        self._due.clear()
        self._timers.clear()
        self._default_executor = None

    async def shutdown_asyncgens(self) -> None:
        self._asyncgens_shutdown_called = True
        if not self._asyncgens:
            return
        closing = [self._asyncgens[k] for k in sorted(self._asyncgens)]
        self._asyncgens.clear()
        results = await asyncio.gather(*[ag.aclose() for ag in closing], return_exceptions=True)
        for result, agen in zip(results, closing, strict=True):
            if isinstance(result, Exception):
                self.call_exception_handler(
                    {
                        "message": f"an error occurred during closing of asynchronous generator {agen!r}",
                        "exception": result,
                        "asyncgen": agen,
                    }
                )

    async def shutdown_default_executor(self, timeout: float | None = None) -> None:
        """No executor threads exist inside a simulation; nothing to shut down."""
        self._default_executor = None

    def _asyncgen_firstiter_hook(self, agen: Any) -> None:
        if self._asyncgens_shutdown_called:
            warnings.warn(
                f"asynchronous generator {agen!r} was scheduled after loop.shutdown_asyncgens() call",
                ResourceWarning,
                source=self,
                stacklevel=2,
            )
        self._agen_counter += 1
        self._asyncgens[self._agen_counter] = agen
        self._asyncgen_keys[agen] = self._agen_counter

    def _asyncgen_finalizer_hook(self, agen: Any) -> None:
        key = self._asyncgen_keys.pop(agen, None)
        if key is not None:
            self._asyncgens.pop(key, None)
        if not self._closed:
            self.call_soon_threadsafe(self.create_task, agen.aclose())

    # ------------------------------------------------------------------
    # Futures and tasks
    def create_future(self) -> asyncio.Future[Any]:
        return asyncio.Future(loop=self)

    def create_task(  # type: ignore[override]
        self,
        coro: Coroutine[Any, Any, _T],
        *,
        name: str | None = None,
        context: Context | None = None,
        **kwargs: Any,
    ) -> asyncio.Task[_T]:
        self._check_closed()
        tid = self._next_task_id
        self._next_task_id += 1
        if name is None:
            # asyncio's default names use a process-global counter; use a
            # per-run counter so names are deterministic.
            name = f"Task-{tid}"
        owner = self._owner_for(context)
        if self._task_factory is not None:
            if context is not None:
                kwargs["context"] = context
            task = self._task_factory(self, coro, **kwargs)
            task.set_name(name)
        else:
            task = asyncio.Task(coro, loop=self, name=name, context=context, **kwargs)
        self._tasks[tid] = (task, owner)
        if self._trace.level >= 2:
            self._task_ids[task] = tid
            self._trace.emit(
                "task", 2, task=tid, owner=owner_label(owner),
                coro=getattr(coro, "__qualname__", type(coro).__name__),
            )
        return task

    def _trace_task_done(self, task: asyncio.Task[Any]) -> None:
        tid = self._task_ids.pop(task, None)
        if tid is None:
            return
        if task.cancelled():
            result = "cancelled"
        elif task.exception() is not None:
            result = type(task.exception()).__qualname__
        else:
            result = "ok"
        self._trace.emit("task_done", 2, task=tid, result=result)

    def _prune_tasks(self) -> None:
        self._tasks = {k: v for k, v in self._tasks.items() if not v[0].done()}
        self._prune_at = max(64, 2 * len(self._tasks))

    def tasks_of(self, owner: Owner | None) -> list[asyncio.Task[Any]]:
        """Pending tasks owned by ``owner`` in creation order."""
        return [t for t, o in self._tasks.values() if o is owner and not t.done()]

    def pending_tasks(self) -> list[tuple[int, asyncio.Task[Any], Owner | None]]:
        return [(tid, t, o) for tid, (t, o) in self._tasks.items() if not t.done()]

    def drop_owner(self, owner: Owner) -> int:
        """Discard queued handles of a dead owner (timers are dropped lazily)."""
        q = self._queues.pop(owner, None)
        d = self._due.pop(owner, None)
        self._limits.pop(owner, None)
        n = (len(q) if q else 0) + (len(d) if d else 0)
        self.dropped_handles += n
        return n

    def set_task_factory(self, factory: Any) -> None:
        if factory is not None and not callable(factory):
            raise TypeError("task factory must be a callable or None")
        self._task_factory = factory

    def get_task_factory(self) -> Any:
        return self._task_factory

    # ------------------------------------------------------------------
    # Deadlock reporting
    def describe_task(self, tid: int, task: asyncio.Task[Any], owner: Owner | None) -> str:
        """``[owner] name outer -> ... -> innermost at file:line`` (follows ``cr_await``)."""
        chain: list[str] = []
        where = ""
        obj: Any = task.get_coro()
        for _ in range(32):
            if obj is None:
                break
            name = getattr(obj, "__qualname__", None)
            frame = getattr(obj, "cr_frame", None) or getattr(obj, "gi_frame", None) or getattr(obj, "ag_frame", None)
            if name is None or frame is None:
                break
            chain.append(name)
            where = f" at {os.path.basename(frame.f_code.co_filename)}:{frame.f_lineno}"
            obj = getattr(obj, "cr_await", None) or getattr(obj, "gi_yieldfrom", None)
        if len(chain) > 3:
            chain = [chain[0], "...", *chain[-2:]]
        return f"[{owner_label(owner)}] {task.get_name()} {' -> '.join(chain) or '?'}{where}"

    def _deadlock_error(self) -> DeadlockError:
        pending = [
            self.describe_task(tid, t, o)
            for tid, t, o in self.pending_tasks()
            if o is None or o.alive
        ]
        msg = (
            f"deadlock at t={self._now / NS:g}s: no runnable callbacks and no pending timers, "
            f"but the run is not finished; {len(pending)} live task(s) are blocked"
        )
        if pending:
            msg += ":\n  " + "\n  ".join(pending[:20])
        return DeadlockError(msg, pending)

    # ------------------------------------------------------------------
    # Teardown support
    def begin_teardown(self) -> None:
        """Switch to teardown mode: outcome is decided, effects are discarded."""
        self._teardown = True
        self._abort = None
        self._teardown_budget_end = self.steps + self._teardown_steps

    # ------------------------------------------------------------------
    # Error handling
    def get_exception_handler(self) -> Any:
        return self._exception_handler

    def set_exception_handler(self, handler: Any) -> None:
        if handler is not None and not callable(handler):
            raise TypeError(f"A callable object or None is expected, got {handler!r}")
        self._exception_handler = handler

    def default_exception_handler(self, context: dict[str, Any]) -> None:
        message = context.get("message") or "Unhandled exception in event loop"
        exc = context.get("exception")
        logger.debug("%s", message, exc_info=exc if isinstance(exc, BaseException) else None)

    def call_exception_handler(self, context: dict[str, Any]) -> None:
        message = str(context.get("message") or "Unhandled exception in event loop")
        exc = context.get("exception")
        record = {
            "message": sanitize_text(message),
            "exception": type(exc).__qualname__ if isinstance(exc, BaseException) else None,
            "detail": sanitize_text(str(exc)) if isinstance(exc, BaseException) else None,
            "owner": owner_label(OWNER.get()),
        }
        if message in _GC_MESSAGES or self._teardown:
            self.warnings.append(f"{record['message']} ({record['exception']})")
        else:
            self.loop_errors.append(record)
            self._trace.emit("loop_error", 1, **record)
            if self._config.fail_on_loop_errors:
                from .errors import SimulationError

                self.abort(SimulationError(f"loop exception handler called: {message}: {record['detail']}"))
        try:
            if self._exception_handler is None:
                self.default_exception_handler(context)
            else:
                self._exception_handler(self, context)
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException:
            logger.debug("exception in custom exception handler", exc_info=True)

    def get_debug(self) -> bool:
        return self._debug

    def set_debug(self, enabled: bool) -> None:
        self._debug = enabled

    # ------------------------------------------------------------------
    # Executors (outside the determinism boundary)
    def run_in_executor(self, executor: Any, func: Callable[..., _T], *args: Any) -> asyncio.Future[_T]:
        self._check_closed()
        if self._config.executor != "inline":
            raise NondeterminismError(
                f"run_in_executor({getattr(func, '__qualname__', func)!r}) would run code on a real "
                "thread, which is outside the determinism boundary. Make the call synchronous, "
                "or opt in with executor='inline' to run it on the simulation thread."
            )
        fut: asyncio.Future[_T] = self.create_future()
        try:
            fut.set_result(func(*args))
        except BaseException as exc:
            if isinstance(exc, KeyboardInterrupt | SystemExit):
                raise
            fut.set_exception(exc)
        return fut

    def set_default_executor(self, executor: Any) -> None:
        self._default_executor = executor

    # ------------------------------------------------------------------
    # Network (delegated to the simulated network)
    def _net(self) -> Any:
        if self._world is None:
            raise UnsupportedOperation("this SimLoop has no simulated world/network attached")
        return self._world.network

    async def getaddrinfo(
        self, host: Any, port: Any, *, family: int = 0, type: int = 0, proto: int = 0, flags: int = 0
    ) -> list[Any]:
        return self._net().getaddrinfo(host, port, family=family, type=type, proto=proto, flags=flags)  # type: ignore[no-any-return]

    async def getnameinfo(self, sockaddr: Any, flags: int = 0) -> tuple[str, str]:
        return self._net().getnameinfo(sockaddr, flags)  # type: ignore[no-any-return]

    async def create_connection(self, protocol_factory: Any, host: Any = None, port: Any = None, **kwargs: Any) -> Any:  # type: ignore[override]
        return await self._net().create_connection(protocol_factory, host, port, **kwargs)

    async def create_server(self, protocol_factory: Any, host: Any = None, port: Any = None, **kwargs: Any) -> Any:  # type: ignore[override]
        return await self._net().create_server(protocol_factory, host, port, **kwargs)

    async def sock_connect(self, sock: Any, address: Any) -> None:
        await self._net().sock_connect(sock, address)

    # ------------------------------------------------------------------
    # Explicitly unsupported APIs: fail loudly, never fall back to reality.
    def _unsupported(self, what: str, hint: str = "") -> UnsupportedOperation:
        return UnsupportedOperation(
            f"{what} is not supported inside a detloop simulation" + (f"; {hint}" if hint else "")
        )

    async def create_unix_connection(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise self._unsupported("UNIX domain sockets", "use TCP between simulated nodes")

    async def create_unix_server(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise self._unsupported("UNIX domain sockets", "use TCP between simulated nodes")

    async def create_datagram_endpoint(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise self._unsupported(
            "create_datagram_endpoint", "use World.messages (node.messages) for datagram-style messaging"
        )

    async def connect_accepted_socket(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise self._unsupported("connect_accepted_socket")

    async def start_tls(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise self._unsupported("TLS", "simulate plaintext connections")

    async def sendfile(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise self._unsupported("loop.sendfile")

    async def subprocess_exec(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise NondeterminismError("subprocesses are outside the determinism boundary")

    async def subprocess_shell(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise NondeterminismError("subprocesses are outside the determinism boundary")

    async def connect_read_pipe(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise self._unsupported("pipes")

    async def connect_write_pipe(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise self._unsupported("pipes")

    def add_reader(self, *a: Any, **k: Any) -> None:  # type: ignore[override]
        raise self._unsupported("add_reader (file-descriptor I/O)")

    def remove_reader(self, *a: Any, **k: Any) -> bool:  # type: ignore[override]
        return False

    def add_writer(self, *a: Any, **k: Any) -> None:  # type: ignore[override]
        raise self._unsupported("add_writer (file-descriptor I/O)")

    def remove_writer(self, *a: Any, **k: Any) -> bool:  # type: ignore[override]
        return False

    async def sock_recv(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise self._unsupported("loop.sock_recv", "use transports/streams")

    async def sock_recv_into(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise self._unsupported("loop.sock_recv_into", "use transports/streams")

    async def sock_recvfrom(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise self._unsupported("loop.sock_recvfrom")

    async def sock_recvfrom_into(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise self._unsupported("loop.sock_recvfrom_into")

    async def sock_sendall(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise self._unsupported("loop.sock_sendall", "use transports/streams")

    async def sock_sendto(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise self._unsupported("loop.sock_sendto")

    async def sock_accept(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise self._unsupported("loop.sock_accept", "use create_server/start_server")

    async def sock_sendfile(self, *a: Any, **k: Any) -> Any:  # type: ignore[override]
        raise self._unsupported("loop.sock_sendfile")

    def add_signal_handler(self, *a: Any, **k: Any) -> None:  # type: ignore[override]
        raise self._unsupported("signal handlers")

    def remove_signal_handler(self, *a: Any, **k: Any) -> bool:  # type: ignore[override]
        return False

    def __repr__(self) -> str:
        return (
            f"<SimLoop t={self._now / NS:g}s running={self._running} "
            f"closed={self._closed} scheduler={self._mode}>"
        )
