"""Compatibility layer for CPython asyncio internals.

detloop implements :class:`asyncio.AbstractEventLoop` itself instead of
subclassing ``BaseEventLoop``. The few private CPython details it still relies
on are isolated here, each with the versions it was verified against
(3.11-3.14) and a test in ``tests/test_compat.py``:

``asyncio.events._set_running_loop`` / ``_get_running_loop``
    Exported in ``asyncio.events.__all__`` for loop implementers.
``Handle._run()``
    The stdlib method that runs a callback in its context and routes
    exceptions to ``loop.call_exception_handler``. Present unchanged in
    3.11-3.14.
``loop._timer_handle_cancelled(handle)``
    Hook that ``asyncio.TimerHandle.cancel()`` calls on its loop.
``TimerHandle._scheduled``
    Flag maintained by loops; only used in ``TimerHandle.__repr__``.
``Task._log_destroy_pending``
    Writable on both the C and Python ``Task``; cleared for tasks of crashed
    nodes, which are frozen by design and must not be reported as leaks.
Task step callbacks expose ``__self__``
    Python tasks schedule bound methods; the C ``TaskStepMethWrapper`` exposes
    ``__self__``. Only used for full-level trace events (task completion).
"""

from __future__ import annotations

import asyncio
import sys
from asyncio import events
from typing import Any

PY_VERSION = sys.version_info[:2]
SUPPORTED_VERSIONS = ((3, 11), (3, 12), (3, 13), (3, 14))
#: ``loop.create_task(**kwargs)`` forwarding exists from 3.13.
CREATE_TASK_KWARGS = PY_VERSION >= (3, 13)
#: Eager task factories exist from 3.12.
HAS_EAGER_TASKS = PY_VERSION >= (3, 12)

set_running_loop = events._set_running_loop
get_running_loop_or_none = events._get_running_loop


def run_handle(handle: asyncio.Handle) -> None:
    handle._run()


def task_of_callback(callback: Any) -> asyncio.Task[Any] | None:
    owner = getattr(callback, "__self__", None)
    if isinstance(owner, asyncio.Task):
        return owner
    return None


def silence_pending_destroy(task: asyncio.Future[Any]) -> None:
    try:
        task._log_destroy_pending = False  # type: ignore[attr-defined]
    except AttributeError:  # pragma: no cover - defensive, verified in tests
        pass


def is_supported_runtime() -> bool:
    return sys.implementation.name == "cpython" and PY_VERSION in SUPPORTED_VERSIONS


def runtime_identity() -> dict[str, str]:
    import platform

    return {
        "implementation": sys.implementation.name,
        "python": platform.python_version(),
        "platform": sys.platform,
        "machine": platform.machine(),
    }
