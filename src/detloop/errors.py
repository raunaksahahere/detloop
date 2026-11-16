"""Exception hierarchy.

Errors are grouped by *who* is at fault so that run outcomes can be classified
precisely (see :class:`detloop.Outcome`):

* :class:`SimulationError` subclasses describe something the simulated program
  did (deadlocked, used a blocked API, exceeded a budget, violated an invariant).
* :class:`ReplayError` subclasses describe a replay that cannot be trusted.
* :class:`InternalError` is a detloop bug.
"""

from __future__ import annotations

from typing import Any


class DetloopError(Exception):
    """Base class for all detloop errors."""


class ConfigError(DetloopError, ValueError):
    """Invalid configuration or API usage detected before a run starts."""


class SimulationError(DetloopError):
    """Base class for failures caused by the simulated program."""


class NondeterminismError(SimulationError):
    """The simulated code used an API outside the determinism boundary.

    Examples: real sockets, threads, subprocesses, ``time.sleep``, or
    uncontrolled entropy in strict mode. The violation is *sticky*: even if the
    code catches this exception the run is still failed.
    """


class UnsupportedOperation(SimulationError):
    """The simulated code used an asyncio feature detloop does not simulate.

    Examples: TLS, UNIX sockets, datagram endpoints, signal handlers. detloop
    never silently falls back to the real implementation.
    """


class DeadlockError(SimulationError):
    """Nothing is runnable, no timer is pending, and the main test has not finished."""

    def __init__(self, message: str, pending: list[str] | None = None) -> None:
        super().__init__(message)
        self.pending = pending or []


class StarvationError(SimulationError):
    """Work stayed runnable for too many steps without virtual time advancing.

    Typical cause: a ``while True: await asyncio.sleep(0)`` busy loop or a
    callback that reschedules itself forever.
    """


class BudgetExceeded(SimulationError):
    """A resource budget (steps, virtual time, buffered bytes, ...) was exhausted.

    Budget exhaustion is never treated as a pass: the run is reported as
    *incomplete*.
    """

    def __init__(self, budget: str, message: str) -> None:
        super().__init__(message)
        self.budget = budget


class InvariantViolation(SimulationError, AssertionError):
    """A user invariant registered through :meth:`World.check` failed."""

    def __init__(self, name: str, message: str = "") -> None:
        super().__init__(f"invariant {name!r} violated" + (f": {message}" if message else ""))
        self.name = name
        self.detail = message


class NodeFailure(SimulationError):
    """A node's main coroutine raised an unhandled exception."""

    def __init__(self, node: str, exc: BaseException) -> None:
        super().__init__(f"node {node} crashed with {type(exc).__qualname__}: {exc}")
        self.node = node
        self.exc = exc


class LinearizabilityViolation(SimulationError, AssertionError):
    """The recorded history is not linearizable with respect to the model."""

    def __init__(self, message: str, result: Any = None) -> None:
        super().__init__(message)
        self.result = result


class CheckIncomplete(SimulationError):
    """A checker could not decide (budget exhausted or unsupported history)."""

    def __init__(self, message: str, result: Any = None) -> None:
        super().__init__(message)
        self.result = result


class WallClockTimeout(SimulationError):
    """The supervisory wall-clock timeout fired (outside simulated time)."""


class ReplayError(DetloopError):
    """Base class for replay problems."""


class ReplayDivergence(ReplayError):
    """Event-decision replay diverged from the recorded decision tape."""

    def __init__(self, stream: str, index: int, message: str) -> None:
        super().__init__(f"replay diverged on stream {stream!r} at decision {index}: {message}")
        self.stream = stream
        self.index = index


class ReplayMismatch(ReplayError):
    """The replay environment (code, config, runtime) is incompatible with the manifest."""


class TargetError(DetloopError):
    """A simulation target (file, module, or test name) could not be loaded."""


class InternalError(DetloopError):
    """A detloop bug. Please report it with the manifest and trace."""


class SimulationFailure(AssertionError):
    """Raised by the pytest integration when a seed fails.

    The message contains the seed, outcome, artifact paths, and the exact
    replay command.
    """
