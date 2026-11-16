"""Operation histories (Jepsen-style invoke/complete records).

Statuses:

``ok``
    The operation completed and returned ``output``.
``fail``
    The operation *definitely* did not take effect (for example the server
    rejected it before doing anything). Failed operations are excluded from
    linearizability checking.
``info``
    Indeterminate: the operation may or may not have taken effect (timeout,
    connection lost after sending). Checkers treat it as possibly taking
    effect at any point after its invocation, or never.
``pending``
    Never completed by the end of the run; treated like ``info``.

Real-time order uses a strictly increasing *logical* event index, so two
events at the same virtual timestamp are still ordered by the order in which
the simulation executed them.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .trace import jsonable

if TYPE_CHECKING:
    from .world import World


class _Unknown:
    _inst: _Unknown | None = None

    def __new__(cls) -> _Unknown:
        if cls._inst is None:
            cls._inst = super().__new__(cls)
        return cls._inst

    def __repr__(self) -> str:
        return "UNKNOWN"


#: Output of an operation whose completion was not observed.
UNKNOWN: Any = _Unknown()


@dataclass
class Operation:
    id: int
    client: str
    f: str
    key: Any = None
    value: Any = None
    invoke_index: int = 0
    invoke_time: int = 0
    status: str = "pending"
    output: Any = UNKNOWN
    complete_index: int | None = None
    complete_time: int | None = None
    error: str | None = None
    _history: History | None = field(default=None, repr=False, compare=False)

    # completion helpers --------------------------------------------------
    def ok(self, output: Any = None) -> Operation:
        assert self._history is not None
        self._history._complete(self, "ok", output, None)
        return self

    def fail(self, error: Any = None) -> Operation:
        """The operation definitely did not take effect."""
        assert self._history is not None
        self._history._complete(self, "fail", UNKNOWN, error)
        return self

    def info(self, error: Any = None) -> Operation:
        """Outcome unknown: the operation may or may not have taken effect."""
        assert self._history is not None
        self._history._complete(self, "info", UNKNOWN, error)
        return self

    @property
    def done(self) -> bool:
        return self.status != "pending"

    @property
    def completed_ok(self) -> bool:
        return self.status == "ok"

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "client": self.client,
            "f": self.f,
            "key": jsonable(self.key),
            "value": jsonable(self.value),
            "status": self.status,
            "output": None if self.output is UNKNOWN else jsonable(self.output),
            "invoke": [self.invoke_index, self.invoke_time],
            "complete": None
            if self.complete_index is None
            else [self.complete_index, self.complete_time],
            "error": self.error,
        }

    def __str__(self) -> str:
        out = "?" if self.output is UNKNOWN else repr(self.output)
        key = "" if self.key is None else f"[{self.key!r}]"
        val = "" if self.value is None else f"({self.value!r})"
        return f"#{self.id} {self.client} {self.f}{key}{val} -> {self.status} {out}"


class History:
    """Records operations for checking. Available as :attr:`World.history`.

    It can also be used standalone (``History()``) to build histories by hand,
    in which case logical indices still order events and times default to 0.
    """

    def __init__(self, world: World | None = None) -> None:
        self._world = world
        self.ops: list[Operation] = []
        self._index = 0

    def _stamp(self) -> tuple[int, int]:
        self._index += 1
        t = self._world.loop.now_ns if self._world is not None else 0
        return self._index, t

    def invoke(self, client: str, f: str, key: Any = None, value: Any = None) -> Operation:
        idx, t = self._stamp()
        op = Operation(len(self.ops), str(client), f, key, value, idx, t, _history=self)
        self.ops.append(op)
        if self._world is not None:
            self._world.trace.emit(
                "invoke",
                1,
                op=op.id,
                client=op.client,
                f=f,
                key=key,
                value=value,
                node=self._world.current_node().name,
            )
        return op

    def _complete(self, op: Operation, status: str, output: Any, error: Any) -> None:
        if op.status != "pending":
            raise RuntimeError(f"operation {op} completed twice")
        idx, t = self._stamp()
        op.status = status
        op.output = output
        op.complete_index = idx
        op.complete_time = t
        op.error = (
            None
            if error is None
            else (error if isinstance(error, str) else f"{type(error).__name__}: {error}")
        )
        if self._world is not None:
            self._world.trace.emit(
                "complete",
                1,
                op=op.id,
                client=op.client,
                status=status,
                output=None if output is UNKNOWN else output,
                error=op.error,
            )

    @asynccontextmanager
    async def op(
        self, client: str, f: str, key: Any = None, value: Any = None
    ) -> AsyncIterator[Operation]:
        """Record an operation around a block. If the block raises before
        completing the operation it is recorded as ``info`` (indeterminate)."""
        o = self.invoke(client, f, key, value)
        try:
            yield o
        except BaseException as exc:
            if not o.done:
                o.info(exc)
            raise
        finally:
            pass

    def by_status(self, status: str) -> list[Operation]:
        return [o for o in self.ops if o.status == status]

    def summary(self) -> dict[str, int]:
        out = {"ok": 0, "fail": 0, "info": 0, "pending": 0}
        for o in self.ops:
            out[o.status] += 1
        return out

    def to_json(self) -> list[dict[str, Any]]:
        return [o.to_json() for o in self.ops]

    @classmethod
    def from_ops(cls, ops: Iterable[Operation]) -> History:
        h = cls()
        h.ops = list(ops)
        h._index = max((max(o.invoke_index, o.complete_index or 0) for o in h.ops), default=0)
        return h
