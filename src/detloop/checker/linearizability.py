"""Bounded linearizability checking.

Algorithm: Wing & Gong's search with Lowe's memoization, in the iterative
linked-list form popularised by Porcupine (see ADR 0005):

* Each operation contributes a *call* entry at its invocation index and a
  *return* entry at its completion index. Indeterminate (``info``/``pending``)
  operations get a return entry after every real event, so they may be
  linearized anywhere after their invocation, including after every observed
  operation, which is equivalent to "never took effect".
* The search repeatedly tries to linearize a call entry whose operation is
  minimal in real-time order; reaching a return entry whose operation has not
  been linearized forces backtracking.
* Memoization caches ``(set of linearized ops, model state)`` pairs.
* Models may be nondeterministic (``step`` returns every possible successor
  state), which is how ambiguous outcomes such as a timed-out compare-and-set
  are handled soundly.
* Work is bounded by ``max_steps`` and ``max_cache``. Exhausting a bound
  yields ``unknown``, never ``valid`` or ``invalid``.

Real-time precedence: ``a`` precedes ``b`` iff ``a`` completed (logical index)
before ``b`` was invoked. Failed operations (status ``fail``) are excluded.
Histories are partitioned by key only when the model says operations on
different partitions are independent (P-compositionality).
"""

from __future__ import annotations

from collections.abc import Hashable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from ..errors import CheckIncomplete, LinearizabilityViolation
from ..history import UNKNOWN, History, Operation

_INF = 1 << 60


class Model:
    """Sequential specification used by the checker.

    Subclasses implement :meth:`init` and :meth:`step`. States must be
    hashable. ``step`` must return every state the operation could lead to
    given its observed output; ``op.output is UNKNOWN`` for indeterminate
    operations, in which case ``step`` must not return an empty list (an
    unobserved operation can always be placed after everything else).
    """

    name = "model"

    def init(self) -> Hashable:
        raise NotImplementedError

    def step(self, state: Any, op: Operation) -> Iterable[Hashable]:
        raise NotImplementedError

    def partition(self, ops: Sequence[Operation]) -> list[tuple[Any, list[Operation]]] | None:
        """Split into independent sub-histories, or return ``None``."""
        return None

    def is_readonly(self, op: Operation) -> bool:
        """Read-only operations with unknown output are dropped before checking."""
        return False


class RegisterModel(Model):
    """A single read/write/compare-and-set register.

    Operations: ``read`` (output = value read), ``write`` (value = new value),
    ``cas`` (value = ``(expected, new)``, output = ``True``/``False``).
    """

    name = "register"

    def __init__(self, initial: Any = None) -> None:
        self.initial = initial

    def init(self) -> Hashable:
        return self.initial

    def is_readonly(self, op: Operation) -> bool:
        return op.f in ("read", "get")

    def step(self, state: Any, op: Operation) -> list[Hashable]:
        f = op.f
        if f in ("read", "get"):
            if op.output is UNKNOWN or op.output == state:
                return [state]
            return []
        if f in ("write", "put"):
            return [op.value]
        if f == "cas":
            expected, new = op.value
            if op.output is UNKNOWN:
                return [new, state] if state == expected and new != state else [state]
            if op.output:
                return [new] if state == expected else []
            return [state] if state != expected else []
        raise ValueError(f"register model does not support operation {f!r}")


class KVModel(RegisterModel):
    """A map of independent registers keyed by ``op.key`` (checked per key)."""

    name = "kv"

    def partition(self, ops: Sequence[Operation]) -> list[tuple[Any, list[Operation]]]:
        groups: dict[str, tuple[Any, list[Operation]]] = {}
        for op in ops:
            k = repr(op.key)
            if k not in groups:
                groups[k] = (op.key, [])
            groups[k][1].append(op)
        return [groups[k] for k in sorted(groups)]


@dataclass
class PartitionResult:
    key: Any
    status: str
    ops: int
    steps: int
    cache: int
    reason: str = ""
    linearized_prefix: list[int] = field(default_factory=list)
    unlinearizable: list[int] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "key": repr(self.key),
            "status": self.status,
            "ops": self.ops,
            "steps": self.steps,
            "cache": self.cache,
            "reason": self.reason,
            "linearized_prefix": self.linearized_prefix,
            "unlinearizable": self.unlinearizable,
        }


@dataclass
class CheckResult:
    status: str  # valid | invalid | unknown
    model: str
    reason: str
    partitions: list[PartitionResult]

    @property
    def valid(self) -> bool:
        return self.status == "valid"

    def to_json(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "model": self.model,
            "reason": self.reason,
            "partitions": [p.to_json() for p in self.partitions],
        }


class _Entry:
    __slots__ = ("is_call", "match", "next", "op", "opid", "prev")

    def __init__(self, op: Operation | None, opid: int, is_call: bool) -> None:
        self.op = op
        self.opid = opid
        self.is_call = is_call
        self.match: _Entry | None = None
        self.prev: _Entry | None = None
        self.next: _Entry | None = None


def _lift(e: _Entry) -> None:
    assert e.prev is not None and e.next is not None and e.match is not None
    e.prev.next = e.next
    e.next.prev = e.prev
    m = e.match
    assert m.prev is not None
    m.prev.next = m.next
    if m.next is not None:
        m.next.prev = m.prev


def _unlift(e: _Entry) -> None:
    m = e.match
    assert m is not None and m.prev is not None and e.prev is not None and e.next is not None
    m.prev.next = m
    if m.next is not None:
        m.next.prev = m
    e.prev.next = e
    e.next.prev = e


def _check_partition(
    model: Model, key: Any, ops: list[Operation], max_steps: int, max_cache: int
) -> PartitionResult:
    events: list[tuple[int, int, int]] = []
    for i, op in enumerate(ops):
        events.append((op.invoke_index, 0, i))
        ret = op.complete_index if op.status == "ok" and op.complete_index is not None else _INF + i
        events.append((ret, 1, i))
    events.sort()
    head = _Entry(None, -1, False)
    calls: dict[int, _Entry] = {}
    rets: dict[int, _Entry] = {}
    prev = head
    for _, kind, i in events:
        e = _Entry(ops[i], i, kind == 0)
        (calls if kind == 0 else rets)[i] = e
        prev.next = e
        e.prev = prev
        prev = e
    for i in range(len(ops)):
        calls[i].match = rets[i]
        rets[i].match = calls[i]

    state: Any = model.init()
    linearized = 0
    stack: list[tuple[_Entry, Any, list[Any], int]] = []
    cache: set[tuple[int, Any]] = set()
    entry = head.next
    steps = 0
    best: list[int] = []
    blocking: list[int] = []

    def try_alternatives(e: _Entry, succ: list[Any], start: int, lin: int) -> int:
        new_lin = lin | (1 << e.opid)
        for j in range(start, len(succ)):
            k = (new_lin, succ[j])
            if k not in cache:
                cache.add(k)
                return j
        return -1

    while head.next is not None:
        steps += 1
        if steps > max_steps or len(cache) > max_cache:
            return PartitionResult(
                key, "unknown", len(ops), steps, len(cache),
                f"search budget exhausted (max_steps={max_steps}, max_cache={max_cache})",
                [ops[i].id for i in best],
            )
        assert entry is not None
        if entry.is_call:
            assert entry.op is not None
            succ = list(model.step(state, entry.op))
            j = try_alternatives(entry, succ, 0, linearized) if succ else -1
            if j >= 0:
                stack.append((entry, state, succ, j))
                state = succ[j]
                linearized |= 1 << entry.opid
                _lift(entry)
                if len(stack) > len(best):
                    best = [s[0].opid for s in stack]
                entry = head.next
            else:
                entry = entry.next
        else:
            if not stack:
                blocking = [entry.opid]
                return PartitionResult(
                    key, "invalid", len(ops), steps, len(cache),
                    "no valid linearization exists", [ops[i].id for i in best],
                    [ops[i].id for i in blocking],
                )
            if len(stack) >= len(best):
                blocking = [entry.opid]
            e, base_state, succ, j = stack.pop()
            linearized &= ~(1 << e.opid)
            _unlift(e)
            j2 = try_alternatives(e, succ, j + 1, linearized)
            if j2 >= 0:
                stack.append((e, base_state, succ, j2))
                state = succ[j2]
                linearized |= 1 << e.opid
                _lift(e)
                entry = head.next
            else:
                state = base_state
                entry = e.next
    return PartitionResult(key, "valid", len(ops), steps, len(cache), "linearizable")


def check_linearizable(
    history: History | Iterable[Operation],
    model: Model,
    *,
    max_steps: int = 2_000_000,
    max_cache: int = 1_000_000,
) -> CheckResult:
    """Check a history; returns ``valid``, ``invalid`` or ``unknown``."""
    ops_all = list(history.ops if isinstance(history, History) else history)
    ops = [
        o for o in ops_all
        if o.status != "fail" and not (o.output is UNKNOWN and o.status != "ok" and model.is_readonly(o))
    ]
    parts = model.partition(ops) or [(None, ops)]
    results = [_check_partition(model, key, list(p), max_steps, max_cache) for key, p in parts]
    if any(r.status == "invalid" for r in results):
        bad = [r for r in results if r.status == "invalid"]
        keys = ", ".join(repr(r.key) for r in bad) if parts[0][0] is not None or len(parts) > 1 else ""
        reason = f"not linearizable{' for key(s) ' + keys if keys else ''}"
        return CheckResult("invalid", model.name, reason, results)
    if any(r.status == "unknown" for r in results):
        return CheckResult("unknown", model.name, "checker budget exhausted for some partitions", results)
    return CheckResult("valid", model.name, f"linearizable ({len(ops)} operations)", results)


def assert_linearizable(
    history: History | Iterable[Operation],
    model: Model,
    *,
    max_steps: int = 2_000_000,
    max_cache: int = 1_000_000,
) -> CheckResult:
    """Raise :class:`LinearizabilityViolation` if invalid, :class:`CheckIncomplete`
    if the checker could not decide; return the result if valid."""
    result = check_linearizable(history, model, max_steps=max_steps, max_cache=max_cache)
    world = history._world if isinstance(history, History) else None
    if world is not None:
        world.trace.emit("check", 1, checker="linearizability", status=result.status, reason=result.reason,
                         partitions=[p.to_json() for p in result.partitions if p.status != "valid"])
    if result.status == "invalid":
        ops = history.ops if isinstance(history, History) else list(history)
        detail = []
        for p in result.partitions:
            if p.status == "invalid":
                by_id = {o.id: o for o in ops}
                culprit = ", ".join(str(by_id[i]) for i in p.unlinearizable if i in by_id)
                detail.append(f"key {p.key!r}: cannot linearize {culprit}")
        raise LinearizabilityViolation(result.reason + ("; " + "; ".join(detail) if detail else ""), result)
    if result.status == "unknown":
        raise CheckIncomplete(result.reason, result)
    return result
