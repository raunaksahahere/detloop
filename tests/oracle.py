"""Independent brute-force linearizability oracle for small histories.

Shares no code with the checker: it enumerates every subset of indeterminate
operations and every permutation consistent with real-time order, and replays
each against a plain sequential register.
"""

from __future__ import annotations

import itertools
from typing import Any

from detloop.history import UNKNOWN, Operation


def _apply(state: Any, op: Operation) -> list[Any]:
    if op.f == "read":
        return [state] if (op.output is UNKNOWN or op.output == state) else []
    if op.f == "write":
        return [op.value]
    if op.f == "cas":
        exp, new = op.value
        succeeded = state == exp
        if op.output is UNKNOWN:
            return [new] if succeeded else [state]
        if op.output is True:
            return [new] if succeeded else []
        return [state] if not succeeded else []
    raise ValueError(op.f)


def _precedes(a: Operation, b: Operation) -> bool:
    return a.status == "ok" and a.complete_index is not None and a.complete_index < b.invoke_index


def brute_force(ops: list[Operation], initial: Any = None) -> bool:
    ops = [o for o in ops if o.status != "fail"]
    definite = [o for o in ops if o.status == "ok"]
    maybe = [o for o in ops if o.status != "ok"]
    for k in range(len(maybe) + 1):
        for chosen in itertools.combinations(maybe, k):
            subset = definite + list(chosen)
            for perm in itertools.permutations(subset):
                if any(
                    _precedes(perm[j], perm[i])
                    for i in range(len(perm))
                    for j in range(i + 1, len(perm))
                ):
                    continue
                states = [initial]
                for op in perm:
                    states = [s2 for s in states for s2 in _apply(s, op)]
                    if not states:
                        break
                if states:
                    return True
    return False
