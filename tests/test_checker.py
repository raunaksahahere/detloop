"""Linearizability checker vs. an independent brute-force oracle."""

from __future__ import annotations

from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from detloop.checker import KVModel, RegisterModel, assert_linearizable, check_linearizable
from detloop.errors import CheckIncomplete, LinearizabilityViolation
from detloop.history import UNKNOWN, History, Operation

from .oracle import brute_force


def H(*spec: tuple[Any, ...]) -> list[Operation]:
    """Build ops from (client, f, value, invoke_idx, status, output, complete_idx[, key])."""
    ops = []
    for i, s in enumerate(spec):
        client, f, value, inv, status, output, comp = s[:7]
        key = s[7] if len(s) > 7 else None
        ops.append(
            Operation(
                i,
                client,
                f,
                key,
                value,
                inv,
                inv,
                status,
                UNKNOWN if output is None and status != "ok" else output,
                comp,
                comp,
            )
        )
    return ops


def check(ops: list[Operation]) -> str:
    return check_linearizable(ops, RegisterModel()).status


def test_sequential_and_overlapping_legal_histories() -> None:
    assert (
        check(H(("a", "write", 1, 1, "ok", None, 2), ("b", "read", None, 3, "ok", 1, 4))) == "valid"
    )
    # overlapping write/read may see either value
    assert (
        check(H(("a", "write", 1, 1, "ok", None, 4), ("b", "read", None, 2, "ok", None, 3)))
        == "valid"
    )
    assert (
        check(H(("a", "write", 1, 1, "ok", None, 4), ("b", "read", None, 2, "ok", 1, 3))) == "valid"
    )


def test_stale_read_after_completed_write_is_invalid() -> None:
    ops = H(("a", "write", 1, 1, "ok", None, 2), ("b", "read", None, 3, "ok", None, 4))
    assert check(ops) == "invalid"
    with pytest.raises(LinearizabilityViolation, match="cannot linearize"):
        assert_linearizable(ops, RegisterModel())


def test_real_time_order_is_respected() -> None:
    # w1 completes before w2 starts; a later read must not see 1
    ops = H(
        ("a", "write", 1, 1, "ok", None, 2),
        ("a", "write", 2, 3, "ok", None, 4),
        ("b", "read", None, 5, "ok", 1, 6),
    )
    assert check(ops) == "invalid"


def test_pending_and_timed_out_writes_may_or_may_not_take_effect() -> None:
    # a timed-out write (info) may be observed...
    ops = H(("a", "write", 1, 1, "info", None, 2), ("b", "read", None, 3, "ok", 1, 4))
    assert check(ops) == "valid"
    # ...or not observed, even long after it was invoked
    ops = H(("a", "write", 1, 1, "info", None, 2), ("b", "read", None, 3, "ok", None, 4))
    assert check(ops) == "valid"
    # a never-completed write may take effect after everything else
    ops = H(
        ("a", "write", 1, 1, "pending", None, None),
        ("b", "read", None, 3, "ok", None, 4),
        ("b", "read", None, 5, "ok", 1, 6),
    )
    assert check(ops) == "valid"
    # but it cannot be "un-applied" once observed
    ops = H(
        ("a", "write", 1, 1, "info", None, 2),
        ("b", "read", None, 3, "ok", 1, 4),
        ("b", "read", None, 5, "ok", None, 6),
    )
    assert check(ops) == "invalid"


def test_failed_operations_are_excluded() -> None:
    ops = H(("a", "write", 1, 1, "fail", None, 2), ("b", "read", None, 3, "ok", None, 4))
    assert check(ops) == "valid"
    ops = H(("a", "write", 1, 1, "fail", None, 2), ("b", "read", None, 3, "ok", 1, 4))
    assert check(ops) == "invalid"


def test_cas_including_ambiguous_outcome() -> None:
    ops = H(
        ("a", "write", 0, 1, "ok", None, 2),
        ("b", "cas", (0, 5), 3, "ok", True, 4),
        ("c", "read", None, 5, "ok", 5, 6),
    )
    assert check(ops) == "valid"
    ops = H(("a", "write", 0, 1, "ok", None, 2), ("b", "cas", (0, 5), 3, "ok", False, 4))
    assert check(ops) == "invalid"
    # timed-out CAS: both outcomes admissible
    for seen in (0, 5):
        ops = H(
            ("a", "write", 0, 1, "ok", None, 2),
            ("b", "cas", (0, 5), 3, "info", None, 4),
            ("c", "read", None, 5, "ok", seen, 6),
        )
        assert check(ops) == "valid"


def test_kv_model_partitions_by_key() -> None:
    ops = H(
        ("a", "write", 1, 1, "ok", None, 2, "x"),
        ("b", "read", None, 3, "ok", None, 4, "y"),
        ("b", "read", None, 5, "ok", 1, 6, "x"),
    )
    res = check_linearizable(ops, KVModel())
    assert res.status == "valid" and len(res.partitions) == 2
    ops.append(Operation(3, "c", "read", "x", None, 7, 7, "ok", None, 8, 8))
    res = check_linearizable(ops, KVModel())
    assert res.status == "invalid" and "'x'" in res.reason


def test_budget_exhaustion_is_unknown_never_valid() -> None:
    ops = [
        Operation(i, f"c{i}", "write", None, i, 1 + i, 0, "info", UNKNOWN, 100 + i, 0)
        for i in range(12)
    ]
    ops.append(Operation(12, "r", "read", None, None, 200, 0, "ok", -1, 201, 0))  # impossible value
    res = check_linearizable(ops, RegisterModel(), max_steps=50)
    assert res.status == "unknown"
    with pytest.raises(CheckIncomplete):
        assert_linearizable(ops, RegisterModel(), max_steps=50)
    # with enough budget the same history is decided (invalid)
    assert check_linearizable(ops, RegisterModel()).status == "invalid"


def test_history_api_records_logical_order() -> None:
    h = History()
    w = h.invoke("a", "write", value=1)
    r = h.invoke("b", "read")
    w.ok()
    r.ok(1)
    assert [o.invoke_index for o in h.ops] == [1, 2]
    assert (w.complete_index, r.complete_index) == (3, 4)
    assert h.summary() == {"ok": 2, "fail": 0, "info": 0, "pending": 0}
    with pytest.raises(RuntimeError):
        w.ok()
    assert check_linearizable(h, RegisterModel()).valid


# ------------------------------------------------------------- oracle agreement
@st.composite
def histories(draw: Any) -> list[Operation]:
    n = draw(st.integers(1, 5))
    events: list[tuple[int, str]] = []
    ops_spec = []
    for i in range(n):
        f = draw(st.sampled_from(["write", "read", "read", "cas"]))
        status = draw(st.sampled_from(["ok", "ok", "ok", "info", "pending", "fail"]))
        if f == "write":
            value, output = draw(st.integers(0, 2)), None
        elif f == "read":
            value, output = None, draw(st.sampled_from([None, 0, 1, 2]))
        else:
            value, output = (
                (draw(st.sampled_from([None, 0, 1])), draw(st.integers(0, 2))),
                draw(st.booleans()),
            )
        ops_spec.append((f, status, value, output))
        events.append((i, "inv"))
        if status not in ("pending",):
            events.append((i, "ret"))
    order = draw(st.permutations(events))
    # each op's return must come after its invocation
    pos: dict[tuple[int, str], int] = {}
    fixed: list[tuple[int, str]] = []
    for ev in order:
        if ev[1] == "ret" and (ev[0], "inv") not in pos:
            continue
        pos[ev] = len(fixed)
        fixed.append(ev)
    for ev in order:
        if ev not in pos:
            pos[ev] = len(fixed)
            fixed.append(ev)
    ops = []
    for i, (f, status, value, output) in enumerate(ops_spec):
        inv = pos[(i, "inv")] + 1
        comp = pos[(i, "ret")] + 1 if (i, "ret") in pos else None
        out = output if status == "ok" else UNKNOWN
        if f == "write" and status == "ok":
            out = None
        ops.append(Operation(i, f"c{i}", f, None, value, inv, inv, status, out, comp, comp))
    return ops


@settings(max_examples=1500, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(histories())
def test_checker_agrees_with_brute_force_oracle(ops: list[Operation]) -> None:
    expected = brute_force(ops)
    result = check_linearizable(ops, RegisterModel())
    assert result.status == ("valid" if expected else "invalid"), [str(o) for o in ops]


def test_exhaustive_two_op_histories_agree_with_oracle() -> None:
    """Every interleaving of two operations over a small value domain."""
    from itertools import product

    shapes = [
        ("write", 1, None),
        ("write", 2, None),
        ("read", None, None),
        ("read", None, 1),
        ("read", None, 2),
    ]
    statuses = ["ok", "info", "fail"]
    orders = [(1, 2, 3, 4), (1, 3, 2, 4), (1, 4, 2, 3)]  # a before b, overlap, b nested in a
    count = 0
    for (fa, va, oa), (fb, vb, ob), sa, sb, (ia, ca, ib, cb) in product(
        shapes, shapes, statuses, statuses, orders
    ):
        ops = [
            Operation(0, "a", fa, None, va, ia, ia, sa, oa if sa == "ok" else UNKNOWN, ca, ca),
            Operation(1, "b", fb, None, vb, ib, ib, sb, ob if sb == "ok" else UNKNOWN, cb, cb),
        ]
        assert check_linearizable(ops, RegisterModel()).status == (
            "valid" if brute_force(ops) else "invalid"
        )
        count += 1
    assert count == len(shapes) ** 2 * len(statuses) ** 2 * len(orders)
