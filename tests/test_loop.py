"""SimLoop scheduling, virtual time, asyncio semantics and budgets."""

from __future__ import annotations

import asyncio
import contextvars
import threading
from typing import Any

import pytest

import detloop
from detloop.config import Budgets, SimConfig
from detloop.errors import UnsupportedOperation
from detloop.loop import SimLoop

from .conftest import expect_pass, run_sim


def run_standalone(coro_fn: Any, **cfg: Any) -> Any:
    loop = SimLoop(config=SimConfig(nodes=(), **cfg))
    with asyncio.Runner(loop_factory=lambda: loop) as runner:
        return runner.run(coro_fn())


# ---------------------------------------------------------------- ordering
async def _ordering_workload() -> list[str]:
    loop = asyncio.get_running_loop()
    log: list[str] = []
    for i in range(5):
        loop.call_soon(log.append, f"soon{i}")

    async def worker(name: str) -> None:
        for step in range(3):
            log.append(f"{name}{step}")
            await asyncio.sleep(0)

    await asyncio.gather(worker("a"), worker("b"), worker("c"))
    fut = loop.create_future()
    fut.add_done_callback(lambda f: log.append("done-cb"))
    loop.call_soon(fut.set_result, 1)
    await fut
    log.append("end")
    return log


def test_fifo_scheduler_matches_real_asyncio_order() -> None:
    real = asyncio.run(_ordering_workload())
    sim = run_standalone(_ordering_workload, scheduler="fifo")
    assert sim == real


def test_call_soon_order_preserved_per_owner_under_interleave() -> None:
    async def body(world: detloop.World) -> None:
        logs: dict[str, list[int]] = {}

        async def node_main(node: detloop.Node) -> None:
            loop = asyncio.get_running_loop()
            out = logs.setdefault(node.name, [])
            for i in range(50):
                loop.call_soon(out.append, i)
            await asyncio.sleep(1)

        world.start(node_main)
        await asyncio.sleep(2)
        for name, out in logs.items():
            assert out == list(range(50)), name

    for seed in range(5):
        expect_pass(body, seed)


def test_interleave_explores_cross_node_orders() -> None:
    async def body(world: detloop.World) -> None:
        order: list[str] = []

        async def node_main(node: detloop.Node) -> None:
            for _ in range(3):
                order.append(node.name)
                await asyncio.sleep(0)

        world.start(node_main)
        await asyncio.sleep(1)
        world.note("order", value="".join(n[-1] for n in order))

    orders = set()
    for seed in range(30):
        res = expect_pass(body, seed)
        orders.add(next(line for line in res.trace_lines if '"note":"order"' in line))
    assert len(orders) > 3


def test_relaxed_scheduler_can_break_fifo_but_is_opt_in() -> None:
    async def body(world: detloop.World) -> None:
        loop = asyncio.get_running_loop()
        out: list[int] = []
        for i in range(20):
            loop.call_soon(out.append, i)
        await asyncio.sleep(0.1)
        world.note("fifo", ok=out == sorted(out))

    broke = False
    for seed in range(10):
        assert any('"ok":true' in line for line in expect_pass(body, seed).trace_lines)
        res = expect_pass(body, seed, scheduler="relaxed")
        broke |= any('"ok":false' in line for line in res.trace_lines)
    assert broke, "relaxed mode should explore non-FIFO orders"


# ---------------------------------------------------------------- virtual time
def test_virtual_time_advances_only_when_idle_and_jumps() -> None:
    async def main() -> tuple[float, ...]:
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        for _ in range(1000):
            await asyncio.sleep(0)
        t1 = loop.time()
        await asyncio.sleep(3600)
        t2 = loop.time()
        return t0, t1, t2

    t0, t1, t2 = run_standalone(main, budgets=Budgets(max_virtual_time=7200.0))
    assert t0 == t1 == 0.0
    assert t2 == 3600.0


def test_equal_time_timers_fire_in_registration_order_and_cancel_works() -> None:
    async def main() -> list[str]:
        loop = asyncio.get_running_loop()
        out: list[str] = []
        handles = [loop.call_later(1.0, out.append, f"t{i}") for i in range(10)]
        handles[3].cancel()
        loop.call_at(loop.time() + 0.5, out.append, "early")
        loop.call_later(-5, out.append, "past")
        await asyncio.sleep(2)
        return out

    assert run_standalone(main) == ["past", "early"] + [f"t{i}" for i in range(10) if i != 3]


def test_timers_due_now_run_after_current_batch_like_asyncio() -> None:
    async def workload() -> list[str]:
        loop = asyncio.get_running_loop()
        out: list[str] = []
        loop.call_later(0, out.append, "timer0")
        loop.call_soon(out.append, "soon1")
        loop.call_soon(lambda: loop.call_soon(out.append, "nested"))
        await asyncio.sleep(0.01)
        return out

    assert run_standalone(workload, scheduler="fifo") == asyncio.run(workload())


# ---------------------------------------------------------------- asyncio features
def test_asyncio_primitives_supported_subset() -> None:
    cv: contextvars.ContextVar[str] = contextvars.ContextVar("cv", default="unset")

    async def main() -> dict[str, Any]:
        out: dict[str, Any] = {}
        try:
            await asyncio.wait_for(asyncio.sleep(10), 1)
        except TimeoutError:
            out["wait_for"] = "timeout"
        async with asyncio.timeout(5):
            await asyncio.sleep(1)
        try:
            async with asyncio.timeout(0.5):
                await asyncio.sleep(1)
        except TimeoutError:
            out["timeout"] = asyncio.get_running_loop().time()
        async with asyncio.TaskGroup() as tg:
            t1 = tg.create_task(asyncio.sleep(1, "a"))
            t2 = tg.create_task(asyncio.sleep(2, "b"))
        out["taskgroup"] = (t1.result(), t2.result())
        with pytest.raises(ExceptionGroup):
            async with asyncio.TaskGroup() as tg:
                tg.create_task(asyncio.sleep(5))

                async def boom() -> None:
                    raise ValueError("x")

                tg.create_task(boom())
        out["gather"] = await asyncio.gather(asyncio.sleep(1, 1), asyncio.sleep(0, 2), return_exceptions=True)
        inner = asyncio.ensure_future(asyncio.sleep(1, "shielded"))
        out["shield"] = await asyncio.shield(inner)
        lock, sem, cond, ev = asyncio.Lock(), asyncio.Semaphore(2), asyncio.Condition(), asyncio.Event()
        seen: list[int] = []

        async def user(i: int) -> None:
            async with sem, lock:
                seen.append(i)
                await asyncio.sleep(0.1)
            async with cond:
                await cond.wait_for(ev.is_set)

        users = [asyncio.create_task(user(i)) for i in range(4)]
        await asyncio.sleep(1)
        ev.set()
        async with cond:
            cond.notify_all()
        await asyncio.gather(*users)
        out["sync"] = seen
        q: asyncio.Queue[int] = asyncio.Queue(maxsize=1)

        async def prod() -> None:
            for i in range(3):
                await q.put(i)

        p = asyncio.create_task(prod())
        out["queue"] = [await q.get() for _ in range(3)]
        await p
        cv.set("parent")

        async def child() -> str:
            return cv.get()

        out["contextvar"] = await asyncio.create_task(child())
        closed: list[bool] = []

        async def agen() -> Any:
            try:
                yield 1
                yield 2
            finally:
                closed.append(True)

        async for _ in agen():
            break
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        out["agen_closed"] = closed
        t = asyncio.create_task(asyncio.sleep(100))
        await asyncio.sleep(0)
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            out["cancelled"] = t.cancelled()
        return out

    out = run_standalone(main)
    assert out["wait_for"] == "timeout"
    assert out["timeout"] == pytest.approx(2.5)
    assert out["taskgroup"] == ("a", "b")
    assert out["gather"] == [1, 2]
    assert out["shield"] == "shielded"
    assert sorted(out["sync"]) == [0, 1, 2, 3]
    assert out["queue"] == [0, 1, 2]
    assert out["contextvar"] == "parent"
    assert out["agen_closed"] == [True]
    assert out["cancelled"] is True


def test_exceptions_propagate_and_runner_cleans_up() -> None:
    leftovers: list[asyncio.Task[Any]] = []

    async def main() -> None:
        leftovers.append(asyncio.create_task(asyncio.sleep(100)))
        raise KeyError("boom")

    loop = SimLoop(config=SimConfig(nodes=()))
    with pytest.raises(KeyError), asyncio.Runner(loop_factory=lambda: loop) as runner:
        runner.run(main())
    assert loop.is_closed()
    assert leftovers[0].cancelled()


def test_eager_task_factory_when_available() -> None:
    if not hasattr(asyncio, "eager_task_factory"):
        pytest.skip("eager tasks need Python 3.12+")

    async def main() -> list[str]:
        asyncio.get_running_loop().set_task_factory(asyncio.eager_task_factory)
        out: list[str] = []

        async def quick() -> None:
            out.append("ran-eagerly")

        t = asyncio.create_task(quick())
        out.append("after-create")
        await t
        return out

    assert run_standalone(main) == ["ran-eagerly", "after-create"]


# ---------------------------------------------------------------- failures and budgets
def test_deadlock_is_detected_with_blocked_tasks() -> None:
    async def body(world: detloop.World) -> None:
        await asyncio.get_running_loop().create_future()

    res = run_sim(body)
    assert (res.status, res.kind) == ("fail", "deadlock")
    assert any("body" in d for d in res.details)


def test_busy_loop_is_starvation_not_a_pass() -> None:
    async def body(world: detloop.World) -> None:
        while True:
            await asyncio.sleep(0)

    res = run_sim(body, max_steps_without_progress=5000)
    assert (res.status, res.kind) == ("fail", "starvation")


def test_step_budget_is_incomplete() -> None:
    async def body(world: detloop.World) -> None:
        while True:
            await asyncio.sleep(0.001)

    res = run_sim(body, max_steps=3000)
    assert (res.status, res.signature) == ("incomplete", "budget:steps")


def test_virtual_time_budget_is_incomplete() -> None:
    async def body(world: detloop.World) -> None:
        await asyncio.sleep(10_000)

    res = run_sim(body, max_virtual_time=60.0)
    assert (res.status, res.signature) == ("incomplete", "budget:virtual_time")


def test_wall_clock_watchdog_interrupts_cpu_bound_code() -> None:
    if threading.current_thread() is not threading.main_thread():
        pytest.skip("SIGINT-based interruption needs the main thread")

    async def body(world: detloop.World) -> None:
        while True:  # never yields
            pass

    res = run_sim(body, wall_timeout=0.5)
    assert (res.status, res.kind) == ("incomplete", "wall_timeout")


def test_unsupported_operations_fail_loudly() -> None:
    async def body(world: detloop.World) -> None:
        loop = asyncio.get_running_loop()
        with pytest.raises(UnsupportedOperation):
            await loop.create_unix_connection(asyncio.Protocol, "/tmp/x")
        with pytest.raises(UnsupportedOperation):
            await loop.create_datagram_endpoint(asyncio.DatagramProtocol)
        with pytest.raises(UnsupportedOperation):
            loop.add_reader(0, lambda: None)
        with pytest.raises(UnsupportedOperation):
            await asyncio.open_connection("n1", 1, ssl=True)
        with pytest.raises(UnsupportedOperation):
            loop.add_signal_handler(2, lambda: None)

    expect_pass(body)


def test_executor_rejected_by_default_and_inline_opt_in() -> None:
    async def body(world: detloop.World) -> None:
        await asyncio.to_thread(sum, [1, 2])

    res = run_sim(body)
    assert (res.status, res.kind) == ("fail", "nondeterminism")

    async def body2(world: detloop.World) -> None:
        assert await asyncio.to_thread(sum, [1, 2]) == 3
        assert await asyncio.get_running_loop().run_in_executor(None, max, 4, 9) == 9

    expect_pass(body2, executor="inline")


def test_cross_thread_call_soon_threadsafe_is_a_violation() -> None:
    async def body(world: detloop.World) -> None:
        loop = asyncio.get_running_loop()
        errors: list[BaseException] = []

        def other() -> None:
            try:
                loop.call_soon_threadsafe(lambda: None)
            except BaseException as exc:
                errors.append(exc)

        # Start the thread with the guard bypassed by using the original start.
        from detloop import guards

        orig = guards._ORIGINALS[(threading.Thread, "start")]
        t = threading.Thread(target=other)
        orig(t)
        t.join()
        assert errors and "another thread" in str(errors[0])
        for _ in range(5000):
            await asyncio.sleep(0)

    res = run_sim(body)
    assert (res.status, res.kind) == ("fail", "nondeterminism")


def test_loop_errors_recorded_and_optionally_fatal() -> None:
    async def body(world: detloop.World) -> None:
        asyncio.get_running_loop().call_soon(lambda: 1 / 0)
        await asyncio.sleep(0.1)

    res = run_sim(body)
    assert res.status == "pass"
    assert res.loop_errors and res.loop_errors[0]["exception"] == "ZeroDivisionError"
    res = run_sim(body, fail_on_loop_errors=True)
    assert res.status == "fail"


def test_task_names_are_deterministic_per_run() -> None:
    async def body(world: detloop.World) -> None:
        t = asyncio.create_task(asyncio.sleep(0))
        world.note("name", value=t.get_name())
        await t

    a = run_sim(body)
    b = run_sim(body)
    assert a.trace_sha256 == b.trace_sha256


def test_loop_cannot_run_inside_running_loop() -> None:
    async def outer() -> None:
        async def body(world: detloop.World) -> None:
            pass

        with pytest.raises(detloop.ConfigError):
            detloop.run(body, seed=0)

    asyncio.run(outer())


def test_stop_and_run_forever_semantics() -> None:
    loop = SimLoop(config=SimConfig(nodes=()))
    out: list[int] = []
    loop.call_soon(out.append, 1)
    loop.call_soon(loop.stop)
    loop.call_soon(out.append, 2)
    loop.run_forever()
    assert out == [1, 2]
    loop.call_soon(out.append, 3)
    loop.stop()
    loop.run_forever()  # stop() before run_forever runs one iteration
    assert out == [1, 2, 3]
    loop.close()
    with pytest.raises(RuntimeError):
        loop.call_soon(out.append, 4)
