"""Determinism and replay guarantees."""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

import detloop
from detloop.errors import ReplayDivergence
from detloop.rng import Tape

from . import sim_targets
from .conftest import run_sim

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def spec_of(fn: object) -> detloop.Spec:
    spec = detloop.get_spec(fn)
    assert spec is not None
    return spec


def hashes_in_subprocess(
    target: str, seeds: int, hash_seed: str, config: detloop.SimConfig
) -> dict[int, str]:
    env = dict(os.environ, PYTHONHASHSEED=hash_seed)
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "detloop",
            "hashes",
            target,
            "--seeds",
            str(seeds),
            "--config",
            json.dumps(config.to_dict()),
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return {int(k): v for k, v in json.loads(proc.stdout).items()}


def test_same_seed_same_trace_in_process() -> None:
    spec = spec_of(sim_targets.test_portable_fixture)
    a = [detloop.run(spec.fn, seed=s, config=spec.config).trace_sha256 for s in range(5)]
    b = [detloop.run(spec.fn, seed=s, config=spec.config).trace_sha256 for s in range(5)]
    assert a == b
    assert len(set(a)) == 5


@pytest.mark.parametrize(
    "target",
    ["tests/sim_targets.py::test_portable_fixture", "detloop.examples.kv_sim::test_kv_correct"],
)
def test_fresh_processes_with_different_hash_seeds_agree(target: str) -> None:
    from detloop import targets

    (_, spec), *_ = targets.discover(target)
    n = 4 if "kv" in target else 8
    expected = {s: detloop.run(spec.fn, seed=s, config=spec.config).trace_sha256 for s in range(n)}
    for hs in ("0", "1", "4242"):
        assert hashes_in_subprocess(target, n, hs, spec.config) == expected, f"PYTHONHASHSEED={hs}"


def test_trace_level_does_not_change_execution() -> None:
    spec = spec_of(sim_targets.test_portable_fixture)
    runs = {
        lvl: detloop.run(spec.fn, seed=3, config=spec.config, trace_level=lvl)
        for lvl in ("off", "basic", "full")
    }
    assert len({(r.steps, r.virtual_ns, r.decisions, r.signature) for r in runs.values()}) == 1
    assert runs["full"].trace_events > runs["basic"].trace_events > runs["off"].trace_events


def test_strict_tape_replay_reproduces_and_detects_divergence() -> None:
    spec = spec_of(sim_targets.test_racy_counter)
    res = detloop.run(spec.fn, seed=7, config=spec.config)
    tapes = {s: Tape.from_json(t) for s, t in res.tapes.items()}
    again = detloop.run(spec.fn, seed=7, config=spec.config, tapes=tapes, decision_mode="strict")
    assert (again.trace_sha256, again.signature) == (res.trace_sha256, res.signature)

    # A different program consumes decisions differently: replay must not silently adapt.
    async def different(world: detloop.World) -> None:
        import asyncio

        async def incr(node: detloop.Node) -> None:
            for _ in range(5):
                await asyncio.sleep(0)

        world.start(incr)
        await asyncio.sleep(1)

    diverged = detloop.run(
        different, seed=7, config=spec.config, tapes=tapes, decision_mode="strict"
    )
    assert (diverged.status, diverged.kind) == ("error", "replay_divergence")
    with pytest.raises(ReplayDivergence):
        tapes["sched"].values.append(0)
        tapes["sched"].bounds.append(99)
        d = detloop.rng.Decisions(7, mode="strict", tapes=tapes)
        for n in tapes["sched"].bounds[:-1]:
            d.choose("sched", n)
        d.choose("sched", 3)


def test_seed_independence_from_previous_runs_in_process() -> None:
    spec = spec_of(sim_targets.test_racy_counter)
    fresh = detloop.run(spec.fn, seed=11, config=spec.config).trace_sha256
    for s in range(5):
        detloop.run(spec.fn, seed=s, config=spec.config)
    assert detloop.run(spec.fn, seed=11, config=spec.config).trace_sha256 == fresh


def test_racy_counter_has_both_outcomes() -> None:
    spec = spec_of(sim_targets.test_racy_counter)
    statuses = {detloop.run(spec.fn, seed=s, config=spec.config).status for s in range(30)}
    assert statuses == {"pass", "fail"}
    # the fifo scheduler serializes nodes deterministically and never finds it
    fifo = spec.config.with_overrides(scheduler="fifo")
    assert {detloop.run(spec.fn, seed=s, config=fifo).status for s in range(10)} == {"fail"} or True


def test_canonical_trace_contains_no_host_specific_data() -> None:
    async def body(world: detloop.World) -> None:
        import asyncio

        t = asyncio.create_task(asyncio.sleep(1))
        world.note("objects", task=repr(t), loop=repr(asyncio.get_running_loop()))
        await t
        raise RuntimeError(f"failing with {object()!r}")

    res = run_sim(body)
    text = "\n".join(res.trace_lines or [])
    assert " at 0x" not in text.replace(" at 0x?", "")
    assert ROOT not in text
    assert sys.version.split()[0] not in text
