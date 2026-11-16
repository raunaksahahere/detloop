"""Reproducible detloop benchmarks. Writes raw JSON to benchmarks/results/.

    python benchmarks/bench.py                 # everything (several minutes)
    python benchmarks/bench.py --quick         # smaller budgets
    python benchmarks/bench.py --only throughput,trace

Sections: throughput (seeds/s), trace (event throughput + trace overhead),
memory (tracemalloc peak), corpus (seeds-to-first-failure per planted bug),
shrink (reduction metrics), baseline (real asyncio + random sleeps).

Numbers are machine-specific; they are recorded with hardware and runtime
metadata and are never used as CI pass/fail thresholds.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import datetime as dt
import json
import os
import platform
import statistics
import subprocess
import sys
import time
import tracemalloc
from concurrent.futures import ProcessPoolExecutor
from typing import Any

import detloop
from detloop.examples import kv_sim
from detloop.explore import explore
from detloop.shrink import shrink
from detloop.targets import Target

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
BUGS = {
    "stale-read": "test_kv_stale_read",
    "lost-ack": "test_kv_lost_ack",
    "split-brain": "test_kv_split_brain",
}


def machine() -> dict[str, Any]:
    cpu = platform.processor() or platform.machine()
    try:
        with open("/proc/cpuinfo") as fh:
            for line in fh:
                if line.startswith("model name"):
                    cpu = line.split(":", 1)[1].strip()
                    break
    except OSError:
        pass
    mem = None
    with contextlib.suppress(ValueError, OSError, AttributeError):
        mem = round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1)
    commit = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    return {
        "cpu": cpu,
        "logical_cpus": os.cpu_count(),
        "memory_gib": mem,
        "os": platform.platform(),
        "python": platform.python_version(),
        "implementation": sys.implementation.name,
        "detloop": detloop.__version__,
        "git_commit": commit or None,
        "date": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat(),
    }


def spec(name: str) -> detloop.Spec:
    s = detloop.get_spec(getattr(kv_sim, name))
    assert s is not None
    return s


# --------------------------------------------------------------- micro workloads
async def pingpong(world: detloop.World) -> None:
    """CPU-bound scheduling: 3 nodes x 20 tasks exchanging 200 messages each."""

    async def node_main(node: detloop.Node) -> None:
        q: asyncio.Queue[int] = asyncio.Queue()

        async def producer() -> None:
            for i in range(200):
                await q.put(i)
                await asyncio.sleep(0)

        async def consumer() -> None:
            for _ in range(200):
                await q.get()

        await asyncio.gather(*(producer() for _ in range(10)), *(consumer() for _ in range(10)))

    world.start(node_main)
    await asyncio.sleep(1)


async def bulk_transfer(world: detloop.World) -> None:
    """Network: 8 MiB echoed between two nodes."""

    async def server(node: detloop.Node) -> None:
        async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
            while data := await r.read(65536):
                w.write(data)
                await w.drain()
            w.close()

        await (await asyncio.start_server(handle, port=9000)).serve_forever()

    world.start(server, nodes=["n1"])
    await asyncio.sleep(0.01)
    r, w = await asyncio.open_connection("n1", 9000)
    payload = b"x" * (8 * 2**20)

    async def send() -> None:
        w.write(payload)
        await w.drain()
        w.write_eof()

    await asyncio.gather(send(), r.readexactly(len(payload)))
    w.close()


def timed_runs(fn: Any, config: detloop.SimConfig, seeds: range) -> dict[str, Any]:
    t = time.perf_counter()
    steps = events = 0
    for s in seeds:
        r = detloop.run(fn, seed=s, config=config)
        assert r.status == "pass", r.message
        steps += r.steps
        events += r.trace_events
    wall = time.perf_counter() - t
    return {
        "seeds": len(seeds),
        "wall_seconds": round(wall, 3),
        "seeds_per_second": round(len(seeds) / wall, 2),
        "steps": steps,
        "steps_per_second": round(steps / wall),
        "trace_events": events,
    }


def bench_throughput(quick: bool) -> dict[str, Any]:
    out: dict[str, Any] = {}
    s = spec("test_kv_correct")
    out["kv_serial"] = timed_runs(s.fn, s.config, range(10 if quick else 40))
    jobs = os.cpu_count() or 2
    n = 48 if quick else 240
    r = explore(
        s,
        range(n),
        target=Target("detloop.examples.kv_sim", "test_kv_correct"),
        jobs=jobs,
        artifacts_dir=None,
    )
    assert r.summary()["passed"] == n
    out["kv_parallel"] = {
        "jobs": jobs,
        "seeds": n,
        "wall_seconds": round(r.wall_seconds, 3),
        "seeds_per_second": round(n / r.wall_seconds, 2),
    }
    cfg = detloop.SimConfig(nodes=("n1",), trace="basic")
    out["bulk_transfer_8MiB"] = timed_runs(bulk_transfer, cfg, range(3))
    return out


def bench_trace(quick: bool) -> dict[str, Any]:
    out: dict[str, Any] = {}
    seeds = range(3 if quick else 10)
    for level in ("off", "basic", "full"):
        cfg = detloop.SimConfig(trace=level)
        out[f"pingpong_{level}"] = timed_runs(pingpong, cfg, seeds)
        s = spec("test_kv_correct")
        out[f"kv_{level}"] = timed_runs(s.fn, s.config.with_overrides(trace=level), seeds)
    for wl in ("pingpong", "kv"):
        base = out[f"{wl}_off"]["wall_seconds"]
        for level in ("basic", "full"):
            out[f"{wl}_overhead_{level}_pct"] = round(
                100 * (out[f"{wl}_{level}"]["wall_seconds"] / base - 1), 1
            )
    return out


def bench_memory(quick: bool) -> dict[str, Any]:
    out: dict[str, Any] = {}
    s = spec("test_kv_correct")
    for name, fn, cfg in (
        ("kv_basic", s.fn, s.config),
        ("kv_full", s.fn, s.config.with_overrides(trace="full")),
        ("pingpong_basic", pingpong, detloop.SimConfig()),
    ):
        tracemalloc.start()
        detloop.run(fn, seed=0, config=cfg, keep_trace=True)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        out[f"{name}_peak_mib"] = round(peak / 2**20, 2)
    return out


def bench_corpus(quick: bool) -> dict[str, Any]:
    n = 100 if quick else 300
    jobs = os.cpu_count() or 2
    out: dict[str, Any] = {"seeds_per_bug": n}
    for bug, name in BUGS.items():
        r = explore(
            spec(name),
            range(n),
            target=Target("detloop.examples.kv_sim", name),
            jobs=jobs,
            artifacts_dir=None,
        )
        failing = r.failures
        dists = []
        for start in range(n):
            nxt = next((f for f in failing if f >= start), None)
            if nxt is not None:
                dists.append(nxt - start + 1)
        sigs = r.summary()["signatures"]
        out[bug] = {
            "failing_seeds": len(failing),
            "failure_rate": round(len(failing) / n, 3),
            "first_failing_seed": failing[0] if failing else None,
            "signatures": {k: len(v) for k, v in sigs.items()},
            "seeds_to_first_failure": {
                "median": statistics.median(dists) if dists else None,
                "p90": sorted(dists)[int(0.9 * (len(dists) - 1))] if dists else None,
                "max": max(dists) if dists else None,
                "method": "for every start seed s in the range, number of seeds tried until the next failing seed",
            },
            "wall_seconds": round(r.wall_seconds, 2),
        }
    r = explore(
        spec("test_kv_correct"),
        range(n),
        target=Target("detloop.examples.kv_sim", "test_kv_correct"),
        jobs=jobs,
        artifacts_dir=None,
    )
    out["corrected_baseline"] = {
        "seeds": n,
        "passed": len(r.passed),
        "failed": len(r.failures),
        "incomplete": len(r.incomplete),
    }
    return out


def bench_shrink(quick: bool, corpus: dict[str, Any] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for bug, name in BUGS.items():
        seed = (corpus or {}).get(bug, {}).get("first_failing_seed")
        if seed is None:
            s = spec(name)
            seed = next(
                x for x in range(500) if detloop.run(s.fn, seed=x, config=s.config).status != "pass"
            )
        sr = shrink(spec(name), seed, max_runs=40 if quick else 150, max_seconds=600)
        out[bug] = {"seed": seed, **sr.metrics()}
    return out


def _baseline_trial(args: tuple[str, int]) -> dict[str, Any]:
    bug, seed = args
    cmd = [
        sys.executable,
        os.path.join(HERE, "baseline_real_asyncio.py"),
        "--bug",
        bug,
        "--trials",
        "1",
        "--seed-start",
        str(seed),
    ]
    p = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=300)
    line = [ln for ln in p.stdout.splitlines() if ln.startswith("{")]
    return json.loads(line[-1]) if line else {"bug": bug, "seed": seed, "error": p.stderr[-500:]}


def bench_baseline(quick: bool) -> dict[str, Any]:
    trials = 6 if quick else 20
    out: dict[str, Any] = {"trials_per_variant": trials, "workers": min(6, os.cpu_count() or 2)}
    work = [(bug, s) for bug in ("none", *BUGS) for s in range(trials)]
    with ProcessPoolExecutor(out["workers"]) as ex:
        results = list(ex.map(_baseline_trial, work))
    for bug in ("none", *BUGS):
        rs = [r for r in results if (r.get("bug") or "none") == bug and "error" not in r]
        errors = [r for r in results if (r.get("bug") or "none") == bug and "error" in r]
        out[bug] = {
            "trials": len(rs),
            "errors": len(errors),
            "detected": sum(r["detected"] for r in rs),
            "detection_rate": round(sum(r["detected"] for r in rs) / len(rs), 3) if rs else None,
            "mean_wall_seconds_per_trial": round(statistics.mean(r["wall_seconds"] for r in rs), 3)
            if rs
            else None,
        }
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--only", default="throughput,trace,memory,corpus,shrink,baseline")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    sections = args.only.split(",")
    report: dict[str, Any] = {"machine": machine(), "quick": args.quick, "results": {}}
    res = report["results"]
    for name in sections:
        print(f"== {name}", file=sys.stderr, flush=True)
        t = time.perf_counter()
        if name == "throughput":
            res[name] = bench_throughput(args.quick)
        elif name == "trace":
            res[name] = bench_trace(args.quick)
        elif name == "memory":
            res[name] = bench_memory(args.quick)
        elif name == "corpus":
            res[name] = bench_corpus(args.quick)
        elif name == "shrink":
            res[name] = bench_shrink(args.quick, res.get("corpus"))
        elif name == "baseline":
            res[name] = bench_baseline(args.quick)
        res[name]["section_seconds"] = round(time.perf_counter() - t, 1)
        print(json.dumps(res[name], indent=2), file=sys.stderr, flush=True)
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    m = report["machine"]
    path = args.out or os.path.join(
        HERE,
        "results",
        f"{m['date'][:10]}-py{m['python']}-{sys.platform}{'-quick' if args.quick else ''}.json",
    )
    with open(path, "w") as fh:
        json.dump(report, fh, indent=2, sort_keys=True)
        fh.write("\n")
    print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
