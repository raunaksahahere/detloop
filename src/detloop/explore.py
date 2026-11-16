"""Seed exploration: serial or across worker processes.

Determinism rules (see docs/DETERMINISM.md):

* A seed's behaviour depends only on (code, config, seed). Workers run seeds
  in fresh :class:`World` objects; worker count and completion order never
  change a seed's result.
* Results are aggregated by seed. The *canonical* first failure is the
  lowest failing seed; the *discovered* first failure (completion order) is
  reported as metadata.
* With ``max_failures=k``, dispatch stops above the k-th lowest failing seed
  found so far, but every lower seed is still run. Results for seeds above the
  final cutoff are discarded, so the reported set is identical to a serial
  run's regardless of ``jobs``.
* Worker processes are started with the ``spawn`` method and a fixed
  ``PYTHONHASHSEED`` (default ``0``) recorded in the results.
* The parent enforces a hard wall-clock deadline per seed and kills a worker
  that overruns (classified ``incomplete``/``wall_timeout``).
"""

from __future__ import annotations

import contextlib
import multiprocessing
import os
import time
import traceback
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from multiprocessing.connection import Connection, wait
from typing import Any

from .api import Spec
from .artifacts import ResultsDB, build_manifest, safe_name, write_run_artifacts
from .config import SimConfig
from .runner import ERROR, INCOMPLETE, PASS, RunResult, run
from .targets import Target, fingerprint, load

_perf = time.perf_counter


@dataclass
class ExploreResult:
    target: str
    test: str
    seeds: list[int]
    results: dict[int, dict[str, Any]] = field(default_factory=dict)
    discovered: list[int] = field(default_factory=list)
    discarded: list[int] = field(default_factory=list)
    artifacts: dict[int, str] = field(default_factory=dict)
    interrupted: bool = False
    wall_seconds: float = 0.0
    jobs: int = 1
    db_path: str | None = None
    exploration_id: int | None = None

    def by_status(self, status: str) -> list[int]:
        return sorted(s for s, r in self.results.items() if r["status"] == status)

    @property
    def failures(self) -> list[int]:
        return self.by_status("fail")

    @property
    def incomplete(self) -> list[int]:
        return self.by_status(INCOMPLETE)

    @property
    def errors(self) -> list[int]:
        return self.by_status(ERROR)

    @property
    def passed(self) -> list[int]:
        return self.by_status(PASS)

    @property
    def first_failure(self) -> int | None:
        """Canonical first failure: the lowest failing (non-pass) seed."""
        bad = [s for s, r in self.results.items() if r["status"] != PASS]
        return min(bad) if bad else None

    @property
    def first_failure_discovered(self) -> int | None:
        for s in self.discovered:
            r = self.results.get(s)
            if r is not None and r["status"] != PASS:
                return s
        return None

    def exit_code(self) -> int:
        if self.interrupted:
            return 130
        if self.errors:
            return 5
        if self.failures:
            return 1
        if self.incomplete:
            return 3
        return 0

    def summary(self) -> dict[str, Any]:
        completed = len(self.results)
        signatures: dict[str, list[int]] = {}
        for s in sorted(self.results):
            r = self.results[s]
            if r["status"] != PASS:
                signatures.setdefault(r["signature"], []).append(s)
        return {
            "target": self.target,
            "test": self.test,
            "seeds_requested": len(self.seeds),
            "seeds_completed": completed,
            "passed": len(self.passed),
            "failed": len(self.failures),
            "incomplete": len(self.incomplete),
            "errors": len(self.errors),
            "first_failure": self.first_failure,
            "first_failure_discovered": self.first_failure_discovered,
            "signatures": signatures,
            "artifacts": {str(k): v for k, v in sorted(self.artifacts.items())},
            "interrupted": self.interrupted,
            "discarded_beyond_cutoff": len(self.discarded),
            "jobs": self.jobs,
            "wall_seconds": round(self.wall_seconds, 3),
            "seeds_per_second": round(completed / self.wall_seconds, 2)
            if self.wall_seconds
            else None,
            "db": self.db_path,
            "exploration_id": self.exploration_id,
            "exit_code": self.exit_code(),
        }


def run_seed(
    spec: Spec,
    config: SimConfig,
    seed: int,
    *,
    target: Target | None,
    artifacts_dir: str | None,
    workload_fp: str,
) -> tuple[dict[str, Any], str | None]:
    """Run one seed and write artifacts for failures. Returns (summary, manifest path)."""
    result = run(spec.fn, seed=seed, config=config, keep_trace=True)
    path = None
    if result.status != PASS and artifacts_dir:
        path = save_failure(result, spec, config, target, artifacts_dir, workload_fp)
    return result.summary(), path


def save_failure(
    result: RunResult,
    spec: Spec,
    config: SimConfig,
    target: Target | None,
    artifacts_dir: str,
    workload_fp: str,
    *,
    suffix: str = "",
    faults_explicit: bool = False,
    include_decisions: bool = False,
    shrink: dict[str, Any] | None = None,
) -> str:
    location = target.location if target is not None else spec.module
    directory = os.path.join(artifacts_dir, safe_name(spec.qualname), f"seed-{result.seed}{suffix}")
    manifest = build_manifest(
        target=location,
        test=spec.qualname,
        workload_fingerprint=workload_fp,
        config=config,
        result=result,
        trace_file=None,
        faults_explicit=faults_explicit,
        include_decisions=include_decisions,
        shrink=shrink,
    )
    return write_run_artifacts(directory, manifest, result.trace_lines)


def _worker_main(
    conn: Connection, target: Target, config_dict: dict[str, Any], artifacts_dir: str | None
) -> None:
    try:
        spec = load(target)
        config = SimConfig.from_dict(config_dict)
        workload_fp = fingerprint(spec)
    except BaseException as exc:
        conn.send(("fatal", f"{type(exc).__name__}: {exc}"))
        return
    conn.send(("ready", None))
    while True:
        try:
            msg = conn.recv()
        except (EOFError, KeyboardInterrupt):
            return
        if msg is None:
            return
        seed = int(msg)
        try:
            summary, path = run_seed(
                spec,
                config,
                seed,
                target=target,
                artifacts_dir=artifacts_dir,
                workload_fp=workload_fp,
            )
            conn.send(("result", (summary, path)))
        except KeyboardInterrupt:
            return
        except BaseException as exc:
            conn.send(("error", (seed, f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}")))


def _error_summary(seed: int, kind: str, message: str, status: str = ERROR) -> dict[str, Any]:
    return {
        "seed": seed,
        "status": status,
        "kind": kind,
        "signature": kind,
        "message": message,
        "steps": 0,
        "virtual_time": 0.0,
        "trace_sha256": "",
        "trace_events": 0,
        "faults": 0,
        "decisions": 0,
        "wall_seconds": 0.0,
    }


class _Cutoff:
    def __init__(self, max_failures: int | None) -> None:
        self.max_failures = max_failures
        self.failing: list[int] = []

    def note(self, summary: dict[str, Any]) -> None:
        if self.max_failures and summary["status"] != PASS:
            self.failing.append(summary["seed"])
            self.failing.sort()

    @property
    def limit(self) -> int | None:
        if self.max_failures and len(self.failing) >= self.max_failures:
            return self.failing[self.max_failures - 1]
        return None


def explore(
    spec: Spec,
    seeds: Iterable[int],
    *,
    target: Target | None = None,
    config: SimConfig | None = None,
    jobs: int = 1,
    max_failures: int | None = None,
    artifacts_dir: str | None = ".detloop/artifacts",
    db_path: str | None = None,
    hash_seed: str = "0",
    fresh_process: bool = False,
    progress: Callable[[dict[str, Any], int, int], None] | None = None,
) -> ExploreResult:
    """Run ``spec`` for every seed. ``jobs > 1`` needs a ``target`` that worker
    processes can import (``Target(location, test)``)."""
    cfg = config or spec.config
    seed_list = sorted(set(seeds))
    if any(s < 0 for s in seed_list):
        raise ValueError("seeds must be non-negative")
    result = ExploreResult(
        target=target.location if target else spec.module,
        test=spec.qualname,
        seeds=seed_list,
        jobs=jobs,
        db_path=db_path,
    )
    db = ResultsDB(db_path) if db_path else None
    workload_fp = fingerprint(spec)
    if db is not None and seed_list:
        span = range(seed_list[0], seed_list[-1] + 1)
        result.exploration_id = db.start(
            target=result.target,
            test=spec.qualname,
            fingerprint=workload_fp,
            config=cfg,
            seeds=span,
            jobs=jobs,
        )
    cutoff = _Cutoff(max_failures)
    started = _perf()
    order = 0

    def record(summary: dict[str, Any], path: str | None) -> None:
        nonlocal order
        order += 1
        seed = summary["seed"]
        result.results[seed] = summary
        result.discovered.append(seed)
        if path:
            result.artifacts[seed] = path
        cutoff.note(summary)
        if db is not None and result.exploration_id is not None:
            db.add_run(result.exploration_id, summary, order, path)
            if order % 50 == 0:
                db.commit()
        if progress is not None:
            progress(summary, len(result.results), len(seed_list))

    try:
        if jobs <= 1:
            for seed in seed_list:
                limit = cutoff.limit
                if limit is not None and seed > limit:
                    break
                summary, path = run_seed(
                    spec,
                    cfg,
                    seed,
                    target=target,
                    artifacts_dir=artifacts_dir,
                    workload_fp=workload_fp,
                )
                record(summary, path)
        else:
            if target is None:
                raise ValueError("parallel exploration requires an importable target")
            _parallel(
                target,
                cfg,
                seed_list,
                jobs,
                artifacts_dir,
                hash_seed,
                fresh_process,
                cutoff,
                record,
            )
    except KeyboardInterrupt:
        result.interrupted = True
    finally:
        limit = cutoff.limit
        if limit is not None:
            for s in sorted(result.results):
                if s > limit:
                    result.discarded.append(s)
                    del result.results[s]
                    result.artifacts.pop(s, None)
        result.wall_seconds = _perf() - started
        if db is not None:
            if result.exploration_id is not None:
                db.finish(
                    result.exploration_id, "interrupted" if result.interrupted else "finished"
                )
            db.close()
    return result


class _Worker:
    def __init__(
        self,
        ctx: Any,
        target: Target,
        cfg: dict[str, Any],
        artifacts_dir: str | None,
        hash_seed: str,
    ) -> None:
        parent, child = ctx.Pipe()
        old = os.environ.get("PYTHONHASHSEED")
        os.environ["PYTHONHASHSEED"] = hash_seed
        try:
            self.proc = ctx.Process(
                target=_worker_main, args=(child, target, cfg, artifacts_dir), daemon=True
            )
            self.proc.start()
        finally:
            if old is None:
                os.environ.pop("PYTHONHASHSEED", None)
            else:
                os.environ["PYTHONHASHSEED"] = old
        child.close()
        self.conn: Connection = parent
        self.seed: int | None = None
        self.started = 0.0
        self.ready = False
        self.runs = 0

    def send(self, seed: int) -> None:
        self.seed = seed
        self.started = _perf()
        self.conn.send(seed)

    def kill(self) -> None:
        with contextlib.suppress(Exception):
            self.proc.kill()
        self.proc.join(5)
        self.conn.close()

    def stop(self) -> None:
        with contextlib.suppress(OSError, EOFError):
            self.conn.send(None)
        self.proc.join(5)
        if self.proc.is_alive():
            self.kill()
        else:
            self.conn.close()


def _parallel(
    target: Target,
    cfg: SimConfig,
    seeds: list[int],
    jobs: int,
    artifacts_dir: str | None,
    hash_seed: str,
    fresh_process: bool,
    cutoff: _Cutoff,
    record: Callable[[dict[str, Any], str | None], None],
) -> None:
    ctx = multiprocessing.get_context("spawn")
    cfg_dict = cfg.to_dict()
    soft = cfg.budgets.wall_timeout
    hard = None if soft is None else soft * 1.5 + 10.0
    queue = list(reversed(seeds))
    workers: list[_Worker] = []

    def spawn() -> _Worker:
        return _Worker(ctx, target, cfg_dict, artifacts_dir, hash_seed)

    try:
        workers = [spawn() for _ in range(min(jobs, max(1, len(seeds))))]
        while True:
            limit = cutoff.limit
            for w in workers:
                if w.ready and w.seed is None:
                    while queue and limit is not None and queue[-1] > limit:
                        queue.clear()
                    if queue:
                        w.send(queue.pop())
            busy = [w for w in workers if w.seed is not None or not w.ready]
            if not busy and not queue:
                break
            if not busy:
                break
            ready = wait([w.conn for w in busy], timeout=0.25)
            now = _perf()
            for w in list(workers):
                if w.conn in ready:
                    try:
                        kind, payload = w.conn.recv()
                    except (EOFError, OSError):
                        seed = w.seed
                        w.kill()
                        workers.remove(w)
                        if seed is not None:
                            record(
                                _error_summary(
                                    seed,
                                    "worker_crashed",
                                    "worker process died while running this seed",
                                ),
                                None,
                            )
                        elif not w.ready:
                            raise RuntimeError("worker process failed to start") from None
                        workers.append(spawn())
                        continue
                    if kind == "fatal":
                        raise RuntimeError(f"worker failed to load target {target}: {payload}")
                    if kind == "ready":
                        w.ready = True
                        continue
                    if kind == "result":
                        summary, path = payload
                        w.seed = None
                        w.runs += 1
                        record(summary, path)
                    elif kind == "error":
                        seed, text = payload
                        w.seed = None
                        record(_error_summary(seed, "internal_error", text), None)
                    if fresh_process and w.seed is None:
                        w.stop()
                        workers.remove(w)
                        workers.append(spawn())
                elif hard is not None and w.seed is not None and now - w.started > hard:
                    seed = w.seed
                    w.kill()
                    workers.remove(w)
                    record(
                        _error_summary(
                            seed,
                            "wall_timeout",
                            f"worker exceeded hard wall-clock deadline of {hard:g}s and was killed",
                            status=INCOMPLETE,
                        ),
                        None,
                    )
                    workers.append(spawn())
    finally:
        for w in workers:
            if w.seed is not None:
                w.kill()
            else:
                w.stop()
