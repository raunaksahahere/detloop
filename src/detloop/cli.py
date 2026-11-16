"""Command-line interface: ``detloop explore|replay|shrink|report``.

Exit codes (stable; see docs/CLI.md):

====  ===========================================================
0     success: every seed passed / replay passed / shrink done
1     at least one seed failed (or the replayed run failed)
2     usage, configuration or target error
3     no failures, but some runs were incomplete (budget/unknown)
4     replay mismatch, divergence, or failure not reproduced
5     internal detloop error
130   interrupted
====  ===========================================================
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from typing import Any

from ._version import __version__
from .errors import ConfigError, DetloopError, ReplayError, TargetError

EXIT_OK, EXIT_FAIL, EXIT_USAGE, EXIT_INCOMPLETE, EXIT_REPLAY, EXIT_INTERNAL, EXIT_INTERRUPT = 0, 1, 2, 3, 4, 5, 130


def _parse_set(items: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for item in items or []:
        if "=" not in item:
            raise ConfigError(f"--set expects KEY=VALUE, got {item!r}")
        key, raw = item.split("=", 1)
        try:
            value: Any = json.loads(raw)
        except json.JSONDecodeError:
            value = raw
        out[key.strip()] = value
    return out


def _eprint(*args: Any) -> None:
    print(*args, file=sys.stderr, flush=True)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="detloop",
        description="Deterministic simulation testing for Python asyncio services.",
        epilog="Exit codes: 0 ok, 1 failures, 2 usage error, 3 incomplete, 4 replay mismatch, "
        "5 internal error, 130 interrupted. Docs: https://github.com/raunaksahahere/detloop",
    )
    p.add_argument("--version", action="version", version=f"detloop {__version__}")
    sub = p.add_subparsers(dest="command", metavar="COMMAND")

    e = sub.add_parser("explore", help="run a simulation test across many seeds",
                       description="Run @detloop.simulate tests for a range of seeds, serially or in parallel.")
    e.add_argument("target", help="path/to/file.py[::test] or package.module[::test]")
    e.add_argument("--seeds", type=int, default=None, help="number of seeds (default: the decorator's 'seeds')")
    e.add_argument("--seed-start", type=int, default=None, help="first seed (default: the decorator's 'seed_start')")
    e.add_argument("-j", "--jobs", type=int, default=1, help="worker processes (1 = in-process, default)")
    e.add_argument("--max-failures", type=int, default=None, metavar="K",
                   help="stop after the K lowest failing seeds are known (deterministic cutoff)")
    e.add_argument("--faults", default=None, help="override fault kinds, e.g. 'partitions,crashes' or 'none'")
    e.add_argument("--scheduler", choices=["interleave", "fifo", "relaxed"], default=None)
    e.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                   help="override any config field, e.g. --set max_virtual_time=60 --set network.rto=0.1")
    e.add_argument("--wall-timeout", type=float, default=None, help="supervisory wall-clock seconds per seed")
    e.add_argument("--artifacts", default=".detloop/artifacts", help="directory for failure traces/manifests")
    e.add_argument("--db", default=".detloop/results.sqlite", help="SQLite results database")
    e.add_argument("--no-db", action="store_true", help="do not write the results database")
    e.add_argument("--shrink", action="store_true", help="shrink the canonical first failure")
    e.add_argument("--hash-seed", default="0", help="PYTHONHASHSEED for worker processes (default 0)")
    e.add_argument("--fresh-process", action="store_true", help="use a new worker process for every seed")
    e.add_argument("--verify-determinism", type=int, default=0, metavar="N",
                   help="re-run the first N seeds in fresh processes with other hash seeds and compare trace hashes")
    e.add_argument("--json", action="store_true", help="print a machine-readable JSON summary to stdout")
    e.add_argument("--summary-file", default=None, help="also write the JSON summary to this file")
    e.add_argument("-q", "--quiet", action="store_true", help="only print the final summary")

    r = sub.add_parser("replay", help="replay a saved failure from its manifest")
    r.add_argument("manifest", help="manifest.json written by explore/pytest/shrink")
    r.add_argument("--allow-mismatch", action="store_true", help="replay even if the test module changed")
    r.add_argument("--trace-level", choices=["off", "basic", "full"], default=None,
                   help="record a trace at a different level (the trace hash is then not compared)")
    r.add_argument("--out", default=None, help="write the replayed trace and manifest to this directory")
    r.add_argument("--expect-reproduce", action="store_true",
                   help="exit 0 only if the failure signature and trace hash are reproduced, else 4")
    r.add_argument("--json", action="store_true")

    s = sub.add_parser("shrink", help="minimize a failing run")
    s.add_argument("source", help="manifest.json, or a target together with --seed")
    s.add_argument("--seed", type=int, default=None)
    s.add_argument("--max-runs", type=int, default=300, help="candidate-run budget (default 300)")
    s.add_argument("--max-seconds", type=float, default=300.0, help="wall-clock budget (default 300)")
    s.add_argument("--artifacts", default=".detloop/artifacts")
    s.add_argument("--json", action="store_true")
    s.add_argument("-q", "--quiet", action="store_true")

    rp = sub.add_parser("report", help="render an offline HTML report")
    rp.add_argument("input", help="manifest.json, trace.jsonl, or results.sqlite")
    rp.add_argument("-o", "--output", default="detloop-report.html")

    h = sub.add_parser("hashes", help=argparse.SUPPRESS)
    h.add_argument("target")
    h.add_argument("--seeds", type=int, required=True)
    h.add_argument("--seed-start", type=int, default=0)
    h.add_argument("--config", required=True)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return EXIT_USAGE
    try:
        if args.command == "explore":
            return cmd_explore(args)
        if args.command == "replay":
            return cmd_replay(args)
        if args.command == "shrink":
            return cmd_shrink(args)
        if args.command == "report":
            return cmd_report(args)
        if args.command == "hashes":
            return cmd_hashes(args)
    except KeyboardInterrupt:
        _eprint("detloop: interrupted")
        return EXIT_INTERRUPT
    except (TargetError, ConfigError) as exc:
        _eprint(f"detloop: error: {exc}")
        return EXIT_USAGE
    except ReplayError as exc:
        _eprint(f"detloop: replay error: {exc}")
        return EXIT_REPLAY
    except DetloopError as exc:
        _eprint(f"detloop: internal error: {type(exc).__name__}: {exc}")
        return EXIT_INTERNAL
    parser.print_help()
    return EXIT_USAGE


# ---------------------------------------------------------------------------
def _explore_config(spec: Any, args: argparse.Namespace) -> Any:
    overrides = _parse_set(args.set)
    if args.faults is not None:
        overrides["faults"] = args.faults
    if args.scheduler:
        overrides["scheduler"] = args.scheduler
    if args.wall_timeout is not None:
        overrides["budgets.wall_timeout"] = args.wall_timeout
    return spec.config.with_overrides(**overrides) if overrides else spec.config


def cmd_explore(args: argparse.Namespace) -> int:
    from . import targets
    from .explore import explore

    if args.jobs < 1:
        raise ConfigError("--jobs must be >= 1")
    found = targets.discover(args.target)
    summaries = []
    worst = EXIT_OK
    for target, spec in found:
        if spec.extra_params:
            _eprint(f"detloop: skipping {target}: it takes extra parameters {spec.extra_params} (pytest-only)")
            continue
        cfg = _explore_config(spec, args)
        seeds = spec.seed_range(args.seeds, args.seed_start)
        if not args.quiet:
            _eprint(f"detloop: exploring {target} seeds {seeds.start}..{seeds.stop - 1} with {args.jobs} job(s)")

        def progress(summary: dict[str, Any], done: int, total: int) -> None:
            if args.quiet:
                return
            if summary["status"] != "pass":
                _eprint(f"  seed {summary['seed']}: {summary['status']} {summary['signature']}: {summary['message'][:200]}")
            if done % max(1, total // 10) == 0 or done == total:
                _eprint(f"  {done}/{total} seeds done")

        result = explore(
            spec, seeds, target=target, config=cfg, jobs=args.jobs, max_failures=args.max_failures,
            artifacts_dir=args.artifacts, db_path=None if args.no_db else args.db,
            hash_seed=args.hash_seed, fresh_process=args.fresh_process, progress=progress,
        )
        summary = result.summary()
        if args.shrink and result.first_failure is not None and result.first_failure in result.artifacts:
            summary["shrink"] = _shrink_manifest(result.artifacts[result.first_failure], args.artifacts,
                                                 300, 300.0, args.quiet)
        if args.verify_determinism:
            summary["determinism"] = _verify_determinism(target, cfg, result, args.verify_determinism, args.quiet)
            if not summary["determinism"]["consistent"]:
                summary["exit_code"] = max(summary["exit_code"], EXIT_FAIL)
        summaries.append(summary)
        worst = max(worst, summary["exit_code"]) if summary["exit_code"] != EXIT_INTERRUPT else EXIT_INTERRUPT
        if not args.quiet or not args.json:
            _print_summary(summary)
        if result.interrupted:
            break
    out = summaries[0] if len(summaries) == 1 else {"tests": summaries, "exit_code": worst}
    if args.json:
        print(json.dumps(out, indent=2, sort_keys=True))
    if args.summary_file:
        from .artifacts import atomic_write

        atomic_write(args.summary_file, json.dumps(out, indent=2, sort_keys=True) + "\n")
    return worst


def _print_summary(s: dict[str, Any]) -> None:
    _eprint(
        f"detloop: {s['test']}: {s['seeds_completed']}/{s['seeds_requested']} seeds, "
        f"{s['passed']} passed, {s['failed']} failed, {s['incomplete']} incomplete, {s['errors']} errors "
        f"in {s['wall_seconds']}s ({s['seeds_per_second']} seeds/s)"
    )
    if s["first_failure"] is not None:
        _eprint(f"  canonical first failure: seed {s['first_failure']} "
                f"(first discovered: seed {s['first_failure_discovered']})")
        for sig, seeds in s["signatures"].items():
            shown = ", ".join(map(str, seeds[:10])) + (" ..." if len(seeds) > 10 else "")
            _eprint(f"  {sig}: {len(seeds)} seed(s): {shown}")
        path = s["artifacts"].get(str(s["first_failure"]))
        if path:
            _eprint(f"  replay: detloop replay {path}")
            _eprint(f"  report: detloop report {path} -o detloop-report.html")
    if s.get("shrink"):
        sh = s["shrink"]
        _eprint(f"  shrunk: {sh.get('manifest')} ({sh.get('status')})")
    if s.get("determinism"):
        d = s["determinism"]
        _eprint(f"  determinism check: {'consistent' if d['consistent'] else 'MISMATCH'} over {d['seeds']} seeds "
                f"(hash seeds {d['hash_seeds']})")


def _verify_determinism(target: Any, cfg: Any, result: Any, n: int, quiet: bool) -> dict[str, Any]:
    seeds = sorted(result.results)[:n]
    if not seeds:
        return {"consistent": True, "seeds": 0, "hash_seeds": [], "mismatches": []}
    expected = {s: result.results[s]["trace_sha256"] for s in seeds}
    mismatches = []
    hash_seeds = ["1", "2"]
    for hs in hash_seeds:
        env = dict(os.environ, PYTHONHASHSEED=hs)
        cmd = [sys.executable, "-m", "detloop", "hashes", f"{target.location}::{target.test}",
               "--seeds", str(len(seeds)), "--seed-start", str(seeds[0]), "--config", json.dumps(cfg.to_dict())]
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)  # noqa: S603
        if proc.returncode != 0:
            raise ConfigError(f"determinism check subprocess failed: {proc.stderr[-2000:]}")
        got = {int(k): v for k, v in json.loads(proc.stdout).items()}
        for s in seeds:
            if s in got and got[s] != expected[s]:
                mismatches.append({"seed": s, "hash_seed": hs, "expected": expected[s], "got": got[s]})
    if not quiet and mismatches:
        _eprint(f"  NONDETERMINISM: {len(mismatches)} trace hash mismatch(es); e.g. seed {mismatches[0]['seed']}")
    return {"consistent": not mismatches, "seeds": len(seeds), "hash_seeds": ["0", *hash_seeds],
            "mismatches": mismatches[:20]}


def cmd_hashes(args: argparse.Namespace) -> int:
    from . import targets
    from .config import SimConfig
    from .runner import run

    (target, spec), *_ = targets.discover(args.target)
    cfg = SimConfig.from_dict(json.loads(args.config))
    out = {}
    for seed in range(args.seed_start, args.seed_start + args.seeds):
        out[seed] = run(spec.fn, seed=seed, config=cfg).trace_sha256
    print(json.dumps(out))
    return EXIT_OK


def cmd_replay(args: argparse.Namespace) -> int:
    from .artifacts import build_manifest, write_run_artifacts
    from .replay import replay_manifest
    from .targets import fingerprint

    report, spec, target = replay_manifest(args.manifest, allow_mismatch=args.allow_mismatch,
                                           trace_level=args.trace_level)
    res = report.result
    if args.out:
        manifest = build_manifest(
            target=target.location, test=spec.qualname, workload_fingerprint=fingerprint(spec),
            config=spec.config if args.trace_level is None else spec.config.with_overrides(trace=args.trace_level),
            result=res, trace_file=None, faults_explicit=True, include_decisions=report.mode == "strict",
        )
        path = write_run_artifacts(args.out, manifest, res.trace_lines)
        _eprint(f"detloop: wrote {path}")
    if args.json:
        print(json.dumps({
            **res.summary(), "reproduced": report.reproduced, "identical_trace": report.identical_trace,
            "expected_signature": report.expected_signature, "mode": report.mode, "warnings": report.warnings,
        }, indent=2, sort_keys=True))
    else:
        print(report.describe())
        if res.status != "pass":
            print(f"  {res.message}")
            if res.traceback:
                print("  traceback (most recent call last, abridged):")
                print("    " + "\n    ".join(res.traceback.rstrip().splitlines()[-12:]))
    if args.expect_reproduce:
        return EXIT_OK if report.reproduced and report.identical_trace else EXIT_REPLAY
    if res.status == "pass":
        return EXIT_OK
    if res.status == "incomplete":
        return EXIT_INCOMPLETE
    if res.status == "error":
        return EXIT_INTERNAL
    return EXIT_FAIL


def _shrink_manifest(path: str, artifacts: str, max_runs: int, max_seconds: float, quiet: bool) -> dict[str, Any]:
    from .artifacts import manifest_config, read_manifest
    from .explore import save_failure
    from .faults import schedule_from_json
    from .replay import check_envelope, resolve_manifest_target
    from .rng import Tape
    from .shrink import shrink
    from .targets import fingerprint

    manifest = read_manifest(path)
    target, spec = resolve_manifest_target(manifest, os.path.dirname(os.path.abspath(path)))
    check_envelope(manifest, spec)
    cfg = manifest_config(manifest)
    faults = schedule_from_json(manifest["faults"]) if manifest.get("faults_explicit") else None
    tapes = None
    if manifest.get("decisions"):
        tapes = {s: Tape.from_json(t) for s, t in manifest["decisions"]["tapes"].items()}
    log = None if quiet else (lambda m: _eprint(f"  shrink: {m}"))
    sr = shrink(spec, int(manifest["seed"]), config=cfg, faults=faults, tapes=tapes,
                max_runs=max_runs, max_seconds=max_seconds, log=log)
    metrics = sr.metrics()
    out = save_failure(sr.final, spec, sr.config or cfg, target, artifacts, fingerprint(spec), suffix="-shrunk",
                       faults_explicit=True, include_decisions=True, shrink=metrics)
    metrics["manifest"] = out
    return metrics


def cmd_shrink(args: argparse.Namespace) -> int:
    source = args.source
    if source.endswith(".json"):
        manifest_path = source
    else:
        if args.seed is None:
            raise ConfigError("shrinking a target requires --seed")
        from . import targets
        from .explore import save_failure
        from .runner import run

        (target, spec), *_ = targets.discover(source)
        res = run(spec.fn, seed=args.seed, config=spec.config, keep_trace=True)
        if res.status == "pass":
            _eprint(f"detloop: seed {args.seed} passes; nothing to shrink")
            return EXIT_REPLAY
        manifest_path = save_failure(res, spec, spec.config, target, args.artifacts, targets.fingerprint(spec))
    metrics = _shrink_manifest(manifest_path, args.artifacts, args.max_runs, args.max_seconds, args.quiet)
    if args.json:
        print(json.dumps(metrics, indent=2, sort_keys=True))
    else:
        print(f"shrunk {metrics['signature']} ({metrics['status']}, {metrics['runs']} runs, {metrics['seconds']}s)")
        for key in ("faults", "nonzero_decisions", "decisions", "steps", "trace_events", "virtual_time"):
            a, b = metrics[key]
            print(f"  {key:18} {a} -> {b}")
        print(f"  manifest: {metrics['manifest']}")
        print(f"  replay:   detloop replay {metrics['manifest']}")
    return EXIT_OK


def cmd_report(args: argparse.Namespace) -> int:
    from .report import render_report

    path = render_report(args.input, args.output)
    _eprint(f"detloop: wrote {path}")
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
