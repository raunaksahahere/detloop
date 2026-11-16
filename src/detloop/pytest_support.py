"""Running a :class:`Spec` from a test function (pytest or plain call)."""

from __future__ import annotations

import os
import shlex
from dataclasses import dataclass
from typing import Any

from .api import Spec
from .artifacts import read_manifest
from .errors import SimulationFailure
from .explore import save_failure
from .runner import PASS, RunResult, run
from .targets import Target, fingerprint


@dataclass
class Options:
    seed: int | None = None
    seeds: int | None = None
    seed_start: int | None = None
    replay: str | None = None
    artifacts: str | None = ".detloop/artifacts"
    shrink: bool = False
    trace: str | None = None
    nodeid: str | None = None
    rootdir: str | None = None


#: Set by the pytest plugin at configure time; plain calls use defaults.
options = Options()


def _target_for(spec: Spec) -> Target | None:
    import sys

    mod = sys.modules.get(spec.module)
    path = getattr(mod, "__file__", None)
    if not path:
        return None
    rel = os.path.relpath(path) if not os.path.isabs(spec.module) else path
    return Target(rel, spec.qualname)


def _replay_command(spec: Spec, seed: int) -> str:
    nodeid = options.nodeid
    if nodeid:
        return f"pytest {shlex.quote(nodeid)} --detloop-seed={seed}"
    t = _target_for(spec)
    return f"pytest {shlex.quote(str(t))} --detloop-seed={seed}" if t else f"--detloop-seed={seed}"


def _format_failure(spec: Spec, res: RunResult, index: int, total: int, manifest: str | None, extra: str = "") -> str:
    lines = [
        f"detloop: seed {res.seed} {res.status} ({res.signature}) after {index} of {total} seed(s)",
        f"  {res.message}",
        f"  virtual time {res.virtual_ns / 1e9:g}s, {res.steps} steps, {len(res.faults)} scheduled fault(s)",
    ]
    for d in res.details[:10]:
        lines.append(f"    {d}")
    if manifest:
        lines.append(f"  manifest: {manifest}")
        lines.append(f"  trace:    {os.path.join(os.path.dirname(manifest), 'trace.jsonl')}")
    lines.append(f"  replay:   {_replay_command(spec, res.seed)}")
    if manifest:
        lines.append(f"            detloop replay {shlex.quote(manifest)}")
        lines.append(f"  report:   detloop report {shlex.quote(manifest)} -o detloop-report.html")
    if extra:
        lines.append(extra)
    if res.traceback:
        lines.append("  original traceback:")
        lines.extend("    " + ln for ln in res.traceback.rstrip().splitlines()[-25:])
    return "\n".join(lines)


def run_spec_for_pytest(spec: Spec, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
    cfg = spec.config
    if options.trace:
        cfg = cfg.with_overrides(trace=options.trace)
    if options.replay:
        _replay_in_pytest(spec, args, kwargs)
        return
    if options.seed is not None:
        seeds = [options.seed]
    else:
        seeds = list(spec.seed_range(options.seeds, options.seed_start))
    target = _target_for(spec)
    for i, seed in enumerate(seeds, 1):
        res = run(spec.fn, seed=seed, config=cfg, args=args, kwargs=kwargs, keep_trace=True)
        if res.status == PASS:
            continue
        manifest = None
        extra = ""
        if options.artifacts and not args and not kwargs:
            manifest = save_failure(res, spec, cfg, target, options.artifacts, fingerprint(spec))
            if options.shrink:
                extra = _shrink_in_pytest(spec, res, cfg, target)
        elif args or kwargs:
            extra = "  (artifacts are not written for parametrized tests; replay with --detloop-seed)"
        raise SimulationFailure(_format_failure(spec, res, i, len(seeds), manifest, extra))


def _shrink_in_pytest(spec: Spec, res: RunResult, cfg: Any, target: Target | None) -> str:
    from .shrink import shrink

    try:
        sr = shrink(spec, res.seed, config=cfg, max_runs=200, max_seconds=120)
    except Exception as exc:
        return f"  shrink failed: {type(exc).__name__}: {exc}"
    m = sr.metrics()
    path = save_failure(
        sr.final, spec, sr.config or cfg, target, options.artifacts or ".detloop/artifacts", fingerprint(spec),
        suffix="-shrunk", faults_explicit=True, include_decisions=True, shrink=m,
    )
    return (
        f"  shrunk ({m['status']}, {m['runs']} runs): faults {m['faults'][0]}->{m['faults'][1]}, "
        f"non-zero decisions {m['nonzero_decisions'][0]}->{m['nonzero_decisions'][1]}, "
        f"steps {m['steps'][0]}->{m['steps'][1]}\n"
        f"  shrunk manifest: {path}\n"
        f"  replay shrunk:   pytest {shlex.quote(options.nodeid or '')} --detloop-replay={shlex.quote(path)}"
    )


def _replay_in_pytest(spec: Spec, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
    from .replay import check_envelope, replay_with

    assert options.replay is not None
    manifest = read_manifest(options.replay)
    if manifest["target"]["test"] != spec.qualname:
        import pytest

        pytest.skip(f"--detloop-replay manifest is for {manifest['target']['test']}")
    warnings = check_envelope(manifest, spec)
    report = replay_with(manifest, spec, args=args, kwargs=kwargs)
    report.warnings[:0] = warnings
    if report.result.status != PASS:
        raise SimulationFailure(
            _format_failure(spec, report.result, 1, 1, None, report.describe())
        )
    raise SimulationFailure("detloop: replay did not reproduce the failure\n" + report.describe())
