"""Replaying a saved failure from its manifest.

The replay envelope: a manifest replays faithfully only with the same code
(workload fingerprint), the same configuration (taken from the manifest), and
a compatible runtime. Mismatches are rejected (code) or flagged (runtime,
detloop version). With recorded decision tapes, replay is *strict*: any
divergence raises :class:`ReplayDivergence` rather than silently choosing a
different action. Without tapes, the run is regenerated from the seed and its
trace hash is compared with the recorded one.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from typing import Any

from ._version import __version__
from .api import Spec
from .artifacts import manifest_config, read_manifest
from .errors import ReplayMismatch
from .faults import schedule_from_json
from .rng import Tape
from .runner import RunResult, run
from .targets import Target, fingerprint, load


@dataclass
class ReplayReport:
    result: RunResult
    reproduced: bool
    identical_trace: bool
    expected_signature: str
    expected_trace: str
    warnings: list[str] = field(default_factory=list)
    mode: str = "seeded"

    def describe(self) -> str:
        lines = [
            f"replayed seed {self.result.seed} ({self.mode} decisions): "
            f"{self.result.status} {self.result.signature}",
            f"  expected signature: {self.expected_signature}",
            "  REPRODUCED: same failure signature" if self.reproduced else "  NOT REPRODUCED",
            f"  trace: {'identical' if self.identical_trace else 'differs'} "
            f"({self.result.trace_sha256[:16]} vs {self.expected_trace[:16]})",
        ]
        lines += [f"  warning: {w}" for w in self.warnings]
        return "\n".join(lines)


def resolve_manifest_target(
    manifest: dict[str, Any], base_dir: str | None = None
) -> tuple[Target, Spec]:
    loc = manifest["target"]["location"]
    test = manifest["target"]["test"]
    candidates = [loc]
    if base_dir and not os.path.isabs(loc):
        candidates.append(os.path.join(base_dir, loc))
    last: Exception | None = None
    for c in candidates:
        try:
            target = Target(c, test)
            return target, load(target)
        except Exception as exc:
            last = exc
    raise ReplayMismatch(f"cannot load target {loc}::{test}: {last}")


def check_envelope(
    manifest: dict[str, Any], spec: Spec, *, allow_mismatch: bool = False
) -> list[str]:
    warnings: list[str] = []
    meta = manifest.get("metadata", {})
    recorded = manifest["target"].get("workload_fingerprint") or ""
    current = fingerprint(spec)
    if recorded and current and recorded != current:
        msg = (
            "the test module changed since the manifest was recorded "
            f"(fingerprint {recorded[:12]} -> {current[:12]})"
        )
        if not allow_mismatch:
            raise ReplayMismatch(msg + "; pass --allow-mismatch to replay anyway")
        warnings.append(msg)
    if meta.get("detloop") and meta["detloop"] != __version__:
        warnings.append(f"recorded with detloop {meta['detloop']}, replaying with {__version__}")
    py = ".".join(map(str, sys.version_info[:3]))
    if meta.get("python") and meta["python"].rsplit(".", 1)[0] != py.rsplit(".", 1)[0]:
        warnings.append(
            f"recorded on Python {meta['python']}, replaying on {py}: asyncio internals "
            "differ between "
            "minor versions, so byte-identical traces are not guaranteed"
        )
    hs = meta.get("hash_seed")
    if hs is not None and os.environ.get("PYTHONHASHSEED") != hs:
        warnings.append(
            f"recorded with PYTHONHASHSEED={hs}; current process uses "
            f"{os.environ.get('PYTHONHASHSEED', 'random')}. Code that iterates sets of "
            "strings may diverge"
        )
    return warnings


def replay_manifest(
    path: str,
    *,
    allow_mismatch: bool = False,
    keep_trace: bool = True,
    trace_level: str | None = None,
) -> tuple[ReplayReport, Spec, Target]:
    manifest = read_manifest(path)
    target, spec = resolve_manifest_target(manifest, os.path.dirname(os.path.abspath(path)))
    warnings = check_envelope(manifest, spec, allow_mismatch=allow_mismatch)
    report = replay_with(manifest, spec, keep_trace=keep_trace, trace_level=trace_level)
    report.warnings[:0] = warnings
    return report, spec, target


def replay_with(
    manifest: dict[str, Any],
    spec: Spec,
    *,
    keep_trace: bool = True,
    trace_level: str | None = None,
    args: tuple[Any, ...] = (),
    kwargs: dict[str, Any] | None = None,
) -> ReplayReport:
    cfg = manifest_config(manifest)
    faults = schedule_from_json(manifest["faults"]) if manifest.get("faults_explicit") else None
    decisions = manifest.get("decisions")
    tapes = None
    mode = "seeded"
    if decisions:
        tapes = {s: Tape.from_json(t) for s, t in decisions["tapes"].items()}
        mode = "strict"
    result = run(
        spec.fn,
        seed=int(manifest["seed"]),
        config=cfg,
        faults=faults,
        tapes=tapes,
        decision_mode=mode,
        keep_trace=keep_trace,
        trace_level=trace_level,
        args=args,
        kwargs=kwargs,
    )
    expected = manifest["outcome"]["signature"]
    level_changed = trace_level is not None and trace_level != cfg.trace
    exp_trace = manifest["trace"]["sha256"]
    return ReplayReport(
        result=result,
        reproduced=result.signature == expected,
        identical_trace=(not level_changed) and result.trace_sha256 == exp_trace,
        expected_signature=expected,
        expected_trace=exp_trace,
        mode=mode,
    )
