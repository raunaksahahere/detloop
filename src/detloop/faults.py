"""Fault schedules.

A fault schedule is an explicit, ordered list of :class:`Fault` records. Random
schedules are generated *up front* from the ``faults`` stream, so the schedule
of a run is fully visible in its manifest and the shrinker can delete
individual faults. Each fault carries its own recovery (heal / restart /
restore), so removing a fault removes both halves.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .config import FaultConfig
from .errors import ConfigError
from .rng import SplitMix64

NS = 1_000_000_000
SCHEDULED_KINDS = ("partitions", "crashes", "resets", "latency", "clock")


@dataclass(frozen=True)
class Fault:
    """One scheduled fault. ``at``/``duration`` are virtual nanoseconds."""

    id: str
    kind: str  # partition | crash | reset | latency | clock
    at: int
    duration: int = 0
    params: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "at": self.at,
            "duration": self.duration,
            "params": self.params,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Fault:
        try:
            return cls(
                id=str(data["id"]),
                kind=str(data["kind"]),
                at=int(data["at"]),
                duration=int(data.get("duration", 0)),
                params=dict(data.get("params", {})),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ConfigError(f"invalid fault record {data!r}: {exc}") from None

    def describe(self) -> str:
        p = self.params
        at = f"t={self.at / NS:g}s"
        dur = f" for {self.duration / NS:g}s" if self.duration else ""
        if self.kind == "partition":
            if p.get("oneway"):
                return f"{at}: block {p['a']}->{p['b']}{dur}"
            return f"{at}: partition {p['groups'][0]} | {p['groups'][1]}{dur}"
        if self.kind == "crash":
            return f"{at}: crash {p['node']}" + (
                f", restart after {self.duration / NS:g}s" if self.duration else ""
            )
        if self.kind == "reset":
            return f"{at}: reset connections {p['a']}<->{p['b']}"
        if self.kind == "latency":
            return f"{at}: latency x{p['factor']:g} {p['a']}<->{p['b']}{dur}"
        if self.kind == "clock":
            return (
                f"{at}: clock of {p['node']} offset {p['offset_ns'] / NS:+g}s "
                f"drift {p['drift_ppm']:+g}ppm{dur}"
            )
        return f"{at}: {self.kind} {p}{dur}"


def _uniform_ns(gen: SplitMix64, lo: float, hi: float) -> int:
    lo_ns, hi_ns = round(lo * NS), round(hi * NS)
    if hi_ns <= lo_ns:
        return lo_ns
    # millisecond resolution keeps schedules readable
    steps = (hi_ns - lo_ns) // 1_000_000
    return lo_ns + gen.below(steps + 1) * 1_000_000


def generate(cfg: FaultConfig, nodes: tuple[str, ...], gen: SplitMix64) -> list[Fault]:
    """Generate a random fault schedule (deterministic given ``gen``)."""
    kinds = [k for k in SCHEDULED_KINDS if k in cfg.kinds]
    if not kinds or cfg.max_faults <= 0:
        return []
    if len(nodes) < 2:
        kinds = [k for k in kinds if k in ("crashes", "clock")]
        if not kinds or not nodes:
            return []
    count = 1 + gen.below(cfg.max_faults)
    faults: list[tuple[int, int, Fault]] = []
    for i in range(count):
        kind = kinds[gen.below(len(kinds))]
        at = _uniform_ns(gen, cfg.window_start, cfg.window_end)
        fid = f"f{i}"
        if kind == "partitions":
            dur = _uniform_ns(gen, *cfg.partition_duration)
            shape = gen.below(3)
            if shape == 0 or len(nodes) == 2:
                # isolate one node from all others
                victim = nodes[gen.below(len(nodes))]
                rest = [n for n in nodes if n != victim]
                fault = Fault(
                    fid, "partition", at, dur, {"groups": [[victim], rest], "oneway": False}
                )
            elif shape == 1:
                mask = 1 + gen.below((1 << len(nodes)) - 2)
                side_a = [n for j, n in enumerate(nodes) if mask >> j & 1]
                side_b = [n for j, n in enumerate(nodes) if not mask >> j & 1]
                fault = Fault(
                    fid, "partition", at, dur, {"groups": [side_a, side_b], "oneway": False}
                )
            else:
                a = nodes[gen.below(len(nodes))]
                b = [n for n in nodes if n != a][gen.below(len(nodes) - 1)]
                fault = Fault(fid, "partition", at, dur, {"a": a, "b": b, "oneway": True})
        elif kind == "crashes":
            node = nodes[gen.below(len(nodes))]
            dur = _uniform_ns(gen, *cfg.crash_downtime)
            fault = Fault(fid, "crash", at, dur, {"node": node})
        elif kind == "resets":
            a = nodes[gen.below(len(nodes))]
            b = [n for n in nodes if n != a][gen.below(len(nodes) - 1)]
            fault = Fault(fid, "reset", at, 0, {"a": a, "b": b})
        elif kind == "latency":
            a = nodes[gen.below(len(nodes))]
            b = [n for n in nodes if n != a][gen.below(len(nodes) - 1)]
            dur = _uniform_ns(gen, *cfg.latency_duration)
            fault = Fault(fid, "latency", at, dur, {"a": a, "b": b, "factor": cfg.latency_factor})
        else:  # clock
            node = nodes[gen.below(len(nodes))]
            max_off = round(cfg.clock_offset_max * 1000)
            offset_ms = gen.below(2 * max_off + 1) - max_off
            max_ppm = round(cfg.clock_drift_ppm_max)
            drift = gen.below(2 * max_ppm + 1) - max_ppm
            dur = _uniform_ns(gen, *cfg.partition_duration)
            fault = Fault(
                fid,
                "clock",
                at,
                dur,
                {"node": node, "offset_ns": offset_ms * 1_000_000, "drift_ppm": drift},
            )
        faults.append((at, i, fault))
    faults.sort(key=lambda x: (x[0], x[1]))
    return [f for _, _, f in faults]


def schedule_to_json(faults: list[Fault]) -> list[dict[str, Any]]:
    return [f.to_json() for f in faults]


def schedule_from_json(data: list[dict[str, Any]]) -> list[Fault]:
    return [Fault.from_json(d) for d in data]
