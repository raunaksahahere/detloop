"""Render a results JSON file as the Markdown tables used in docs/BENCHMARKS.md.

python benchmarks/summarize.py benchmarks/results/<file>.json
"""

from __future__ import annotations

import json
import sys
from typing import Any

BUGS = ("stale-read", "lost-ack", "split-brain")


def render(d: dict[str, Any], source: str) -> str:
    m, r = d["machine"], d["results"]
    out = [
        f"Source: [`{source}`](../{source}) — {m['cpu']}, {m['logical_cpus']} logical CPUs, "
        f"{m['memory_gib']} GiB RAM, {m['os']}, {m['implementation']} {m['python']}, "
        f"detloop {m['detloop']} (commit `{m['git_commit']}`), {m['date']}.",
        "",
    ]
    t = r.get("throughput")
    if t:
        out += [
            "### Throughput",
            "",
            "| Workload | Seeds | Wall (s) | Seeds/s | Loop steps/s |",
            "|---|---|---|---|---|",
            f"| KV corrected baseline, serial | {t['kv_serial']['seeds']} | {t['kv_serial']['wall_seconds']} | "
            f"{t['kv_serial']['seeds_per_second']} | {t['kv_serial']['steps_per_second']:,} |",
            f"| KV corrected baseline, {t['kv_parallel']['jobs']} workers | {t['kv_parallel']['seeds']} | "
            f"{t['kv_parallel']['wall_seconds']} | {t['kv_parallel']['seeds_per_second']} | |",
            f"| 8 MiB echo over one connection | {t['bulk_transfer_8MiB']['seeds']} | "
            f"{t['bulk_transfer_8MiB']['wall_seconds']} | {t['bulk_transfer_8MiB']['seeds_per_second']} | "
            f"{t['bulk_transfer_8MiB']['steps_per_second']:,} |",
            "",
        ]
    tr = r.get("trace")
    if tr:
        out += [
            "### Event throughput and trace overhead",
            "",
            "| Workload | Trace level | Steps/s | Trace events | Overhead vs `off` |",
            "|---|---|---|---|---|",
        ]
        for wl, label in (
            ("pingpong", "scheduler micro-benchmark (60 tasks)"),
            ("kv", "KV corrected baseline"),
        ):
            for lvl in ("off", "basic", "full"):
                row = tr[f"{wl}_{lvl}"]
                ov = "—" if lvl == "off" else f"{tr[f'{wl}_overhead_{lvl}_pct']}%"
                out.append(
                    f"| {label} | `{lvl}` | {row['steps_per_second']:,} | {row['trace_events']:,} | {ov} |"
                )
        out.append("")
    mem = r.get("memory")
    if mem:
        out += [
            "### Memory (tracemalloc peak, one run)",
            "",
            f"KV run, basic trace: {mem['kv_basic_peak_mib']} MiB; KV run, full trace: {mem['kv_full_peak_mib']} MiB; "
            f"scheduler micro-benchmark: {mem['pingpong_basic_peak_mib']} MiB.",
            "",
        ]
    c = r.get("corpus")
    if c:
        out += [
            f"### Bug corpus: seeds to first failure ({c['seeds_per_bug']} seeds per variant)",
            "",
            "| Variant | Failing seeds | Failure rate | First failing seed | Seeds to failure: median / p90 / max |",
            "|---|---|---|---|---|",
        ]
        for b in BUGS:
            x = c[b]
            s = x["seeds_to_first_failure"]
            out.append(
                f"| `{b}` | {x['failing_seeds']} | {x['failure_rate']:.1%} | {x['first_failing_seed']} | "
                f"{s['median']} / {s['p90']} / {s['max']} |"
            )
        cb = c["corrected_baseline"]
        out += [
            f"| corrected | {cb['failed']} (of {cb['seeds']}; {cb['incomplete']} incomplete) | 0% | — | — |",
            "",
            "Seeds-to-failure: for every start seed *s*, the number of seeds tried from *s* until the next "
            "failing seed (so `1` means *s* itself fails).",
            "",
        ]
    sh = r.get("shrink")
    if sh:
        out += [
            "### Shrinking (budget 150 candidate runs)",
            "",
            "| Variant | Seed | Status | Faults | Non-zero decisions | Steps | Trace events | Wall (s) |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for b in BUGS:
            x = sh[b]
            out.append(
                f"| `{b}` | {x['seed']} | {x['status']} | {x['faults'][0]} → {x['faults'][1]} | "
                f"{x['nonzero_decisions'][0]} → {x['nonzero_decisions'][1]} | {x['steps'][0]} → {x['steps'][1]} | "
                f"{x['trace_events'][0]} → {x['trace_events'][1]} | {x['seconds']} |"
            )
        out.append("")
    bl = r.get("baseline")
    if bl:
        out += [
            f"### Baseline: real asyncio with random sleeps ({bl['trials_per_variant']} trials per variant, "
            f"{bl['workers']} in parallel)",
            "",
            "| Variant | Detected | Detection rate | Mean wall time per trial (s) | Simulated failure rate per seed |",
            "|---|---|---|---|---|",
        ]
        for b in ("none", *BUGS):
            x = bl[b]
            sim = "0%" if b == "none" else (f"{c[b]['failure_rate']:.1%}" if c else "")
            out.append(
                f"| `{b if b != 'none' else 'corrected'}` | {x['detected']}/{x['trials']} | {x['detection_rate']:.0%} | "
                f"{x['mean_wall_seconds_per_trial']} | {sim} |"
            )
        out.append("")
    return "\n".join(out)


def main() -> int:
    path = sys.argv[1]
    with open(path) as fh:
        data = json.load(fh)
    rel = path.split("detloop/", 1)[-1] if "detloop/" in path else path
    print(render(data, rel))
    return 0


if __name__ == "__main__":
    sys.exit(main())
