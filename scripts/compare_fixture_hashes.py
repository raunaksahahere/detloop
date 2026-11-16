"""Compare portable-fixture hashes collected from CI jobs.

    python scripts/compare_fixture_hashes.py hashes/*.json

Fails (exit 1) if two jobs with the same Python minor version but different
operating systems produced different trace hashes. Differences across Python
minor versions are reported, not failed: asyncio internals differ between
versions (see docs/DETERMINISM.md).
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path


def main() -> int:
    files = [Path(p) for p in sys.argv[1:]]
    if not files:
        print("no hash files given")
        return 1
    runs = [json.loads(f.read_text(encoding="utf-8")) for f in files]
    failed = False
    by_version: dict[str, list[dict]] = defaultdict(list)
    for r in runs:
        by_version[r["python"]].append(r)
    reference: dict[str, dict[str, str]] = {}
    for version, group in sorted(by_version.items()):
        base = group[0]
        for other in group[1:]:
            for fixture, hashes in base["fixtures"].items():
                theirs = other["fixtures"].get(fixture, {})
                diff = sorted(k for k in hashes if hashes[k] != theirs.get(k))
                if diff:
                    failed = True
                    print(
                        f"MISMATCH python {version}: {base['platform']} vs {other['platform']} "
                        f"{fixture} seeds {diff[:10]}"
                    )
        platforms = ", ".join(sorted(r["platform"] for r in group))
        print(f"python {version}: {len(group)} job(s) [{platforms}] agree" if not failed else "")
        reference[version] = base["fixtures"]
    versions = sorted(reference)
    for i, a in enumerate(versions):
        for b in versions[i + 1 :]:
            for fixture in reference[a]:
                ha, hb = reference[a][fixture], reference[b].get(fixture, {})
                same = sum(1 for k in ha if ha[k] == hb.get(k))
                print(f"info: python {a} vs {b}: {fixture}: {same}/{len(ha)} seeds byte-identical")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
