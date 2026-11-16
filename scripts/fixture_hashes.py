"""Print trace hashes of the declared portable fixtures as JSON (used by CI).

python scripts/fixture_hashes.py > hashes.json
"""

from __future__ import annotations

import json
import platform
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import detloop  # noqa: E402
from detloop import targets  # noqa: E402

#: Declared portable fixtures and seed ranges (see docs/DETERMINISM.md).
FIXTURES = {"tests/sim_targets.py::test_portable_fixture": range(20)}


def main() -> int:
    out: dict[str, object] = {
        "python": ".".join(map(str, sys.version_info[:2])),
        "python_full": platform.python_version(),
        "platform": sys.platform,
        "fixtures": {},
    }
    for target, seeds in FIXTURES.items():
        (_, spec), *_ = targets.discover(
            str(ROOT / target.split("::")[0]) + "::" + target.split("::")[1]
        )
        hashes = {}
        for s in seeds:
            r = detloop.run(spec.fn, seed=s, config=spec.config)
            hashes[str(s)] = r.trace_sha256
        out["fixtures"][target] = hashes  # type: ignore[index]
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
