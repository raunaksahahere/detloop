"""Smoke test for an *installed* detloop (wheel, sdist, or PyPI).

Run from a directory outside the source tree with the target environment's
Python. It must not import detloop from a source checkout.

Checks: import and version, package resources (report assets), the CLI,
the pytest plugin entry point, finding and replaying a planted bug from the
shipped KV example, shrinking, and rendering a report.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path


def run(*args: str, cwd: Path, check: bool = True) -> subprocess.CompletedProcess[str]:
    p = subprocess.run(
        [sys.executable, *args], cwd=cwd, capture_output=True, text=True, check=False
    )
    if check and p.returncode not in (0, 1):
        raise SystemExit(f"command failed ({p.returncode}): {args}\n{p.stdout}\n{p.stderr}")
    return p


def main() -> int:
    expected = sys.argv[1] if len(sys.argv) > 1 else None
    import detloop
    from detloop import report

    location = Path(detloop.__file__).resolve()
    print(f"detloop {detloop.__version__} from {location}")
    if Path.cwd().resolve() in location.parents or "src" in location.parts[-3:-1]:
        raise SystemExit("detloop was imported from a source tree, not an installed package")
    if expected and detloop.__version__ != expected:
        raise SystemExit(f"version {detloop.__version__} != expected {expected}")
    assert report._asset("report.js") and report._asset("report.css"), "package resources missing"

    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        p = run("-m", "detloop", "--version", cwd=work)
        assert p.stdout.strip() == f"detloop {detloop.__version__}", p.stdout
        p = run(
            "-m",
            "detloop",
            "explore",
            "detloop.examples.kv_sim::test_kv_stale_read",
            "--seeds",
            "20",
            "--jobs",
            "2",
            "--max-failures",
            "1",
            "--json",
            "-q",
            cwd=work,
        )
        summary = json.loads(p.stdout)
        assert p.returncode == 1 and summary["failed"] >= 1, summary
        manifest = summary["artifacts"][str(summary["first_failure"])]
        p = run("-m", "detloop", "replay", manifest, "--expect-reproduce", cwd=work)
        assert p.returncode == 0, p.stdout + p.stderr
        p = run("-m", "detloop", "shrink", manifest, "--max-runs", "15", "--json", "-q", cwd=work)
        shrunk = json.loads(p.stdout)
        assert shrunk["signature"] == "linearizability", shrunk
        p = run("-m", "detloop", "replay", shrunk["manifest"], "--expect-reproduce", cwd=work)
        assert p.returncode == 0, p.stdout + p.stderr
        p = run("-m", "detloop", "report", shrunk["manifest"], "-o", "r.html", cwd=work)
        assert (work / "r.html").read_text(encoding="utf-8").startswith("<!doctype html>")
        p = run(
            "-m",
            "detloop",
            "explore",
            "detloop.examples.kv_sim::test_kv_correct",
            "--seeds",
            "8",
            "--jobs",
            "2",
            "-q",
            "--no-db",
            cwd=work,
        )
        assert p.returncode == 0, p.stdout + p.stderr
        # pytest plugin discovered through the entry point (no -p flag)
        (work / "test_smoke.py").write_text(
            textwrap.dedent("""
            import asyncio, detloop

            @detloop.simulate(nodes=1, seeds=3)
            async def test_ok(world):
                await asyncio.sleep(1)
                assert world.time() == 1.0
        """),
            encoding="utf-8",
        )
        env = dict(os.environ)
        p = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "-p",
                "no:cacheprovider",
                "test_smoke.py",
                "--detloop-seeds=2",
            ],
            cwd=work,
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        assert p.returncode == 0 and "1 passed" in p.stdout, p.stdout + p.stderr
    print("smoke test passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
