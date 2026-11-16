"""The shipped examples behave as the documentation says."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import detloop
from detloop import targets
from detloop.explore import explore

ROOT = Path(__file__).resolve().parent.parent
QUICK = ROOT / "examples" / "quickstart"


def test_quickstart_finds_and_replays_the_race(tmp_path: Path) -> None:
    (target, spec), *_ = targets.discover(str(QUICK / "test_counter.py"))
    result = explore(spec, range(200), target=target, artifacts_dir=str(tmp_path))
    assert result.failures, "the lost-update race should be found"
    seed = result.first_failure
    assert result.results[seed]["signature"].startswith("assertion:test_counter.py")
    p = subprocess.run(
        [sys.executable, "-m", "detloop", "replay", result.artifacts[seed], "--expect-reproduce"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert p.returncode == 0, p.stdout + p.stderr


def test_fixed_quickstart_passes_every_seed() -> None:
    (target, spec), *_ = targets.discover(str(QUICK / "test_counter_fixed.py"))
    result = explore(spec, range(200), target=target, artifacts_dir=None)
    assert result.summary()["passed"] == 200


def test_examples_run_under_pytest(tmp_path: Path) -> None:
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    p = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            str(QUICK),
            f"--detloop-artifacts={tmp_path}",
            "--rootdir",
            str(QUICK),
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
        cwd=QUICK,
    )
    assert "1 failed, 1 passed" in p.stdout, p.stdout
    assert "--detloop-seed=" in p.stdout


def test_shipped_kv_example_module_is_importable() -> None:
    from detloop.examples import kv_sim

    assert detloop.get_spec(kv_sim.test_kv_correct) is not None
