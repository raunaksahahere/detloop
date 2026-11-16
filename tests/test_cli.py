"""CLI commands, exit codes and machine-readable output."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from detloop import __version__
from detloop.cli import main

ROOT = Path(__file__).resolve().parent.parent


def cli(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, "-m", "detloop", *args], cwd=cwd or ROOT, capture_output=True,
                          text=True, check=False)


def test_version_and_help() -> None:
    p = cli("--version")
    assert p.returncode == 0 and p.stdout.strip() == f"detloop {__version__}"
    p = cli("explore", "--help")
    assert p.returncode == 0 and "--seeds" in p.stdout and "--jobs" in p.stdout
    assert main([]) == 2


def test_usage_errors_exit_2(tmp_path: Path) -> None:
    assert cli("explore", "does/not/exist.py").returncode == 2
    assert cli("explore", "tests/sim_targets.py::nope").returncode == 2
    assert cli("explore", "tests/sim_targets.py::test_always_passes", "--set", "bogus_key=1").returncode == 2
    assert cli("report", str(tmp_path / "missing.json")).returncode == 2
    p = cli("shrink", "tests/sim_targets.py::test_racy_counter")
    assert p.returncode == 2 and "--seed" in p.stderr


def test_explore_replay_shrink_report_round_trip(tmp_path: Path) -> None:
    art = tmp_path / "art"
    db = tmp_path / "r.sqlite"
    summary_file = tmp_path / "s.json"
    p = cli("explore", "tests/sim_targets.py::test_racy_counter", "--seeds", "12", "--jobs", "2", "--artifacts",
            str(art), "--db", str(db), "--json", "--summary-file", str(summary_file), "-q")
    assert p.returncode == 1, p.stderr
    summary = json.loads(p.stdout)
    assert summary == json.loads(summary_file.read_text())
    assert summary["failed"] > 0 and summary["exit_code"] == 1
    seed = summary["first_failure"]
    manifest = summary["artifacts"][str(seed)]

    p = cli("replay", manifest, "--json")
    assert p.returncode == 1
    out = json.loads(p.stdout)
    assert out["reproduced"] and out["identical_trace"] and out["seed"] == seed
    assert cli("replay", manifest, "--expect-reproduce").returncode == 0

    p = cli("shrink", manifest, "--artifacts", str(art), "--max-runs", "40", "--json", "-q")
    assert p.returncode == 0, p.stderr
    metrics = json.loads(p.stdout)
    shrunk = metrics["manifest"]
    assert metrics["signature"].startswith("assertion:")
    assert cli("replay", shrunk, "--expect-reproduce").returncode == 0

    html = tmp_path / "r.html"
    assert cli("report", shrunk, "-o", str(html)).returncode == 0
    assert "detloop report" in html.read_text()
    assert cli("report", str(db), "-o", str(tmp_path / "x.html")).returncode == 0
    trace = os.path.join(os.path.dirname(manifest), "trace.jsonl")
    assert cli("report", trace, "-o", str(tmp_path / "t.html")).returncode == 0


def test_explore_passing_and_incomplete_exit_codes(tmp_path: Path) -> None:
    p = cli("explore", "tests/sim_targets.py::test_always_passes", "--no-db", "--artifacts", str(tmp_path), "-q")
    assert p.returncode == 0
    p = cli("explore", "tests/sim_targets.py::test_always_passes", "--no-db", "--set", "max_virtual_time=0.5",
            "--artifacts", str(tmp_path), "-q", "--json")
    assert p.returncode == 3
    assert json.loads(p.stdout)["incomplete"] == 5


def test_explore_skips_tests_with_extra_parameters(tmp_path: Path) -> None:
    p = cli("explore", "tests/sim_targets.py::test_with_fixture", "--no-db", "--artifacts", str(tmp_path))
    assert "skipping" in p.stderr


def test_replay_rejects_changed_code(tmp_path: Path) -> None:
    src = tmp_path / "sim_mod.py"
    src.write_text(
        "import asyncio, detloop\n"
        "@detloop.simulate(nodes=1, seeds=3)\n"
        "async def test_fail(world):\n"
        "    await asyncio.sleep(1)\n"
        "    assert False, 'always'\n"
    )
    p = cli("explore", "sim_mod.py", "--no-db", "--artifacts", "art", "--json", "-q", cwd=tmp_path)
    assert p.returncode == 1
    manifest = json.loads(p.stdout)["artifacts"]["0"]
    assert cli("replay", manifest, "--expect-reproduce", cwd=tmp_path).returncode == 0
    src.write_text(src.read_text() + "\n# edited\n")
    p = cli("replay", manifest, cwd=tmp_path)
    assert p.returncode == 4 and "changed" in p.stderr
    assert cli("replay", manifest, "--allow-mismatch", cwd=tmp_path).returncode == 1


@pytest.mark.parametrize("bad", ["{}", '{"schema": "other"}'])
def test_replay_rejects_foreign_manifests(tmp_path: Path, bad: str) -> None:
    f = tmp_path / "m.json"
    f.write_text(bad)
    assert cli("replay", str(f)).returncode == 4


def test_verify_determinism_flag(tmp_path: Path) -> None:
    p = cli("explore", "tests/sim_targets.py::test_portable_fixture", "--seeds", "4", "--no-db", "--artifacts",
            str(tmp_path), "--verify-determinism", "4", "--json", "-q")
    out = json.loads(p.stdout)
    assert out["determinism"]["consistent"] is True and out["determinism"]["seeds"] == 4
