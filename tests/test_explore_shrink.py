"""Explorer aggregation, worker supervision, SQLite, artifacts and shrinking."""

from __future__ import annotations

import json
import os
import sqlite3

import pytest

import detloop
from detloop.artifacts import ResultsDB, read_manifest
from detloop.explore import explore
from detloop.replay import replay_manifest
from detloop.shrink import shrink
from detloop.targets import Target

from . import sim_targets

RACY = Target("tests/sim_targets.py", "test_racy_counter")


def spec_of(fn: object) -> detloop.Spec:
    spec = detloop.get_spec(fn)
    assert spec is not None
    return spec


@pytest.fixture
def in_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_serial_and_parallel_agree_per_seed(tmp_path: object, in_root: None) -> None:
    spec = spec_of(sim_targets.test_racy_counter)
    serial = explore(spec, range(30), target=RACY, jobs=1, artifacts_dir=None)
    parallel = explore(spec, range(30), target=RACY, jobs=3, artifacts_dir=None)
    assert serial.results.keys() == parallel.results.keys()
    for s in serial.results:
        a, b = serial.results[s], parallel.results[s]
        assert (a["status"], a["signature"], a["trace_sha256"]) == (
            b["status"],
            b["signature"],
            b["trace_sha256"],
        )
    assert serial.first_failure == parallel.first_failure is not None
    assert serial.exit_code() == parallel.exit_code() == 1


def test_max_failures_cutoff_is_deterministic(in_root: None) -> None:
    spec = spec_of(sim_targets.test_racy_counter)
    full = explore(spec, range(30), target=RACY, jobs=1, artifacts_dir=None)
    expect = sorted(full.failures)[:2]
    for jobs in (1, 3):
        r = explore(spec, range(30), target=RACY, jobs=jobs, max_failures=2, artifacts_dir=None)
        assert r.failures == expect
        assert max(r.results) == expect[-1]
        assert set(r.results) == set(range(expect[-1] + 1))


def test_hard_kill_of_cpu_bound_worker(in_root: None) -> None:
    spec = spec_of(sim_targets.test_cpu_hog)
    r = explore(
        spec,
        range(3),
        target=Target("tests/sim_targets.py", "test_cpu_hog"),
        jobs=2,
        artifacts_dir=None,
        config=spec.config.with_overrides(wall_timeout=1.0),
    )
    assert r.results[1]["status"] == "incomplete"
    assert r.results[1]["signature"] == "wall_timeout"
    assert r.results[0]["status"] == r.results[2]["status"] == "pass"
    assert r.exit_code() == 3


def test_sqlite_schema_and_artifacts(tmp_path: object, in_root: None) -> None:
    spec = spec_of(sim_targets.test_racy_counter)
    db = os.path.join(str(tmp_path), "res.sqlite")
    art = os.path.join(str(tmp_path), "art")
    r = explore(spec, range(10), target=RACY, jobs=2, artifacts_dir=art, db_path=db)
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone() == ("1",)
    rows = conn.execute("SELECT seed, status FROM runs ORDER BY seed").fetchall()
    assert [row[0] for row in rows] == list(range(10))
    assert conn.execute("SELECT status FROM explorations").fetchone() == ("finished",)
    conn.close()
    for seed, path in r.artifacts.items():
        m = read_manifest(path)
        assert m["seed"] == seed and m["outcome"]["status"] != "pass"
        assert os.path.isfile(os.path.join(os.path.dirname(path), "trace.jsonl"))
    # schema mismatch is rejected
    c = sqlite3.connect(db)
    c.execute("UPDATE meta SET value='99'")
    c.commit()
    c.close()
    with pytest.raises(detloop.ConfigError):
        ResultsDB(db)


def test_failure_artifacts_replay_exactly(tmp_path: object, in_root: None) -> None:
    spec = spec_of(sim_targets.test_racy_counter)
    r = explore(spec, range(15), target=RACY, jobs=1, artifacts_dir=str(tmp_path))
    seed = r.first_failure
    assert seed is not None
    report, _, _ = replay_manifest(r.artifacts[seed])
    assert report.reproduced and report.identical_trace


def test_shrink_preserves_signature_and_replays_strictly(in_root: None) -> None:
    from detloop.faults import Fault

    spec = spec_of(sim_targets.test_racy_counter)
    seed = next(
        s for s in range(30) if detloop.run(spec.fn, seed=s, config=spec.config).status == "fail"
    )
    noise = [
        Fault(
            f"f{i}",
            "latency",
            int((0.1 + i * 0.1) * 1e9),
            int(0.05e9),
            {"a": "n1", "b": "n2", "factor": 3.0},
        )
        for i in range(4)
    ]
    sr = shrink(spec, seed, config=spec.config, faults=noise, max_runs=150)
    m = sr.metrics()
    assert sr.final.signature == sr.original.signature
    assert m["faults"][1] < m["faults"][0]
    assert m["nonzero_decisions"][1] <= m["nonzero_decisions"][0]
    assert m["status"] in ("fixed_point", "budget_exhausted") and m["globally_minimal"] is False
    again = detloop.run(
        spec.fn,
        seed=seed,
        config=sr.config,
        faults=sr.faults,
        tapes=sr.tapes,
        decision_mode="strict",
    )
    assert again.trace_sha256 == sr.final.trace_sha256


def test_shrink_budget_exhaustion_is_reported(in_root: None) -> None:
    spec = spec_of(sim_targets.test_racy_counter)
    seed = next(
        s for s in range(30) if detloop.run(spec.fn, seed=s, config=spec.config).status == "fail"
    )
    sr = shrink(spec, seed, config=spec.config, max_runs=2)
    assert sr.status == "budget_exhausted"
    assert sr.final.signature == sr.original.signature


def test_shrinking_a_passing_seed_is_rejected() -> None:
    spec = spec_of(sim_targets.test_always_passes)
    with pytest.raises(detloop.ReplayMismatch):
        shrink(spec, 0)


def test_explore_summary_is_json_serializable(in_root: None) -> None:
    spec = spec_of(sim_targets.test_always_passes)
    r = explore(spec, range(5), artifacts_dir=None)
    s = r.summary()
    json.dumps(s)
    assert s["passed"] == 5 and s["exit_code"] == 0 and s["first_failure"] is None
