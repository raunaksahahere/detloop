"""Known-bug corpus: every planted KV bug is found by exploration within a
declared seed budget, saved, replayed exactly, and shrunk; the corrected
baseline passes its regression budget. These are *expected* findings, asserted
without turning the suite red."""

from __future__ import annotations

import os

import pytest

import detloop
from detloop.examples import kv_sim
from detloop.explore import explore
from detloop.replay import replay_manifest
from detloop.shrink import shrink
from detloop.targets import Target

#: Declared exploration budgets (seeds) and expected signatures.
CORPUS = {
    "test_kv_stale_read": (20, "linearizability"),
    "test_kv_lost_ack": (60, "linearizability"),
    "test_kv_split_brain": (60, "invariant:election-safety"),
}
#: Fixed regression seeds (first failing seed found by exploration at the time
#: of writing). Exploration below is the real evidence; these pin behaviour.
REGRESSION_SEEDS = {"test_kv_stale_read": 1, "test_kv_lost_ack": 0, "test_kv_split_brain": 17}
BASELINE_BUDGET = 40


@pytest.mark.parametrize("name", sorted(CORPUS))
def test_planted_bug_found_replayed_and_shrunk(name: str, tmp_path: object) -> None:
    budget, signature = CORPUS[name]
    spec = detloop.get_spec(getattr(kv_sim, name))
    assert spec is not None
    target = Target("detloop.examples.kv_sim", name)
    result = explore(
        spec, range(budget), target=target, jobs=4, max_failures=1, artifacts_dir=str(tmp_path)
    )
    seed = result.first_failure
    assert seed is not None, f"{name}: not found within {budget} seeds"
    assert result.results[seed]["signature"] == signature
    manifest = result.artifacts[seed]
    report, _, _ = replay_manifest(manifest)
    assert report.reproduced and report.identical_trace, report.describe()
    sr = shrink(spec, seed, config=spec.config, max_runs=25, max_seconds=120)
    m = sr.metrics()
    assert sr.final.signature == signature
    assert m["faults"][1] <= m["faults"][0]
    assert m["nonzero_decisions"][1] <= m["nonzero_decisions"][0]
    again = detloop.run(
        spec.fn,
        seed=seed,
        config=sr.config,
        faults=sr.faults,
        tapes=sr.tapes,
        decision_mode="strict",
    )
    assert again.signature == signature and again.trace_sha256 == sr.final.trace_sha256


@pytest.mark.parametrize("name", sorted(REGRESSION_SEEDS))
def test_regression_seed_still_fails(name: str) -> None:
    spec = detloop.get_spec(getattr(kv_sim, name))
    assert spec is not None
    res = detloop.run(spec.fn, seed=REGRESSION_SEEDS[name], config=spec.config)
    assert res.signature == CORPUS[name][1]


def test_corrected_baseline_passes_regression_budget() -> None:
    spec = detloop.get_spec(kv_sim.test_kv_correct)
    assert spec is not None
    result = explore(
        spec,
        range(BASELINE_BUDGET),
        target=Target("detloop.examples.kv_sim", "test_kv_correct"),
        jobs=4,
        artifacts_dir=None,
    )
    assert result.summary()["passed"] == BASELINE_BUDGET, result.summary()["signatures"]


@pytest.mark.slow
@pytest.mark.skipif(
    not os.environ.get("DETLOOP_SLOW"), reason="set DETLOOP_SLOW=1 for the long baseline run"
)
def test_corrected_baseline_long_run() -> None:
    spec = detloop.get_spec(kv_sim.test_kv_correct)
    assert spec is not None
    result = explore(
        spec,
        range(1000),
        target=Target("detloop.examples.kv_sim", "test_kv_correct"),
        jobs=os.cpu_count() or 2,
        artifacts_dir=None,
    )
    assert result.summary()["passed"] == 1000, result.summary()["signatures"]
