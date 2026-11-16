from __future__ import annotations

from typing import Any

import pytest

import detloop
from detloop.config import SimConfig

pytest_plugins = ["pytester"]


def run_sim(fn: Any, seed: int = 0, **overrides: Any) -> detloop.RunResult:
    cfg = SimConfig(nodes=overrides.pop("nodes", ("n1", "n2", "n3")))
    if overrides:
        cfg = cfg.with_overrides(**overrides)
    return detloop.run(fn, seed=seed, config=cfg, keep_trace=True)


def expect_pass(fn: Any, seed: int = 0, **overrides: Any) -> detloop.RunResult:
    res = run_sim(fn, seed, **overrides)
    if res.status != "pass":
        pytest.fail(
            f"simulation did not pass: {res.status} {res.signature}: {res.message}\n{res.traceback}"
        )
    return res


@pytest.fixture
def sim() -> Any:
    return run_sim


@pytest.fixture
def passing() -> Any:
    return expect_pass
