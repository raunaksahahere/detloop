"""pytest integration: discovery, options, failure reporting, coexistence."""

from __future__ import annotations

import importlib.util

import pytest

pytest_plugins = ["pytester"]


@pytest.fixture(autouse=True)
def _ini(pytester: pytest.Pytester) -> None:
    pytester.makeini("[pytest]\nasyncio_default_fixture_loop_scope = function\n")

TESTS = '''
import asyncio
import pytest
import detloop

@detloop.simulate(nodes=2, seeds=5)
async def test_passes(world):
    await asyncio.sleep(1)
    assert world.time() == 1.0

@detloop.simulate(nodes=3, seeds=40, faults="partitions")
async def test_racy(world):
    shared = {"v": 0}
    async def incr(node):
        v = shared["v"]
        await asyncio.sleep(0)
        shared["v"] = v + 1
    world.start(incr)
    await asyncio.sleep(1)
    assert shared["v"] == 3, "lost update"

@pytest.fixture
def factor():
    return 3

@pytest.mark.parametrize("n", [1, 2])
@detloop.simulate(nodes=1, seeds=2)
async def test_param_and_fixture(world, n, factor):
    await asyncio.sleep(n * factor)
    assert world.time() == n * factor
'''


def test_discovery_parametrize_fixtures_and_failure_report(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_sim=TESTS)
    result = pytester.runpytest("-q")
    result.assert_outcomes(passed=3, failed=1)
    result.stdout.fnmatch_lines([
        "*detloop: seed * fail (assertion:*) after * of 40 seed(s)*",
        "*replay:   pytest test_sim.py::test_racy --detloop-seed=*",
        "*detloop replay*manifest.json*",
    ])


def test_single_seed_option_and_replay_manifest(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_sim=TESTS)
    import re

    first = pytester.runpytest("-q", "test_sim.py::test_racy")
    out = first.stdout.str()
    seed = int(re.search(r"detloop: seed (\d+) fail", out).group(1))  # type: ignore[union-attr]
    manifest = re.search(r"detloop replay (\S+manifest\.json)", out).group(1)  # type: ignore[union-attr]
    again = pytester.runpytest("-q", "test_sim.py::test_racy", f"--detloop-seed={seed}")
    again.assert_outcomes(failed=1)
    again.stdout.fnmatch_lines([f"*seed {seed} fail*after 1 of 1 seed(s)*"])
    replay = pytester.runpytest("-q", "test_sim.py::test_racy",
                                f"--detloop-replay={manifest}")
    replay.assert_outcomes(failed=1)
    replay.stdout.fnmatch_lines(["*REPRODUCED*"])
    passing_seed = pytester.runpytest("-q", "test_sim.py::test_passes",
                                      "--detloop-seeds=2", "--detloop-seed-start=100")
    passing_seed.assert_outcomes(passed=1)


def test_shrink_option_writes_minimized_manifest(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_sim=TESTS)
    result = pytester.runpytest("-q", "test_sim.py::test_racy", "--detloop-shrink")
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*shrunk (*", "*shrunk manifest:*-shrunk*manifest.json*", "*--detloop-replay=*"])


def test_report_header_and_options_registered(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_sim=TESTS)
    result = pytester.runpytest("--help")
    result.stdout.fnmatch_lines(["*--detloop-seed=N*", "*--detloop-replay=MANIFEST*"])
    result = pytester.runpytest("-q", "test_sim.py::test_passes", "-v")
    result.stdout.fnmatch_lines(["detloop: *"])


@pytest.mark.skipif(importlib.util.find_spec("pytest_asyncio") is None, reason="pytest-asyncio not installed")
def test_coexists_with_pytest_asyncio_auto_mode(pytester: pytest.Pytester) -> None:
    pytester.makeini("[pytest]\nasyncio_mode = auto\nasyncio_default_fixture_loop_scope = function\n")
    pytester.makepyfile(test_sim=TESTS + '''

async def test_plain_async_test():
    await asyncio.sleep(0)
''')
    result = pytester.runpytest("-p", "asyncio", "-q", "-k", "passes or plain or param")
    result.assert_outcomes(passed=4)
