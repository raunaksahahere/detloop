"""pytest plugin (registered through the ``pytest11`` entry point).

``@detloop.simulate`` tests are ordinary synchronous test functions, so they
are collected by pytest's normal discovery and are not claimed by
pytest-asyncio or other async plugins. This plugin only adds command-line
options and reporting.
"""

from __future__ import annotations

from typing import Any

import pytest

from . import pytest_support
from ._version import __version__


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("detloop", "deterministic simulation testing")
    group.addoption("--detloop-seed", type=int, default=None, metavar="N",
                    help="run only seed N of every @detloop.simulate test")
    group.addoption("--detloop-seeds", type=int, default=None, metavar="N",
                    help="override the number of seeds per test")
    group.addoption("--detloop-seed-start", type=int, default=None, metavar="N",
                    help="override the first seed")
    group.addoption("--detloop-replay", default=None, metavar="MANIFEST",
                    help="replay a saved (possibly shrunk) failure manifest")
    group.addoption("--detloop-artifacts", default=None, metavar="DIR",
                    help="directory for failure traces/manifests (default .detloop/artifacts)")
    group.addoption("--detloop-no-artifacts", action="store_true", default=False,
                    help="do not write failure artifacts")
    group.addoption("--detloop-shrink", action="store_true", default=False,
                    help="shrink failures and save a minimized replay manifest")
    group.addoption("--detloop-trace", default=None, choices=["off", "basic", "full"],
                    help="override the trace level")


def pytest_configure(config: pytest.Config) -> None:
    opts = pytest_support.options
    opts.seed = config.getoption("--detloop-seed")
    opts.seeds = config.getoption("--detloop-seeds")
    opts.seed_start = config.getoption("--detloop-seed-start")
    opts.replay = config.getoption("--detloop-replay")
    opts.shrink = config.getoption("--detloop-shrink")
    opts.trace = config.getoption("--detloop-trace")
    if config.getoption("--detloop-no-artifacts"):
        opts.artifacts = None
    else:
        opts.artifacts = config.getoption("--detloop-artifacts") or str(config.rootpath / ".detloop" / "artifacts")
    opts.rootdir = str(config.rootpath)


def pytest_report_header(config: pytest.Config) -> str:
    return f"detloop: {__version__}"


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item: pytest.Item) -> None:
    pytest_support.options.nodeid = item.nodeid


def pytest_runtest_teardown(item: pytest.Item, nextitem: Any) -> None:
    pytest_support.options.nodeid = None
