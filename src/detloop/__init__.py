"""detloop: deterministic simulation testing for Python asyncio services.

Run cooperating asyncio services inside a simulated world with virtual time,
seeded scheduling, a simulated network, partitions, crashes and restarts;
check invariants and linearizability; explore thousands of seeds; shrink and
replay failures from a seed.

Quick start::

    import asyncio, detloop

    @detloop.simulate(nodes=2, seeds=200, faults="partitions")
    async def test_echo(world):
        async def server(node):
            async def handle(r, w):
                w.write(await r.readline()); await w.drain(); w.close()
            srv = await asyncio.start_server(handle, port=7000)
            await srv.serve_forever()
        world.start(server, nodes=["n1"])
        await asyncio.sleep(0.1)
        r, w = await asyncio.open_connection("n1", 7000)
        w.write(b"hi\\n")
        assert await r.readline() == b"hi\\n"

See https://github.com/raunaksahahere/detloop for documentation.
"""

from ._version import __version__
from .api import Spec, get_spec, simulate
from .checker import (
    CheckResult,
    KVModel,
    Model,
    RegisterModel,
    assert_linearizable,
    check_linearizable,
)
from .config import Budgets, FaultConfig, NetworkConfig, SimConfig
from .errors import (
    BudgetExceeded,
    CheckIncomplete,
    ConfigError,
    DeadlockError,
    DetloopError,
    InternalError,
    InvariantViolation,
    LinearizabilityViolation,
    NodeFailure,
    NondeterminismError,
    ReplayDivergence,
    ReplayMismatch,
    SimulationError,
    SimulationFailure,
    StarvationError,
    UnsupportedOperation,
    WallClockTimeout,
)
from .faults import Fault
from .history import UNKNOWN, History, Operation
from .loop import SimLoop
from .rng import SimRandom
from .runner import RunResult, run
from .world import Node, World

__all__ = [
    "UNKNOWN",
    "BudgetExceeded",
    "Budgets",
    "CheckIncomplete",
    "CheckResult",
    "ConfigError",
    "DeadlockError",
    "DetloopError",
    "Fault",
    "FaultConfig",
    "History",
    "InternalError",
    "InvariantViolation",
    "KVModel",
    "LinearizabilityViolation",
    "Model",
    "NetworkConfig",
    "Node",
    "NodeFailure",
    "NondeterminismError",
    "Operation",
    "RegisterModel",
    "ReplayDivergence",
    "ReplayMismatch",
    "RunResult",
    "SimConfig",
    "SimLoop",
    "SimRandom",
    "SimulationError",
    "SimulationFailure",
    "Spec",
    "StarvationError",
    "UnsupportedOperation",
    "WallClockTimeout",
    "World",
    "__version__",
    "assert_linearizable",
    "check_linearizable",
    "get_spec",
    "run",
    "simulate",
]
