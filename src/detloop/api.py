"""The ``@detloop.simulate`` decorator and test registration.

A decorated ``async def test_x(world, ...)`` becomes a plain synchronous
function (so pytest-asyncio and other async plugins leave it alone) that runs
the simulation for a range of seeds. The original coroutine function and its
:class:`Spec` are attached as ``__detloop_spec__`` for discovery by the CLI
(``detloop explore path/to/file.py``).

Configuration precedence (highest first): ``--detloop-*`` pytest options or
CLI flags, decorator arguments, :class:`~detloop.config.SimConfig` defaults.
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar, overload

from .config import SimConfig, apply_overrides, node_names, parse_faults
from .errors import ConfigError

F = TypeVar("F", bound=Callable[..., Any])

#: Defaults of :func:`simulate` (documented in docs/API.md).
DEFAULT_SEEDS = 100
DEFAULT_NODES = 3


@dataclass(frozen=True)
class Spec:
    """A registered simulation: the coroutine function plus its configuration."""

    fn: Callable[..., Any]
    config: SimConfig
    seeds: int
    seed_start: int
    name: str
    module: str
    qualname: str
    extra_params: tuple[str, ...] = ()
    overrides: dict[str, Any] = field(default_factory=dict)

    @property
    def test_id(self) -> str:
        return self.qualname

    def seed_range(self, seeds: int | None = None, start: int | None = None) -> range:
        s = self.seed_start if start is None else start
        n = self.seeds if seeds is None else seeds
        return range(s, s + n)


def build_config(nodes: Any = DEFAULT_NODES, faults: Any = None, **overrides: Any) -> SimConfig:
    cfg = SimConfig(nodes=node_names(nodes))
    if faults is not None:
        overrides["faults.kinds"] = parse_faults(faults)
    return apply_overrides(cfg, overrides) if overrides else cfg


@overload
def simulate(fn: F) -> F: ...


@overload
def simulate(
    fn: None = None,
    *,
    nodes: int | list[str] | tuple[str, ...] = ...,
    seeds: int = ...,
    seed_start: int = ...,
    faults: str | list[str] | tuple[str, ...] | None = ...,
    scheduler: str = ...,
    **config: Any,
) -> Callable[[F], F]: ...


def simulate(
    fn: Callable[..., Any] | None = None,
    *,
    nodes: int | list[str] | tuple[str, ...] = DEFAULT_NODES,
    seeds: int = DEFAULT_SEEDS,
    seed_start: int = 0,
    faults: str | list[str] | tuple[str, ...] | None = None,
    scheduler: str = "interleave",
    **config: Any,
) -> Any:
    """Declare a simulation test.

    ``@detloop.simulate(nodes=3, seeds=2000, faults="partitions,crashes")``

    Parameters
    ----------
    nodes:
        Number of nodes (named ``n1..nN``) or explicit names. Default 3.
    seeds:
        How many seeds a pytest run executes (``seed_start .. seed_start+seeds-1``).
        Default 100.
    faults:
        Comma-separated fault kinds (``partitions``, ``crashes``, ``resets``,
        ``latency``, ``clock``, ``messages``), ``"all"`` or ``None``.
    scheduler:
        ``interleave`` (default), ``fifo`` or ``relaxed`` (opt-in, breaks
        asyncio FIFO guarantees).
    **config:
        Any :class:`SimConfig` field, ``section.field`` key, or unique field
        name, e.g. ``max_virtual_time=60``, ``latency_max=0.05``.
    """

    def decorate(f: Callable[..., Any]) -> Any:
        if not inspect.iscoroutinefunction(f):
            raise ConfigError(f"@detloop.simulate requires an 'async def' function, got {f!r}")
        params = list(inspect.signature(f).parameters.values())
        if not params:
            raise ConfigError(f"{f.__qualname__} must accept the World as its first parameter")
        if seeds < 1 or seed_start < 0:
            raise ConfigError("seeds must be >= 1 and seed_start >= 0")
        cfg = build_config(nodes, faults, scheduler=scheduler, **config)
        extra = tuple(p.name for p in params[1:])
        spec = Spec(
            fn=f,
            config=cfg,
            seeds=seeds,
            seed_start=seed_start,
            name=f.__name__,
            module=f.__module__,
            qualname=f.__qualname__,
            extra_params=extra,
            overrides=dict(config),
        )

        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> None:
            from .pytest_support import run_spec_for_pytest

            run_spec_for_pytest(spec, args, kwargs)

        # pytest must see the signature without the World parameter
        wrapper.__signature__ = inspect.Signature(params[1:])  # type: ignore[attr-defined]
        wrapper.__detloop_spec__ = spec  # type: ignore[attr-defined]
        del wrapper.__wrapped__  # keep pytest from introspecting the async function
        return wrapper

    if fn is not None:
        return decorate(fn)
    return decorate


def get_spec(obj: Any) -> Spec | None:
    spec = getattr(obj, "__detloop_spec__", None)
    return spec if isinstance(spec, Spec) else None
