"""Run configuration.

A :class:`SimConfig` fully describes the *semantics* of a run besides the seed
and the code under test. Its canonical JSON form is hashed into the
``config_fingerprint`` recorded in traces and manifests; replay refuses a
mismatching fingerprint. Supervisory settings that cannot change simulated
behaviour (the wall-clock timeout) are excluded from the fingerprint.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from dataclasses import dataclass, field, fields, replace
from typing import Any

from .errors import ConfigError

SCHEDULERS = ("interleave", "fifo", "relaxed")
FAULT_KINDS = ("partitions", "crashes", "resets", "latency", "clock", "messages")
TRACE_LEVELS = ("off", "basic", "full")
CRASH_MODES = ("process", "host")


@dataclass(frozen=True)
class NetworkConfig:
    """Simulated network parameters. Times are in seconds; sizes in bytes."""

    #: One-way latency range; each segment draws uniformly at microsecond resolution.
    latency_min: float = 0.001
    latency_max: float = 0.010
    #: Per-direction link bandwidth in bytes/second (``None`` = unlimited).
    bandwidth: float | None = None
    #: Maximum segment payload.
    segment_size: int = 16384
    #: Receiver window: bytes in flight plus bytes buffered while reading is paused.
    recv_window: int = 65536
    #: Hard cap on a transport's write buffer; exceeding it is a budget failure.
    max_write_buffer: int = 16 * 1024 * 1024
    #: Retransmission check interval for stalled stream data.
    rto: float = 0.2
    #: A stream direction stalled this long is aborted with ``TimeoutError``.
    retransmit_timeout: float = 15.0
    #: ``connect()`` gives up with ``TimeoutError`` after this long.
    connect_timeout: float = 10.0
    #: ``process``: peers of a crashed node receive a reset (the OS closed the
    #: sockets). ``host``: the machine vanished silently; peers notice only
    #: when they send (timeout, or reset once the node is back up).
    crash_mode: str = "process"
    #: Message-bus (datagram-style) fault probabilities, used when the
    #: ``messages`` fault kind is enabled.
    msg_loss: float = 0.05
    msg_duplicate: float = 0.05
    #: Extra uniform jitter added per message, allowing reordering.
    msg_reorder_jitter: float = 0.020
    #: Maximum queued messages per message endpoint (excess is dropped).
    msg_queue_limit: int = 1024


@dataclass(frozen=True)
class FaultConfig:
    """Random fault-schedule generation parameters."""

    kinds: tuple[str, ...] = ()
    max_faults: int = 4
    #: Faults start uniformly within ``[window_start, window_end]`` virtual seconds.
    window_start: float = 0.1
    window_end: float = 10.0
    partition_duration: tuple[float, float] = (0.5, 5.0)
    crash_downtime: tuple[float, float] = (0.2, 3.0)
    latency_duration: tuple[float, float] = (0.5, 3.0)
    latency_factor: float = 20.0
    clock_offset_max: float = 2.0
    clock_drift_ppm_max: float = 500.0


@dataclass(frozen=True)
class Budgets:
    """Execution budgets. Exhausting one never counts as a pass."""

    max_steps: int = 2_000_000
    #: Steps allowed without virtual time advancing before declaring starvation.
    max_steps_without_progress: int = 200_000
    max_virtual_time: float = 600.0
    max_trace_events: int = 200_000
    #: Supervisory wall-clock timeout per run (``None`` disables it).
    wall_timeout: float | None = 60.0
    #: Steps allowed for deterministic teardown after the outcome is decided.
    teardown_steps: int = 100_000


@dataclass(frozen=True)
class SimConfig:
    """Complete semantic configuration of a simulation run."""

    nodes: tuple[str, ...] = ("n1", "n2", "n3")
    scheduler: str = "interleave"
    network: NetworkConfig = field(default_factory=NetworkConfig)
    faults: FaultConfig = field(default_factory=FaultConfig)
    budgets: Budgets = field(default_factory=Budgets)
    trace: str = "basic"
    #: ``strict``: ``os.urandom``/``secrets``/``SystemRandom`` raise.
    #: ``seeded``: they return deterministic bytes derived from the seed.
    entropy: str = "strict"
    #: ``reject``: ``run_in_executor``/``to_thread`` raise. ``inline``: the
    #: function runs synchronously on the simulation thread (changes
    #: concurrency semantics; opt-in).
    executor: str = "reject"
    #: Fail the run when a node's main coroutine raises.
    fail_on_node_error: bool = True
    #: Fail the run when a callback exception reaches the loop's exception handler.
    fail_on_loop_errors: bool = False
    #: Virtual wall-clock epoch for ``time.time()`` (2026-01-01T00:00:00Z).
    epoch: float = 1767225600.0

    def __post_init__(self) -> None:
        if self.scheduler not in SCHEDULERS:
            raise ConfigError(f"scheduler must be one of {SCHEDULERS}, got {self.scheduler!r}")
        if self.trace not in TRACE_LEVELS:
            raise ConfigError(f"trace must be one of {TRACE_LEVELS}, got {self.trace!r}")
        if self.entropy not in ("strict", "seeded"):
            raise ConfigError(f"entropy must be 'strict' or 'seeded', got {self.entropy!r}")
        if self.executor not in ("reject", "inline"):
            raise ConfigError(f"executor must be 'reject' or 'inline', got {self.executor!r}")
        if self.network.crash_mode not in CRASH_MODES:
            raise ConfigError(f"crash_mode must be one of {CRASH_MODES}")
        if len(set(self.nodes)) != len(self.nodes):
            raise ConfigError(f"duplicate node names in {self.nodes}")
        for name in self.nodes:
            if not name or not name.replace("-", "").replace("_", "").isalnum():
                raise ConfigError(f"invalid node name {name!r} (use letters, digits, - and _)")
            if name in ("control", "localhost"):
                raise ConfigError(f"node name {name!r} is reserved")
        if len(self.nodes) > 250:
            raise ConfigError("at most 250 nodes are supported")
        for k in self.faults.kinds:
            if k not in FAULT_KINDS:
                raise ConfigError(f"unknown fault kind {k!r}; expected one of {FAULT_KINDS}")
        n = self.network
        if not 0 <= n.latency_min <= n.latency_max:
            raise ConfigError("network latency must satisfy 0 <= latency_min <= latency_max")
        if n.segment_size <= 0 or n.recv_window < n.segment_size:
            raise ConfigError("segment_size must be positive and <= recv_window")
        if self.budgets.max_steps <= 0 or self.budgets.max_virtual_time <= 0:
            raise ConfigError("budgets must be positive")

    # -- canonical form -------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = _to_jsonable(dataclasses.asdict(self))
        return out

    def semantic_dict(self) -> dict[str, Any]:
        d = self.to_dict()
        d["budgets"].pop("wall_timeout", None)
        return d

    def fingerprint(self) -> str:
        blob = json.dumps(self.semantic_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode()).hexdigest()[:16]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SimConfig:
        data = dict(data)
        try:
            net = NetworkConfig(**data.pop("network", {}))
            fd = dict(data.pop("faults", {}))
            for key in ("kinds", "partition_duration", "crash_downtime", "latency_duration"):
                if key in fd:
                    fd[key] = tuple(fd[key])
            faults = FaultConfig(**fd)
            budgets = Budgets(**data.pop("budgets", {}))
            if "nodes" in data:
                data["nodes"] = tuple(data["nodes"])
            return cls(network=net, faults=faults, budgets=budgets, **data)
        except TypeError as exc:
            raise ConfigError(f"invalid configuration: {exc}") from None

    def with_overrides(self, **overrides: Any) -> SimConfig:
        """Return a copy with flat or dotted overrides, e.g. ``{"network.rto": 0.1}``."""
        return apply_overrides(self, overrides)


def _to_jsonable(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [_to_jsonable(v) for v in obj]
    return obj


def parse_faults(spec: str | tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    """Parse ``"partitions,crashes"`` (or ``"none"``/``"all"``) into fault kinds."""
    if spec is None:
        return ()
    if isinstance(spec, str):
        items = [s.strip() for s in spec.split(",") if s.strip()]
    else:
        items = list(spec)
    if items in (["none"], []):
        return ()
    if items == ["all"]:
        return FAULT_KINDS
    out: list[str] = []
    for item in items:
        if item not in FAULT_KINDS:
            raise ConfigError(f"unknown fault kind {item!r}; expected one of {FAULT_KINDS}")
        if item not in out:
            out.append(item)
    # Canonical order, independent of how the user spelled the list.
    return tuple(k for k in FAULT_KINDS if k in out)


def node_names(nodes: int | list[str] | tuple[str, ...]) -> tuple[str, ...]:
    if isinstance(nodes, int):
        if nodes < 0:
            raise ConfigError("nodes must be >= 0")
        return tuple(f"n{i}" for i in range(1, nodes + 1))
    return tuple(nodes)


_SECTIONS = {"network": NetworkConfig, "faults": FaultConfig, "budgets": Budgets}


def apply_overrides(cfg: SimConfig, overrides: dict[str, Any]) -> SimConfig:
    """Apply overrides. Keys may be top-level fields, ``section.field``, or a bare
    field name that is unique across sections (e.g. ``max_steps``)."""
    top: dict[str, Any] = {}
    sections: dict[str, dict[str, Any]] = {k: {} for k in _SECTIONS}
    field_owner: dict[str, str] = {}
    for sec, typ in _SECTIONS.items():
        for f in fields(typ):
            field_owner.setdefault(f.name, sec)
    top_names = {f.name for f in fields(SimConfig)}
    for key, value in overrides.items():
        if value is None and key not in (
            "bandwidth",
            "network.bandwidth",
            "wall_timeout",
            "budgets.wall_timeout",
        ):
            continue
        if "." in key:
            sec, name = key.split(".", 1)
            if sec not in _SECTIONS or name not in {f.name for f in fields(_SECTIONS[sec])}:
                raise ConfigError(f"unknown configuration key {key!r}")
            sections[sec][name] = value
        elif key == "faults" and not isinstance(value, FaultConfig):
            sections["faults"]["kinds"] = parse_faults(value)
        elif key == "nodes":
            top["nodes"] = node_names(value)
        elif key in top_names:
            top[key] = value
        elif key in field_owner:
            sections[field_owner[key]][key] = value
        else:
            raise ConfigError(f"unknown configuration key {key!r}")
    kwargs: dict[str, Any] = dict(top)
    for sec, vals in sections.items():
        if vals:
            if "kinds" in vals:
                vals["kinds"] = parse_faults(vals["kinds"])
            for tkey in ("partition_duration", "crash_downtime", "latency_duration"):
                if tkey in vals:
                    vals[tkey] = tuple(vals[tkey])
            base = kwargs.get(sec, getattr(cfg, sec))
            kwargs[sec] = replace(base, **vals)
    try:
        return replace(cfg, **kwargs)
    except TypeError as exc:
        raise ConfigError(str(exc)) from None
