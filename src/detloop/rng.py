"""Deterministic randomness: PRNG, stream derivation, and decision tapes.

Algorithm ``detloop-prng/1`` (normative, see docs/DETERMINISM.md):

* Stream seed: the first 8 bytes (big-endian) of
  ``SHA-256("detloop/prng/v1/<master-seed-decimal>/<stream-name>")``.
* Generator: SplitMix64 over that 64-bit state.
* Bounded choice ``below(n)``: rejection sampling on 64-bit outputs with
  ``limit = 2**64 - (2**64 mod n)``; returns ``x mod n``. ``n == 1`` consumes
  nothing and is not recorded.

Every random decision in a simulation is an integer choice in ``[0, n)``
recorded on a per-stream *tape* as ``(n, value)``. Independent streams mean a
change in one subsystem (for example removing a fault) does not shift the
draws of another (for example the workload).

Value ``0`` is always the *least eventful* option (minimum latency, no loss,
first candidate, ``random() == 0.0``), which lets the shrinker simplify a run by
zeroing decisions.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, MutableSequence, Sequence
from typing import Any, TypeVar

from .errors import ConfigError, ReplayDivergence

T = TypeVar("T")

MASK64 = (1 << 64) - 1
_GOLDEN = 0x9E3779B97F4A7C15

#: Decision streams. The order is part of the format (used for reporting only).
STREAMS = ("sched", "net", "workload", "faults")
#: Streams whose decisions are recorded on tapes and can be replayed / shrunk.
TAPED_STREAMS = ("sched", "net", "workload")


def derive_seed(master: int, stream: str) -> int:
    """Derive the 64-bit seed for ``stream`` from ``master`` (stable across runtimes)."""
    if master < 0:
        raise ConfigError(f"seeds must be non-negative integers, got {master}")
    digest = hashlib.sha256(f"detloop/prng/v1/{master}/{stream}".encode("ascii")).digest()
    return int.from_bytes(digest[:8], "big")


class SplitMix64:
    """SplitMix64 generator (Steele, Lea & Flood 2014). Pure Python, fully specified."""

    __slots__ = ("state",)

    def __init__(self, seed: int) -> None:
        self.state = seed & MASK64

    def next64(self) -> int:
        self.state = (self.state + _GOLDEN) & MASK64
        z = self.state
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK64
        return z ^ (z >> 31)

    def below(self, n: int) -> int:
        """Uniform integer in ``[0, n)`` for ``1 <= n <= 2**64``."""
        if n <= 1:
            return 0
        limit = (1 << 64) - ((1 << 64) % n)
        while True:
            x = self.next64()
            if x < limit:
                return x % n

    def bytes(self, k: int) -> bytes:
        out = bytearray()
        while len(out) < k:
            out += self.next64().to_bytes(8, "big")
        return bytes(out[:k])


class Tape:
    """Recorded decisions of one stream: parallel lists of bounds and values."""

    __slots__ = ("bounds", "values")

    def __init__(self, bounds: list[int] | None = None, values: list[int] | None = None) -> None:
        self.bounds: list[int] = bounds if bounds is not None else []
        self.values: list[int] = values if values is not None else []

    def __len__(self) -> int:
        return len(self.values)

    def to_json(self) -> dict[str, list[int]]:
        return {"bounds": list(self.bounds), "values": list(self.values)}

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Tape:
        bounds = [int(x) for x in data.get("bounds", [])]
        values = [int(x) for x in data["values"]]
        if bounds and len(bounds) != len(values):
            raise ConfigError("tape bounds and values have different lengths")
        return cls(bounds, values)

    def nonzero(self) -> int:
        return sum(1 for v in self.values if v)


class Decisions:
    """Source of every random decision in a run.

    Modes:

    ``seeded``
        Draw from per-stream SplitMix64 generators; record a tape.
    ``strict``
        Replay recorded tapes exactly. Any mismatch in the number of options
        or running past the end of a tape raises :class:`ReplayDivergence`
        instead of silently choosing something else.
    ``guided``
        Used by the shrinker: take values from candidate tapes, reducing
        out-of-range values modulo ``n`` and using ``0`` past the end. The
        normalized decisions actually taken are recorded, so the result can be
        replayed in ``strict`` mode.
    """

    def __init__(
        self,
        seed: int,
        *,
        mode: str = "seeded",
        tapes: dict[str, Tape] | None = None,
    ) -> None:
        if mode not in ("seeded", "strict", "guided"):
            raise ConfigError(f"unknown decision mode {mode!r}")
        if mode != "seeded" and tapes is None:
            raise ConfigError(f"decision mode {mode!r} requires tapes")
        self.seed = seed
        self.mode = mode
        self._gens = {s: SplitMix64(derive_seed(seed, s)) for s in STREAMS}
        self._input = tapes or {}
        self._pos = dict.fromkeys(TAPED_STREAMS, 0)
        self.recorded: dict[str, Tape] = {s: Tape() for s in TAPED_STREAMS}

    def choose(self, stream: str, n: int, label: str = "") -> int:
        """Return a decision in ``[0, n)`` on ``stream``. ``n == 1`` is free."""
        if n <= 1:
            if n < 1:
                raise ValueError(f"choose() needs at least one option (stream {stream}, {label})")
            return 0
        if stream not in self.recorded:
            # Untaped stream (fault generation): always seeded.
            return self._gens[stream].below(n)
        if self.mode == "seeded":
            v = self._gens[stream].below(n)
        else:
            i = self._pos[stream]
            self._pos[stream] = i + 1
            tape = self._input.get(stream)
            if self.mode == "strict":
                if tape is None or i >= len(tape.values):
                    raise ReplayDivergence(
                        stream, i, f"tape exhausted while choosing among {n} options ({label})"
                    )
                if tape.bounds and tape.bounds[i] != n:
                    raise ReplayDivergence(
                        stream,
                        i,
                        f"recorded {tape.bounds[i]} options but execution offers {n} ({label})",
                    )
                v = tape.values[i]
                if not 0 <= v < n:
                    raise ReplayDivergence(stream, i, f"recorded value {v} outside [0, {n})")
            else:  # guided
                v = tape.values[i] if tape is not None and i < len(tape.values) else 0
                if v >= n or v < 0:
                    v %= n
        rec = self.recorded[stream]
        rec.bounds.append(n)
        rec.values.append(v)
        return v

    def chance(self, stream: str, p: float, label: str = "") -> bool:
        """Bernoulli trial with probability ``p``; decision ``0`` means *no event*."""
        if p <= 0.0:
            return False
        if p >= 1.0:
            return True
        scale = 1_000_000
        threshold = scale - round(p * scale)
        return self.choose(stream, scale, label) >= threshold

    def generator(self, stream: str) -> SplitMix64:
        """Raw generator for untaped uses (fault-schedule generation)."""
        return self._gens[stream]

    def entropy_seed(self) -> int:
        """Seed for the patched ``random`` module and seeded ``os.urandom``."""
        return derive_seed(self.seed, "entropy")

    def tapes_json(self) -> dict[str, dict[str, list[int]]]:
        return {s: t.to_json() for s, t in self.recorded.items()}

    def total_decisions(self) -> int:
        return sum(len(t) for t in self.recorded.values())


class SimRandom:
    """A :class:`random.Random`-like API whose every draw is a recorded decision.

    Obtain one with :attr:`World.rng`. All draws go to the ``workload`` stream,
    so they are replayable and shrinkable.
    """

    def __init__(self, decisions: Decisions, stream: str = "workload") -> None:
        self._d = decisions
        self._stream = stream

    def _below(self, n: int, label: str) -> int:
        return self._d.choose(self._stream, n, label)

    def randrange(self, start: int, stop: int | None = None, step: int = 1) -> int:
        if stop is None:
            start, stop = 0, start
        if step <= 0:
            raise ValueError("randrange() step must be positive")
        width = (stop - start + step - 1) // step
        if width <= 0:
            raise ValueError(f"empty range for randrange({start}, {stop}, {step})")
        return start + step * self._below(width, "randrange")

    def randint(self, a: int, b: int) -> int:
        return self.randrange(a, b + 1)

    def choice(self, seq: Sequence[T]) -> T:
        if not seq:
            raise IndexError("cannot choose from an empty sequence")
        return seq[self._below(len(seq), "choice")]

    def random(self) -> float:
        return self._below(1 << 53, "random") / float(1 << 53)

    def uniform(self, a: float, b: float) -> float:
        return a + (b - a) * self.random()

    def chance(self, p: float) -> bool:
        """``True`` with probability ``p`` (shrinks toward ``False``)."""
        return self._d.chance(self._stream, p, "chance")

    def getrandbits(self, k: int) -> int:
        if k <= 0:
            return 0
        out = 0
        while k > 0:
            take = min(k, 32)
            out = (out << take) | self._below(1 << take, "getrandbits")
            k -= take
        return out

    def shuffle(self, x: MutableSequence[Any]) -> None:
        for i in range(len(x) - 1, 0, -1):
            j = self._below(i + 1, "shuffle")
            x[i], x[j] = x[j], x[i]

    def sample(self, population: Iterable[T], k: int) -> list[T]:
        pool = list(population)
        if not 0 <= k <= len(pool):
            raise ValueError("sample larger than population or negative")
        out: list[T] = []
        for _ in range(k):
            out.append(pool.pop(self._below(len(pool), "sample")))
        return out
