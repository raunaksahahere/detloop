"""PRNG, stream derivation and decision tapes."""

from __future__ import annotations

import collections

import pytest

from detloop.errors import ConfigError, ReplayDivergence
from detloop.rng import Decisions, SimRandom, SplitMix64, Tape, derive_seed


def test_splitmix64_reference_vectors() -> None:
    # Published SplitMix64 outputs (seed 1234567) and the seed-0 first output.
    g = SplitMix64(1234567)
    assert [g.next64() for _ in range(5)] == [
        6457827717110365317,
        3203168211198807973,
        9817491932198370423,
        4593380528125082431,
        16408922859458223821,
    ]
    assert SplitMix64(0).next64() == 0xE220A8397B1DCDAF


def test_stream_derivation_is_stable_and_independent() -> None:
    # These values are part of the detloop-prng/1 format; changing them breaks replay.
    assert derive_seed(0, "sched") == 3647518648884899269
    assert derive_seed(42, "net") == 11584402684542238276
    assert len({derive_seed(7, s) for s in ("sched", "net", "workload", "faults", "entropy")}) == 5
    with pytest.raises(ConfigError):
        derive_seed(-1, "sched")


def test_below_range_and_uniformity() -> None:
    g = SplitMix64(99)
    counts = collections.Counter(g.below(6) for _ in range(60_000))
    assert set(counts) == set(range(6))
    for c in counts.values():
        assert abs(c - 10_000) < 600
    assert g.below(1) == 0


def test_seeded_decisions_record_tapes_and_skip_trivial_choices() -> None:
    d = Decisions(5)
    vals = [d.choose("net", 10) for _ in range(20)]
    assert d.choose("net", 1) == 0  # free: not recorded
    assert d.recorded["net"].values == vals
    assert d.recorded["net"].bounds == [10] * 20
    assert d.total_decisions() == 20


def test_streams_do_not_shift_each_other() -> None:
    a, b = Decisions(11), Decisions(11)
    wa = [a.choose("workload", 1000) for _ in range(10)]
    for _ in range(50):  # extra draws on another stream
        b.choose("net", 7)
    wb = [b.choose("workload", 1000) for _ in range(10)]
    assert wa == wb


def test_strict_replay_detects_divergence() -> None:
    d = Decisions(3)
    for n in (4, 5, 6):
        d.choose("sched", n)
    tapes = {"sched": d.recorded["sched"]}
    r = Decisions(3, mode="strict", tapes=tapes)
    assert [r.choose("sched", n) for n in (4, 5, 6)] == d.recorded["sched"].values
    r = Decisions(3, mode="strict", tapes=tapes)
    r.choose("sched", 4)
    with pytest.raises(ReplayDivergence, match="recorded 5 options but execution offers 9"):
        r.choose("sched", 9)
    r = Decisions(3, mode="strict", tapes=tapes)
    for n in (4, 5, 6):
        r.choose("sched", n)
    with pytest.raises(ReplayDivergence, match="tape exhausted"):
        r.choose("sched", 2)


def test_guided_mode_normalizes_and_defaults_to_zero() -> None:
    d = Decisions(0, mode="guided", tapes={"net": Tape([], [7, 3])})
    assert d.choose("net", 5) == 2  # 7 mod 5
    assert d.choose("net", 5) == 3
    assert d.choose("net", 5) == 0  # past the end
    assert d.recorded["net"].values == [2, 3, 0]


def test_chance_zero_means_no_event() -> None:
    d = Decisions(0, mode="guided", tapes={"workload": Tape([], [0])})
    assert d.chance("workload", 0.99) is False
    assert Decisions(0).chance("workload", 0.0) is False
    assert Decisions(0).chance("workload", 1.0) is True


def test_simrandom_api_is_deterministic() -> None:
    def draw(seed: int) -> list[object]:
        r = SimRandom(Decisions(seed))
        items = list(range(10))
        r.shuffle(items)
        return [r.randint(1, 6), r.choice("abc"), round(r.random(), 12), r.uniform(1, 2) < 2,
                r.getrandbits(40) < 2**40, items, r.sample(range(100), 3), r.randrange(0, 10, 3)]

    assert draw(1) == draw(1)
    assert draw(1) != draw(2)
    r = SimRandom(Decisions(0))
    with pytest.raises(IndexError):
        r.choice([])
    with pytest.raises(ValueError):
        r.randrange(5, 5)


def test_decision_mode_validation() -> None:
    with pytest.raises(ConfigError):
        Decisions(0, mode="bogus")
    with pytest.raises(ConfigError):
        Decisions(0, mode="strict")
    with pytest.raises(ValueError):
        Decisions(0).choose("net", 0)
