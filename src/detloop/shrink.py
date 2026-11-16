"""Delta-debugging shrinker.

Input: a failing run (seed, config, optionally explicit faults/tapes).
Output: a smaller run with the same failure *signature*, replayable in
``strict`` decision mode.

Phases, repeated until a full round makes no progress (a local fixed point)
or the budget is exhausted:

1. **Faults**: ddmin over the explicit fault list (each fault includes its own
   recovery).
2. **Decision tapes** (``workload``, ``net``, ``sched``): truncate (binary
   search on the prefix length; decisions past the end default to 0), then
   ddmin over the positions of non-zero decisions, zeroing the rest.

Every candidate is *executed* in ``guided`` mode and accepted only if it fails
with the original signature; we never assume that deleting decisions leaves
the remaining schedule intact. The result is 1-minimal with respect to these
transformations, not globally minimal.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

from .api import Spec
from .config import SimConfig
from .errors import ReplayMismatch
from .faults import Fault, schedule_from_json
from .rng import Tape

#: Scheduling and network decisions are simplified before workload decisions,
#: which tend to change what the test does rather than how it interleaves.
SHRINK_ORDER = ("sched", "net", "workload")
from .runner import PASS, RunResult, run

T = TypeVar("T")
_perf = time.perf_counter


@dataclass
class ShrinkResult:
    original: RunResult
    final: RunResult
    runs: int
    status: str  # fixed_point | budget_exhausted
    seconds: float
    faults: list[Fault] = field(default_factory=list)
    tapes: dict[str, Tape] = field(default_factory=dict)
    #: Config of the shrunk run (budgets tightened relative to the original).
    config: SimConfig | None = None

    def metrics(self) -> dict[str, Any]:
        def nz(r: RunResult) -> int:
            return sum(sum(1 for v in t["values"] if v) for t in r.tapes.values())

        o, f = self.original, self.final
        return {
            "status": self.status,
            "signature": f.signature,
            "runs": self.runs,
            "seconds": round(self.seconds, 3),
            "faults": [len(o.faults), len(f.faults)],
            "nonzero_decisions": [nz(o), nz(f)],
            "decisions": [o.decisions, f.decisions],
            "steps": [o.steps, f.steps],
            "virtual_time": [o.virtual_ns / 1e9, f.virtual_ns / 1e9],
            "trace_events": [o.trace_events, f.trace_events],
            "globally_minimal": False,
        }


class _Budget(Exception):
    pass


class Shrinker:
    def __init__(
        self,
        spec: Spec,
        config: SimConfig,
        seed: int,
        *,
        max_runs: int = 300,
        max_seconds: float = 300.0,
        log: Callable[[str], None] | None = None,
    ) -> None:
        self.spec = spec
        self.config = config
        self.seed = seed
        self.max_runs = max_runs
        self.max_seconds = max_seconds
        self.log = log or (lambda msg: None)
        self.runs = 0
        self._deadline = 0.0
        self.signature = ""

    def _run(self, faults: list[Fault], tapes: dict[str, Tape] | None, mode: str) -> RunResult:
        if self.runs >= self.max_runs or _perf() > self._deadline:
            raise _Budget
        self.runs += 1
        return run(self.spec.fn, seed=self.seed, config=self.config, faults=faults, tapes=tapes,
                   decision_mode=mode, keep_trace=False)

    def _accept(self, r: RunResult) -> bool:
        return r.status != PASS and r.signature == self.signature

    def shrink(self, faults: list[Fault] | None = None, tapes: dict[str, Tape] | None = None) -> ShrinkResult:
        start = _perf()
        self._deadline = start + self.max_seconds
        mode = "guided" if tapes is not None else "seeded"
        original = run(self.spec.fn, seed=self.seed, config=self.config, faults=faults, tapes=tapes,
                       decision_mode=mode, keep_trace=False)
        self.runs = 1
        if original.status == PASS:
            raise ReplayMismatch(f"seed {self.seed} passes; nothing to shrink")
        self.signature = original.signature
        # A candidate that runs much longer than the original is not "smaller";
        # bound candidates so a pathological candidate cannot eat the budget.
        b = self.config.budgets
        self.config = self.config.with_overrides(
            max_steps=min(b.max_steps, 2 * original.steps + 1000),
            max_virtual_time=min(b.max_virtual_time, 2 * original.virtual_ns / 1e9 + 1.0),
        )
        cur_faults = schedule_from_json(original.faults)
        cur_tapes = {s: Tape.from_json(t) for s, t in original.tapes.items()}
        best = original
        status = "fixed_point"
        try:
            while True:
                progress = False
                # Phase 1: faults
                if cur_faults:
                    kept = self._ddmin(cur_faults, lambda fs: self._try(fs, cur_tapes))
                    if len(kept) < len(cur_faults):
                        r = self._last_ok
                        cur_faults, cur_tapes, best = kept, self._tapes_of(r), r
                        progress = True
                        self.log(f"faults -> {len(cur_faults)}")
                # Phase 2: decision tapes
                for stream in SHRINK_ORDER:
                    tape = cur_tapes.get(stream)
                    if tape is None or not tape.nonzero():
                        continue
                    new = self._shrink_tape(stream, cur_faults, cur_tapes)
                    if new is not None:
                        cur_tapes, best = self._tapes_of(new), new
                        progress = True
                        self.log(f"{stream}: nonzero decisions -> {cur_tapes[stream].nonzero()}")
                if not progress:
                    break
        except _Budget:
            status = "budget_exhausted"
        # Final: strict replay of the chosen candidate must reproduce it exactly.
        final = run(self.spec.fn, seed=self.seed, config=self.config, faults=cur_faults, tapes=cur_tapes,
                    decision_mode="strict", keep_trace=True)
        if final.signature != self.signature or final.trace_sha256 != best.trace_sha256:
            raise ReplayMismatch(
                "strict replay of the shrunk run did not reproduce it "
                f"(signature {final.signature!r} vs {self.signature!r}); this indicates nondeterminism "
                "in the code under test or a detloop bug"
            )
        result = ShrinkResult(original, final, self.runs + 1, status, _perf() - start, cur_faults, cur_tapes)
        result.config = self.config
        return result

    # -- helpers --------------------------------------------------------------
    _last_ok: RunResult

    @staticmethod
    def _tapes_of(r: RunResult) -> dict[str, Tape]:
        return {s: Tape.from_json(t) for s, t in r.tapes.items()}

    def _try(self, faults: list[Fault], tapes: dict[str, Tape]) -> bool:
        r = self._run(faults, tapes, "guided")
        if self._accept(r):
            self._last_ok = r
            return True
        return False

    def _ddmin(self, items: list[T], test: Callable[[list[T]], bool]) -> list[T]:
        """Classic ddmin (Zeller & Hildebrandt) returning a 1-minimal subset."""
        if not items:
            return items
        if test([]):
            return []
        n = 2
        while len(items) >= 2:
            size = max(1, len(items) // n)
            chunks = [items[i:i + size] for i in range(0, len(items), size)]
            reduced = False
            for i in range(len(chunks)):
                complement = [x for j, c in enumerate(chunks) if j != i for x in c]
                if test(complement):
                    items = complement
                    n = max(n - 1, 2)
                    reduced = True
                    break
            if not reduced:
                if n >= len(items):
                    break
                n = min(len(items), n * 2)
        if len(items) == 1 and test([]):
            return []
        return items

    def _with_tape(self, tapes: dict[str, Tape], stream: str, values: list[int]) -> dict[str, Tape]:
        out = dict(tapes)
        out[stream] = Tape([], values)
        return out

    def _shrink_tape(self, stream: str, faults: list[Fault], tapes: dict[str, Tape]) -> RunResult | None:
        values = list(tapes[stream].values)
        improved: RunResult | None = None
        # truncation: smallest prefix that still fails
        lo, hi = 0, len(values)
        while lo < hi:
            mid = (lo + hi) // 2
            if self._try(faults, self._with_tape(tapes, stream, values[:mid])):
                hi = mid
                improved = self._last_ok
            else:
                lo = mid + 1
        if improved is not None:
            tapes = self._tapes_of(improved)
            values = list(tapes[stream].values)
        # zero out non-zero decisions
        positions = [i for i, v in enumerate(values) if v]
        if positions:
            base = values

            def keep(subset: list[int]) -> bool:
                keep_set = set(subset)
                cand = [v if (i in keep_set) else 0 for i, v in enumerate(base)]
                return self._try(faults, self._with_tape(tapes, stream, cand))

            kept = self._ddmin(positions, keep)
            if len(kept) < len(positions):
                improved = self._last_ok
        return improved


def shrink(
    spec: Spec,
    seed: int,
    *,
    config: SimConfig | None = None,
    faults: list[Fault] | None = None,
    tapes: dict[str, Tape] | None = None,
    max_runs: int = 300,
    max_seconds: float = 300.0,
    log: Callable[[str], None] | None = None,
) -> ShrinkResult:
    """Shrink the failure of ``spec`` at ``seed`` (see module docs)."""
    shrinker = Shrinker(spec, config or spec.config, seed, max_runs=max_runs, max_seconds=max_seconds, log=log)
    return shrinker.shrink(faults, tapes)
