# Benchmarks

Benchmarks are reproducible commands with raw JSON results committed under
[`benchmarks/results/`](../benchmarks/results/). They describe one machine and
one workload; they are never used as CI pass/fail thresholds.

## How to reproduce

```console
uv sync --group dev                      # or: pip install -e . pytest hypothesis
python benchmarks/bench.py               # all sections, ~6 minutes on 12 CPUs
python benchmarks/bench.py --quick       # smaller budgets
python benchmarks/bench.py --only corpus,baseline
python benchmarks/summarize.py benchmarks/results/<file>.json   # the tables below
python benchmarks/baseline_real_asyncio.py --bug lost-ack --trials 10
```

## Workloads

- **KV corrected baseline / bug variants**: `detloop.examples.kv_sim` — three
  Raft-style KV nodes, three clients doing 30% puts / 70% gets over four keys
  for 4 virtual seconds under up to four random partitions or crashes, then
  heal, final reads and a linearizability check. About 6.2–6.5 s of virtual
  time and ~6,000–7,000 loop steps per seed.
- **Scheduler micro-benchmark**: three nodes × 20 tasks exchanging 200
  messages each through `asyncio.Queue` (pure scheduling, no network).
- **8 MiB echo**: one connection, flow-controlled bulk transfer.
- **Real-asyncio baseline**: the *same* KV node code on the real event loop
  over localhost TCP; every directed link goes through a proxy adding a random
  1–10 ms delay per chunk; partitions hold proxy traffic; crashes cancel the
  node's tasks and abort its connections, then restart it with the same
  `disk`. Same clients, workload, final reads and checker.

## Results

<!-- generated: benchmarks/summarize.py -->
Source: [`benchmarks/results/2026-11-16-py3.12.3-linux.json`](../benchmarks/results/2026-11-16-py3.12.3-linux.json) — AMD Ryzen 5 5600H with Radeon Graphics, 12 logical CPUs, 7.1 GiB RAM, Linux-7.0.0-43-generic-x86_64-with-glibc2.39, cpython 3.12.3, detloop 0.1.0 (commit `160132b`), 2026-11-16T15:52:02+00:00.

### Throughput

| Workload | Seeds | Wall (s) | Seeds/s | Loop steps/s |
|---|---|---|---|---|
| KV corrected baseline, serial | 40 | 4.596 | 8.7 | 51,723 |
| KV corrected baseline, 12 workers | 240 | 4.694 | 51.13 | |
| 8 MiB echo over one connection | 3 | 0.166 | 18.09 | 41,671 |

### Event throughput and trace overhead

| Workload | Trace level | Steps/s | Trace events | Overhead vs `off` |
|---|---|---|---|---|
| scheduler micro-benchmark (60 tasks) | `off` | 194,026 | 10 | — |
| scheduler micro-benchmark (60 tasks) | `basic` | 198,708 | 80 | -2.4% |
| scheduler micro-benchmark (60 tasks) | `full` | 60,517 | 120,534 | 220.4% |
| KV corrected baseline | `off` | 64,267 | 10 | — |
| KV corrected baseline | `basic` | 52,573 | 16,292 | 22.3% |
| KV corrected baseline | `full` | 39,665 | 42,837 | 62.1% |

### Memory (tracemalloc peak, one run)

KV run, basic trace: 2.87 MiB; KV run, full trace: 3.31 MiB; scheduler micro-benchmark: 0.44 MiB.

### Bug corpus: seeds to first failure (300 seeds per variant)

| Variant | Failing seeds | Failure rate | First failing seed | Seeds to failure: median / p90 / max |
|---|---|---|---|---|
| `stale-read` | 199 | 66.3% | 1 | 1.0 / 3 / 9 |
| `lost-ack` | 44 | 14.7% | 0 | 4.5 / 11 / 18 |
| `split-brain` | 16 | 5.3% | 17 | 11 / 33 / 59 |
| corrected | 0 (of 300; 0 incomplete) | 0% | — | — |

Seeds-to-failure: for every start seed *s*, the number of seeds tried from *s* until the next failing seed (so `1` means *s* itself fails).

### Shrinking (budget 150 candidate runs)

| Variant | Seed | Status | Faults | Non-zero decisions | Steps | Trace events | Wall (s) |
|---|---|---|---|---|---|---|---|
| `stale-read` | 1 | budget_exhausted | 2 → 2 | 2753 → 2708 | 6613 → 6613 | 2271 → 2271 | 107.091 |
| `lost-ack` | 0 | budget_exhausted | 3 → 1 | 2820 → 2760 | 7306 → 7880 | 2212 → 2321 | 84.13 |
| `split-brain` | 17 | budget_exhausted | 1 → 0 | 452 → 401 | 1096 → 1096 | 348 → 348 | 8.237 |

### Baseline: real asyncio with random sleeps (20 trials per variant, 6 in parallel)

| Variant | Detected | Detection rate | Mean wall time per trial (s) | Simulated failure rate per seed |
|---|---|---|---|---|
| `corrected` | 0/20 | 0% | 6.997 | 0% |
| `stale-read` | 18/20 | 90% | 6.209 | 66.3% |
| `lost-ack` | 3/20 | 15% | 6.487 | 14.7% |
| `split-brain` | 0/20 | 0% | 6.62 | 5.3% |
<!-- end generated -->

## Interpretation and limits

- **Per-trial detection is not where detloop wins on this workload.** The
  real-asyncio baseline with random sleeps detected stale reads more often per
  trial (90% vs 66% of seeds) and lost acknowledged writes about as often (15%
  vs 15%); it found split brain in 0 of 20 trials versus 5.3% of simulated
  seeds. Its timing noise comes from OS scheduling plus an extra proxy hop per
  link.
- **The baseline is not reproducible.** Its rates change from run to run (an
  earlier, unrecorded run during development detected more lost-ack and
  split-brain cases); the simulated numbers above are identical on every run
  because every seed is deterministic.
- **Cost per trial and replayability are the advantage.** A simulated seed
  costs about 0.11 s of wall time serially (~51 seeds/s on 12 workers) against
  ~6.2–7.0 s per real trial, because virtual time skips idle periods. Every
  simulated failure replays exactly from its seed and can be shrunk; a
  real-asyncio failure cannot be replayed at all.
- Expected serial wall time to a first simulated detection, from the measured
  rates: `stale-read` ≈ 0.2 s, `lost-ack` ≈ 0.8 s, `split-brain` ≈ 2 s.
- **Trace overhead** depends on event density: `basic` is within measurement
  noise for pure scheduling but costs ~22% on the network-heavy KV workload;
  `full` (per-segment and per-decision events) costs 60–220%. Exploration
  should use `basic` (the default) or `off`; replays can re-record at `full`.
- **Shrinking** removed most faults (lost-ack 3 → 1, split-brain 1 → 0) but
  stopped on its 150-run budget while simplifying network and scheduling
  tapes: zeroing latency decisions makes messages faster, so the fixed 4-second
  workload performs more operations and runs get longer, not shorter. For
  stale reads no single fault could be removed while preserving the failure.
  Shrinking is reported as `budget_exhausted`, never as minimal.
- The simulated network has instantaneous acknowledgements and no congestion
  control; bulk-transfer numbers are simulator throughput, not network
  realism.
- Python overhead dominates: ~40,000–200,000 loop steps per second per core.
