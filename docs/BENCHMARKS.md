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
Source: [`benchmarks/results/2026-11-16-py3.12.3-linux.json`](../benchmarks/results/2026-11-16-py3.12.3-linux.json) — AMD Ryzen 5 5600H with Radeon Graphics, 12 logical CPUs, 7.1 GiB RAM, Linux-7.0.0-43-generic-x86_64-with-glibc2.39, cpython 3.12.3, detloop 0.1.0 (commit `35cd060`), 2026-11-16T15:04:12+00:00.

### Throughput

| Workload | Seeds | Wall (s) | Seeds/s | Loop steps/s |
|---|---|---|---|---|
| KV corrected baseline, serial | 40 | 4.605 | 8.69 | 51,626 |
| KV corrected baseline, 12 workers | 240 | 4.669 | 51.4 | |
| 8 MiB echo over one connection | 3 | 0.155 | 19.4 | 44,686 |

### Event throughput and trace overhead

| Workload | Trace level | Steps/s | Trace events | Overhead vs `off` |
|---|---|---|---|---|
| scheduler micro-benchmark (60 tasks) | `off` | 204,842 | 10 | — |
| scheduler micro-benchmark (60 tasks) | `basic` | 199,548 | 80 | 2.6% |
| scheduler micro-benchmark (60 tasks) | `full` | 63,660 | 120,534 | 221.7% |
| KV corrected baseline | `off` | 64,669 | 10 | — |
| KV corrected baseline | `basic` | 52,185 | 16,292 | 23.9% |
| KV corrected baseline | `full` | 39,352 | 42,837 | 64.3% |

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
| `stale-read` | 1 | budget_exhausted | 2 → 2 | 2753 → 2708 | 6613 → 6613 | 2271 → 2271 | 112.199 |
| `lost-ack` | 0 | budget_exhausted | 3 → 1 | 2820 → 2760 | 7306 → 7880 | 2212 → 2321 | 88.095 |
| `split-brain` | 17 | budget_exhausted | 1 → 0 | 452 → 401 | 1096 → 1096 | 348 → 348 | 8.625 |

### Baseline: real asyncio with random sleeps (20 trials per variant, 6 in parallel)

| Variant | Detected | Detection rate | Mean wall time per trial (s) | Simulated failure rate per seed |
|---|---|---|---|---|
| `corrected` | 0/20 | 0% | 6.59 | 0% |
| `stale-read` | 18/20 | 90% | 6.204 | 66.3% |
| `lost-ack` | 6/20 | 30% | 6.555 | 14.7% |
| `split-brain` | 3/20 | 15% | 6.644 | 5.3% |
<!-- end generated -->

## Interpretation and limits

- **Per-trial detection is not where detloop wins on this workload.** The
  real-asyncio baseline with random sleeps detected each bug at a similar or
  higher rate per trial. Its timing noise comes from OS scheduling and an
  extra proxy hop per link, which this toy store happens to be sensitive to.
- **Cost per trial and reproducibility are.** A simulated seed costs about
  0.11 s of wall time serially (and scales across processes), against ~6.5 s
  per real trial, because virtual time skips idle periods. Every simulated
  failure replays exactly from its seed and can be shrunk; a real-asyncio
  failure cannot be replayed at all.
- Expected wall time to a first detection, from the measured rates:
  `stale-read` ≈ 0.2 s simulated vs ≈ 7 s real; `lost-ack` ≈ 0.8 s vs ≈ 22 s;
  `split-brain` ≈ 2 s vs ≈ 44 s (serial simulated seeds vs sequential real
  trials; both can be parallelized).
- **Trace overhead** depends on event density: `basic` costs little for pure
  scheduling but ~24% on the network-heavy KV workload; `full` (per-segment and
  per-decision events) costs 64–220%. Exploration should use `basic` (the
  default) or `off`; replays can re-record at `full`.
- **Shrinking** removed most faults (lost-ack 3 → 1, split-brain 1 → 0) but
  stopped on its 150-run budget while simplifying network and scheduling
  tapes: zeroing latency decisions makes messages faster, so the fixed 4-second
  workload performs more operations and runs get longer, not shorter.
  Shrinking is reported as `budget_exhausted`, never as minimal.
- The simulated network has instantaneous acknowledgements and no congestion
  control; bulk-transfer numbers are simulator throughput, not network
  realism.
- Python overhead dominates: ~50,000–200,000 loop steps per second per core.
