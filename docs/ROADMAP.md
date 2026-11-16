# Roadmap

## In 0.1.0 (implemented and tested)

Virtual-time event loop with per-node FIFO scheduling and seeded interleaving;
`fifo` and opt-in `relaxed` schedulers; budgets and deadlock/starvation
detection; seeded decision streams with replayable tapes; determinism guards;
multi-node worlds with crash generations, restarts, persistent `disk`, clock
skew; simulated TCP-like streams with flow control, partitions, resets and two
crash modes; lossy message bus; operation histories and a bounded
linearizability checker; serial and parallel exploration with SQLite
aggregation; delta-debugging shrinker with strict replay; pytest plugin; CLI;
offline HTML report; aiohttp/httpx compatibility; toy Raft-style KV store with
three planted bugs.

## Not in 0.1.0 (future work, not promised)

| Area | Notes |
|---|---|
| PCT / coverage-guided exploration | Probabilistic Concurrency Testing priorities; seed selection guided by coverage or trace novelty. Today exploration is a uniform random walk over decisions |
| Better tape shrinking | Block-wise zeroing, value halving, and workload-aware passes; today decision shrinking often stops on its budget |
| Full disk simulation | Filesystem, fsync semantics, write reordering, torn writes, disk-full errors. Today `node.disk` is a dict that is durable on assignment |
| Datagram endpoints | `create_datagram_endpoint` on top of the message model |
| TLS | Would require a deterministic TLS stand-in; real OpenSSL entropy is uncontrolled |
| uvloop shims | Compatibility shims for code that requires uvloop APIs |
| Native acceleration | Rust (PyO3) or Cython hot paths for the scheduler and network |
| IPv6, UNIX sockets | Addressing and transports |
| Intra-node interleaving | Exploring task orders within one node without breaking asyncio's FIFO guarantee (today only via timing, or the opt-in `relaxed` mode) |
| More checkers and models | Queues, sets, sequential consistency, per-session guarantees |
| Report | Trace diffing between two runs, GIF/screenshot export |
| Hypothesis integration | Drive workloads from Hypothesis strategies |

Priorities will be set by user feedback; open an issue with a concrete use
case.
