# Fault model

## Topology and addressing

- Configured nodes `n1..nN` have addresses `10.0.0.1..` and resolvable
  names. The control node (test body) is `10.0.0.254`, named `control`.
- `127.0.0.1`/`localhost` is the current node's loopback (10 µs latency,
  never partitioned).
- Links are **directed**: `(a, b)` can be blocked independently of `(b, a)`.
  Blocks are reference-counted, so overlapping partitions compose and heal
  independently.

## Streams (TCP-like)

Streams are reliable and ordered: applications never see duplicated,
reordered or silently missing bytes. Faults happen *below* the stream and
surface as stalls, resets and timeouts.

| Aspect | Model |
|---|---|
| Segmentation | Writes are cut into segments of at most `segment_size` (16 KiB) |
| Latency | Per segment, uniform in `[latency_min, latency_max]` (1–10 ms) at µs resolution, multiplied by active latency faults |
| Bandwidth | Optional per directed link; serialization delay `len / bandwidth` |
| Ordering | Arrival times are forced non-decreasing per direction |
| Acknowledgement | Implicit and instantaneous at delivery |
| Receiver window | In-flight bytes + bytes buffered while the receiver paused reading ≤ `recv_window` (64 KiB) |
| Write buffer | Unsent + unacknowledged bytes; drives `pause_writing`/`resume_writing` (asyncio defaults 64 KiB / 16 KiB) and so `drain()`. Exceeding `max_write_buffer` (16 MiB) fails the run as `budget:buffer` |
| `write_eof()` | FIN after pending data; the peer gets `eof_received()`; the writer can still read |
| `close()` | FIN after pending data; `connection_lost(None)` once the local buffer has been handed to the network; data later arriving at the closed socket triggers a reset |
| `abort()` | Discards buffers; local `connection_lost(None)`; peer gets `ConnectionResetError` |

### Connection establishment

- SYN to a host whose OS answers but has no listener (never-started node, or
  crashed node in `process` mode) → `ConnectionRefusedError` after one RTT.
- SYN lost (blocked link, down host in `host` mode, unknown address) → retried
  at 1, 3, 7 s ...; `TimeoutError` after `connect_timeout` (10 s).
- A SYN-ACK lost after the server accepted → the client retries; the server
  answers duplicates; a client that gave up resets the server side.

### Partitions and in-flight data

1. A segment that arrives while its directed link is blocked is **lost**; the
   direction becomes **stalled** and every later segment in that direction is
   held back as well (no reordering).
2. The sender's retransmission check (`rto`, 200 ms) resends all
   unacknowledged data, in order, once the link is usable.
3. A direction stalled longer than `retransmit_timeout` (15 s) aborts the
   **sender** with `TimeoutError` (`ETIMEDOUT`). The receiver of an idle
   half-open connection notices nothing until it sends.
4. Healing therefore delivers held data in order, without loss or duplication.
5. Control packets (SYN, SYN-ACK, RST) on a blocked link are simply lost.

### Resets

`world.reset_connections(a, b)` and the `resets` fault abort both endpoints
of every live connection between `a` and `b` with `ConnectionResetError`.

## Messages (datagram-style)

`node.messages(port)` endpoints are the application-visible lossy model:
independent latency per message (reordering possible even without faults),
and with the `messages` fault kind: loss (`msg_loss`, 5%), duplication
(`msg_duplicate`, 5%) and extra jitter (`msg_reorder_jitter`, 20 ms).
Messages to blocked links, down nodes, unbound ports or full queues
(`msg_queue_limit`) are dropped and traced. Payloads are `bytes`.

## Crashes and restarts

A crash ends an **incarnation** (`n1#1`, `n1#2`, ...):

- Every task, timer and callback owned by the incarnation, including
  descendants created by service code, is never executed again. Because each
  task step is a loop handle, this holds even for code that suppresses
  `CancelledError`. Frozen tasks are disposed of at world teardown, after the
  outcome is decided, with all effects discarded.
- Its listening ports are released; message endpoints are closed.
- Its connections are dropped locally without callbacks. Peers see:
  - `crash_mode="process"` (default): the OS closed the sockets — a reset
    reaches the peer (if the link allows), and new connections are refused.
  - `crash_mode="host"`: the machine vanished — idle peers notice nothing;
    peers that send time out; after the node restarts its OS answers stale
    connections with resets.
- Packets in flight to the old incarnation are dropped on arrival: a late
  packet or callback can never reach a new incarnation.
- `restart()` creates a new incarnation and runs the stored factory again with
  a fresh `volatile` dict.

### What survives a crash

| Survives | Lost |
|---|---|
| `node.disk` (a plain `dict`; every assignment is durable immediately) | `node.volatile`, all in-memory objects created by the incarnation, its sockets, tasks and timers |

There is **no disk model**: no fsync, no write ordering, no torn writes, no
filesystem. A full storage fault model is future work (see
[ROADMAP.md](ROADMAP.md)).

## Clock faults

`node.set_clock(offset, drift_ppm)` and the `clock` fault change only that
node's wall clock (`time.time()`, `node.wall_time()`). Scheduling, timers and
`loop.time()` use the global monotonic virtual clock.

## Random fault schedules

With `faults="..."`, a schedule of 1..`max_faults` (4) faults is generated
from the `faults` stream **before** the run, with start times uniform in
`[window_start, window_end]` (0.1–10 s) at millisecond resolution:

| Kind | Shape | Duration |
|---|---|---|
| `partition` | isolate one node; split nodes into two random groups; or block one directed link | `partition_duration` 0.5–5 s, then heal |
| `crash` | one node | down for `crash_downtime` 0.2–3 s, then restart (skipped if the node is not running) |
| `reset` | all connections between two nodes | instant |
| `latency` | multiply latency between two nodes by `latency_factor` (20) | `latency_duration` 0.5–3 s |
| `clock` | offset up to ±`clock_offset_max` (2 s), drift up to ±`clock_drift_ppm_max` (500 ppm) | `partition_duration`, then restore |

Random faults target configured nodes only (not `control` or nodes added with
`add_node`). Schedules can also be supplied explicitly
(`detloop.run(..., faults=[Fault(...)])`); manifests of shrunk runs store them.
