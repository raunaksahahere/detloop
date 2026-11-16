# 0002: Per-owner interleaving that preserves asyncio FIFO order

## Context

The project brief suggests processing "the ready queue in a seeded permuted
order". asyncio documents that `call_soon` callbacks run in registration
order; libraries (streams, futures, `TaskGroup`) rely on it. Shuffling every
callback would produce executions real asyncio can never produce, so a found
"bug" could be an artifact of the simulator.

Distributed-systems bugs mostly come from the relative timing of *independent
processes*, which in reality each run their own event loop.

## Decision

- Every handle has an owner (control or a node incarnation; ADR 0004).
- Each owner behaves like its own event loop: its own FIFO queue, its own
  iterations and batch boundary, its due timers joining after already-queued
  handles at its iteration start — exactly `BaseEventLoop._run_once` per owner.
- When several owners are runnable, a recorded decision chooses the owner of
  the next handle (candidates ordered by their oldest pending handle, so `0`
  means global FIFO).
- `fifo` mode keeps a single global iteration structure for single-loop
  fidelity.
- The literal ready-queue permutation is available only as the opt-in
  `relaxed` mode, documented as able to produce executions impossible under
  asyncio.

An earlier version used a *global* batch boundary across owners. Testing
showed it could never interleave a node's continuation before another node's
first step within the same batch, hiding races that independent processes
exhibit (`test_racy_counter_has_both_outcomes`). Per-owner iterations fixed
this.

## Consequences

- Within a node, asyncio's ordering guarantees hold in the default mode.
- Interleavings between nodes, and between a node and network deliveries
  (owned by the receiving node), are explored.
- Interleavings of independent tasks *within one node* are explored only
  through timing (latency, timers), not by reordering ready callbacks, unless
  `relaxed` is enabled. This is a deliberate fidelity trade-off.
