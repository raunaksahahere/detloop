# 0004: Context-variable ownership and incarnation generations

## Context

A crash must stop everything a node started: tasks, their descendants,
timers, callbacks, transports and servers. Cancelling tasks is not enough:
service code can suppress `CancelledError`, and late callbacks or packets
could act on a "dead" node or reach its next incarnation.

## Decision

- Ownership travels in a `ContextVar`. asyncio already copies the current
  context into new tasks, into every `call_soon`/`call_later` handle, and
  registers future done-callbacks with the awaiting task's context. Ownership
  therefore follows descendants without wrapping asyncio APIs.
- Control contexts carry an explicit `None` owner; a context that never saw an
  owner (e.g. `contextvars.Context()` created by service code) adopts the
  current owner. An early bug where these were indistinguishable let a node
  "adopt" the test body's task; the sentinel fixed it.
- Each start/restart creates an `Incarnation` (`n1#1`, `n1#2`, ...). A crash
  marks it dead; the loop never executes a handle owned by a dead incarnation.
  Since every task step is a handle, crashed coroutines can never run again,
  even if they would ignore cancellation.
- Packet arrivals are timers owned by the receiving incarnation at send time,
  so they are dropped if it dies; retransmission timers are owned by the
  sender.
- Frozen tasks are disposed of at world teardown after the outcome is fixed:
  their coroutines are closed, effects are discarded and nothing is traced.

## Consequences

- No cooperation from service code is needed for a hard stop.
- Teardown may run `finally` blocks of crashed code (coroutine close), but it
  happens after the result is recorded and cannot affect it.
- Code that passes objects (futures, queues) directly between nodes bypasses
  the network model; detloop cannot prevent that and documents it.
