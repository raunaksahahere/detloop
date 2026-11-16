# Architecture decision records

| # | Decision | Status |
|---|---|---|
| [0001](0001-custom-event-loop.md) | Implement `AbstractEventLoop` directly instead of patching `BaseEventLoop` | Accepted |
| [0002](0002-scheduling-compatibility.md) | Explore interleavings per owner while preserving asyncio FIFO order | Accepted |
| [0003](0003-fault-semantics.md) | Reliable streams with faults below them; a separate lossy message model | Accepted |
| [0004](0004-node-ownership-and-crashes.md) | Context-variable ownership and incarnation generations for crashes | Accepted |
| [0005](0005-linearizability-checker.md) | Bounded WGL search with memoization and a three-valued result | Accepted |
| [0006](0006-replay-and-shrinking.md) | Decision tapes, strict replay, and execution-validated delta debugging | Accepted |
| [0007](0007-version-support.md) | Support CPython 3.11–3.14 with explicit loop injection | Accepted |

Each record states the context, the decision, the alternatives considered and
the consequences. New decisions get the next number; superseded records are
kept and marked.
