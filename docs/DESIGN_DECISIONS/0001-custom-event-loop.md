# 0001: Implement `AbstractEventLoop` directly

## Context

A deterministic loop needs full control over the ready queue, the clock and
every I/O entry point. Two approaches exist:

1. Subclass `asyncio.BaseEventLoop`, supply a fake selector that advances a
   virtual clock in `select(timeout)`, and override the networking methods.
2. Implement `asyncio.AbstractEventLoop` from scratch.

`BaseEventLoop._run_once` is private, processes the whole ready deque in one
pass (no hook between handles for a scheduling decision), changes between
releases, and its `create_connection`/`create_server` paths create real
sockets. A fake selector would still require overriding most of the class and
would couple detloop to private attributes (`_ready`, `_scheduled`,
`_selector`, `_process_events`).

## Decision

`SimLoop` implements `AbstractEventLoop` directly. It reuses the public
`asyncio.Handle`/`TimerHandle` classes (subclassed with two slots), the
stdlib `Task`/`Future` (C-accelerated), and `asyncio.Runner(loop_factory=...)`
for lifecycle. Out-of-scope methods raise `UnsupportedOperation` or
`NondeterminismError` with an actionable message instead of placeholders or
silent fallbacks.

Private CPython details still used are isolated in `_compat.py` and covered by
tests: `events._set_running_loop`, `Handle._run`, the
`_timer_handle_cancelled` hook, `TimerHandle._scheduled`,
`Task._log_destroy_pending`, and `__self__` on task step callbacks.

## Alternatives

- **BaseEventLoop + fake selector** (approach of async-solipsism): less code
  initially, but no per-handle scheduling point and more private coupling.
- **Policy-based installation**: deprecated in 3.14 and scheduled for removal;
  explicit `Runner(loop_factory=...)` is the supported injection point.

## Consequences

- One handle per step gives a clean decision point and budget accounting.
- The loop must faithfully mirror `_run_once` iteration semantics; this is
  tested against real asyncio ordering (`test_fifo_scheduler_matches_real_asyncio_order`).
- Each new asyncio release must be checked for changes in `Handle`, `Task`
  and `Runner` (see [MAINTENANCE.md](../MAINTENANCE.md)).
