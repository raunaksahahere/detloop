# 0007: CPython 3.11–3.14 with explicit loop injection

## Context

`asyncio.Runner(loop_factory=...)` exists since 3.11; `asyncio.run(loop_factory=...)`
since 3.12. The policy system is deprecated in 3.14 and scheduled for removal
in 3.16. asyncio internals differ across minor versions: `create_task`
forwards `**kwargs` from 3.13, eager task factories exist from 3.12,
`Server.wait_closed` semantics changed in 3.12, and `serve_forever` closes
clients on cancellation from 3.13.

## Decision

- Support CPython 3.11, 3.12, 3.13 and 3.14 on Linux, macOS and Windows,
  verified by the CI matrix. Newer versions are unsupported until validated.
- Always inject the loop with `asyncio.Runner(loop_factory=...)`; never
  install a policy.
- Mirror version-specific behaviour where it is observable to applications
  (`SimServer.wait_closed`, `serve_forever` client closing, `create_task`
  keyword forwarding).
- Do not promise byte-identical traces across minor versions. CI compares a
  declared portable fixture across operating systems per version and reports
  cross-version differences. Pinned regression seeds are declared per runtime
  family (3.11 vs 3.12+).
- PyPy and other implementations are unsupported (the C-accelerated `Task`
  path and `_compat` assumptions are CPython-specific).

## Consequences

- Each new CPython release requires re-running the compatibility tests and
  reviewing `_compat.py` (see [MAINTENANCE.md](../MAINTENANCE.md)).
